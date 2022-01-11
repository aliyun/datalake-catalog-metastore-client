package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.hive.common.utils.Utils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PartitionTest {

    private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;
    protected static Warehouse warehouse;
    private static IMetaStoreClient client;
    private static String databaseName;
    private static HiveConf hiveConf;
    private static Database database;
    private String tableName;
    private Table table;

    @BeforeClass
    public static void initClient() throws TException, FileNotFoundException {
        hiveConf = new HiveConf();
        hiveConf.setInt(DataLakeConfig.CATALOG_ACCURATE_BATCH_SIZE, 3);
        hiveConf.setInt(DataLakeConfig.CATALOG_CLIENT_NUM_THREADS, 2);

        client = TestUtil.getDlfClient();
        databaseName = "metastore_ut_partition_test_from_client_t";
        database = TestUtil.buildDatabase(databaseName);
        try {
            client.dropDatabase(databaseName, true, true, true);
        } catch (Exception e) {
            Assert.assertFalse(true);
        }
        warehouse = new Warehouse(hiveConf);
        client.createDatabase(database);
    }

    @AfterClass
    public static void tearDown() {
        try {
            List<String> allTables = client.getTables(databaseName, "*");
            allTables.forEach(t -> {
                try {
                    client.dropTable(databaseName, t, true, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            client.dropDatabase(databaseName, true, true, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void checkExpr(int numParts,
                                 String dbName, String tblName, ExprNodeGenericFuncDesc expr) throws Exception {
        List<Partition> parts = new ArrayList<Partition>();
        client.listPartitionsByExpr(dbName, tblName, SerializationUtilities.serializeExpressionToKryo(expr), null, (short) -1, parts);

        Assert.assertEquals("Partition check failed: " + expr.getExprString(), numParts, parts.size());
    }

    private static List<String> makeVals(String ds, String id) {
        List<String> vals4 = new ArrayList<String>(2);
        vals4 = new ArrayList<String>(2);
        vals4.add(ds);
        vals4.add(id);
        return vals4;
    }

    private static Partition makePartitionObject(String dbName, String tblName,
                                                 List<String> ptnVals, Table tbl, String ptnLocationSuffix) throws MetaException {
        Partition part4 = new Partition();
        part4.setDbName(dbName);
        part4.setTableName(tblName);
        part4.setValues(ptnVals);
        part4.setParameters(new HashMap<String, String>());
        part4.setSd(tbl.getSd().deepCopy());
        part4.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo().deepCopy());
        part4.getSd().setLocation(tbl.getSd().getLocation() + ptnLocationSuffix);
        MetaStoreUtils.updatePartitionStatsFast(part4, warehouse, null);
        return part4;
    }

    private static void adjust(Partition part,
                               String dbName, String tblName)
            throws NoSuchObjectException, MetaException, TException {
        Partition part_get = client.getPartition(dbName, tblName, part.getValues());
        part.setCreateTime(part_get.getCreateTime());
        part.putToParameters(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DDL_TIME, Long.toString(part_get.getCreateTime()));
    }

    private static void verifyPartitionsPublished(String dbName, String tblName, List<String> partialSpec,
                                                  List<Partition> expectedPartitions)
            throws NoSuchObjectException, MetaException, TException {
        // Test partition listing with a partial spec

        List<Partition> mpartial = client.listPartitions(dbName, tblName, partialSpec,
                (short) -1);
        Assert.assertEquals("Should have returned " + expectedPartitions.size() +
                        " partitions, returned " + mpartial.size(),
                expectedPartitions.size(), mpartial.size());
        Assert.assertTrue("Not all parts returned", mpartial.containsAll(expectedPartitions));
    }

    @Before
    public void createTable() throws Exception {
        tableName = "partition_table";
        client.dropTable(databaseName, tableName, true, true);
        table = TestUtil.buildTable(databaseName, tableName);
        //table's location ： WAREHOUSE_BASE_PATH + databaseName + "/" + tableName;
        //db's location WAREHOUSE_PATH+"db_create";
        client.createTable(table);
    }

    @After
    public void dropTable() throws TException {
        List<String> allTables = client.getTables(databaseName, "*");
        allTables.forEach(t -> {
            try {
                client.dropTable(databaseName, t, true, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testCreatePartition() throws TException {
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        Partition addResult = client.add_partition(partition);
        Partition getResult = client.getPartition(databaseName, tableName, Lists.newArrayList("2020"));
        partition.setCreateTime(addResult.getCreateTime());
        partition.setLastAccessTime(addResult.getLastAccessTime());
        partition.setParameters(addResult.getParameters());

        Assert.assertEquals(partition, addResult);
        Assert.assertEquals(addResult, getResult);
    }

    @Test
    public void testCreatePartitionExtend() throws TException {
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020-20-23 12:34:15/1234"));
        Partition addResult = client.add_partition(partition);
        Partition getResult = client.getPartition(databaseName, tableName, Lists.newArrayList("2020-20-23 12:34:15/1234"));
        partition.setCreateTime(addResult.getCreateTime());
        partition.setLastAccessTime(addResult.getLastAccessTime());
        partition.setParameters(addResult.getParameters());

        Assert.assertEquals(partition, addResult);
        Assert.assertEquals(addResult, getResult);
    }

    @Test
    public void testReCreatePartition() throws TException {
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        client.add_partition(partition);
//        Partition part = client.getPartition(databaseName, tableName, partitionName);
        Assert.assertThrows(AlreadyExistsException.class, () -> client.add_partition(partition));
    }

    @Test
    public void testBatchCreatePartition() throws TException {
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        Partition partition2 = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020_2"));

        int i = client.add_partitions(Lists.newArrayList(partition, partition2));
        Assert.assertEquals(i, 2);

        // recreate
        Assert.assertThrows(AlreadyExistsException.class, () -> client.add_partitions(Lists.newArrayList(partition, partition2)));
    }

    @Test
    public void testGetPartition() throws TException {
        Assert.assertThrows(NoSuchObjectException.class, () -> client.getPartition(databaseName, tableName,
                Lists.newArrayList("2020")));
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        Partition addResult = client.add_partition(partition);
        Partition result = client.getPartition(databaseName, tableName, Lists.newArrayList("2020"));
        Assert.assertEquals(addResult.getValues(), result.getValues());
    }

    @Test
    public void testAppendPartition() throws TException {
        Assert.assertThrows(NoSuchObjectException.class, () -> client.getPartition(databaseName, tableName,
                Lists.newArrayList("2020")));
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        Partition addResult = client.appendPartition(databaseName, tableName, partition.getValues());
        Partition result = client.getPartition(databaseName, tableName, Lists.newArrayList("2020"));
        Assert.assertEquals(addResult.getValues(), result.getValues());
    }

    @Test
    public void testUpdatePartitions() throws TException {
        List<Partition> partitionList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList("2020" + "_" + i));
            partitionList.add(partition);
        }

        List<Partition> partitions = client.add_partitions(partitionList, false, true);
        for (int i = 0; i < 20; i++) {
            Map<String, String> map = new HashMap<>();
            map.put("testUpdatePartitionsK" + i, "testUpdatePartitionsV");
            partitions.get(i).getSd().setParameters(map);
        }

        Map<List<String>, Partition> partitionMap = Maps.newHashMap();
        for (Partition partition : partitions) {
            partitionMap.put(partition.getValues(), partition);
        }

        // Normal Update
        client.alter_partitions(databaseName, tableName, partitions);
        List<Partition> resultList = client.listPartitions(databaseName, tableName, (short)100);

        // check all partition update
        for (Partition partition : resultList) {
            Partition sourcePartition = partitionMap.get(partition.getValues());
            // update table parameter because of meta server will add something，check sd parameter instead
            sourcePartition.setParameters(partition.getParameters());
            Assert.assertEquals(sourcePartition, partition);
        }

        //alter partition will gather stats
        Assert.assertTrue("it's impossible that alter partition will not gather stats", resultList.get(0).getParameters().get("numFiles") != null);
        Assert.assertTrue("it's impossible that alter partition will not gather stats", resultList.get(0).getParameters().get("totalSize") != null);
        // alter partition will just update basic stats, if old partition and new partition's parameters are same.
        partitions.get(0).setParameters(resultList.get(0).getParameters());
        EnvironmentContext environmentContext = new EnvironmentContext();
        environmentContext.setProperties(new HashMap<>());
        environmentContext.getProperties().put("STATS_GENERATED", "TASK");
        client.alter_partitions(databaseName, tableName, partitions, environmentContext);
        Partition result = client.getPartition(databaseName, tableName, Lists.newArrayList("2020_0"));
        assertTrue(result.getParameters().get("COLUMN_STATS_ACCURATE") != null);

        // Change tableName when Update partition
        String newTableName = "partition_table" + "_new";
        partitionList.get(0).setTableName(newTableName);
        Assert.assertThrows(MetaException.class, () -> client.alter_partitions(databaseName, tableName,
                Lists.newArrayList(partitionList.get(0))));

        // Update None exists partition
        partitionList.get(0).setValues(Lists.newArrayList("111111"));
        Assert.assertThrows(MetaException.class, () -> client.alter_partitions(databaseName, tableName,
                Lists.newArrayList(partitionList.get(0))));
    }

    @Test
    public void testRenamePartition() throws TException {
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        Partition add_partition = client.add_partition(partition);

        add_partition.setValues(Lists.newArrayList("2021"));
        client.renamePartition(databaseName, tableName, Lists.newArrayList("2020"), add_partition);

        // 2020 not exist now
        Assert.assertThrows(NoSuchObjectException.class, () -> client.getPartition(databaseName, tableName,
                Lists.newArrayList("2020")));

        // 2021 exit
        Partition result = client.getPartition(databaseName, tableName, Lists.newArrayList("2021"));
        add_partition.setValues(Lists.newArrayList("2021"));
        result.setParameters(add_partition.getParameters());
        Assert.assertEquals(add_partition, result);

        //rename partition will gather stats
        Assert.assertTrue("it's impossible that rename partition operation not gather stats", result.getParameters().get("numFiles") != null);
        Assert.assertTrue("it's impossible that rename partition operation not gather stats", result.getParameters().get("totalSize") != null);
    }

    @Test
    public void testGetNumPartitionsByFilter() throws TException {
        List<Partition> partitionList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partitionList.add(partition);
        }

        List<Partition> partitions = client.add_partitions(partitionList, false, true);
        int numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                table.getPartitionKeys().get(0).getName() + "=2020");
        Assert.assertEquals(0, numPartitionsByFilter);

        numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                table.getPartitionKeys().get(0).getName() + "=20201");
        Assert.assertEquals(1, numPartitionsByFilter);

        numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                table.getPartitionKeys().get(0).getName() + "=20201");
        Assert.assertEquals(1, numPartitionsByFilter);

        numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                table.getPartitionKeys().get(0).getName() + ">20201");
        Assert.assertEquals(18, numPartitionsByFilter);

        numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                table.getPartitionKeys().get(0).getName() + "<=20201");
        Assert.assertEquals(2, numPartitionsByFilter);

        numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                table.getPartitionKeys().get(0).getName() + ">-1");
        Assert.assertEquals(20, numPartitionsByFilter);
    }

    @Test
    public void testGetPartitionByNames() throws TException {
        List<Partition> partitionList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partitionList.add(partition);
        }

        client.add_partitions(partitionList, false, true);

        List<String> part = Lists.newArrayList();

        // get none
        List<Partition> partitionsByNames = client.getPartitionsByNames(databaseName, tableName, part);
        Assert.assertEquals(0, partitionsByNames.size());

        // get one for one thread
        part.add(table.getPartitionKeys().get(0).getName() + "=20201");
        partitionsByNames = client.getPartitionsByNames(databaseName, tableName, part);
        Assert.assertEquals(1, partitionsByNames.size());
        checkPartitionNamesInResult(partitionsByNames, part);
        checkPartition(partitionsByNames, partitionList.subList(1, 2));

        // get three, one/two per thread
        part.add(table.getPartitionKeys().get(0).getName() + "=20202");
        part.add(table.getPartitionKeys().get(0).getName() + "=20203");

        partitionsByNames = client.getPartitionsByNames(databaseName, tableName, part);
        Assert.assertEquals(3, partitionsByNames.size());
        checkPartitionNamesInResult(partitionsByNames, part);
        checkPartition(partitionsByNames, partitionList.subList(1, 4));

        // get four and none exists, one for another thread
        part.add(table.getPartitionKeys().get(0).getName() + "=20204");
        part.add(table.getPartitionKeys().get(0).getName() + "=2020211111111");
        partitionsByNames = client.getPartitionsByNames(databaseName, tableName, part);
        Assert.assertEquals(4, partitionsByNames.size());
        part.remove(part.size() -1);
        checkPartitionNamesInResult(partitionsByNames, part);
        checkPartition(partitionsByNames, partitionList.subList(1, 5));
    }

    @Test
    public void testGetPartitionsWithShareSd() throws Exception {
        //测试共享sd的效果
        List<String> partNames = Lists.newArrayList();
        List<String> partValues = Lists.newArrayList();
        List<Partition> partitionList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partValues.add(2020 + "" + i);
            if (i < 10 ) {
                //如果每个partition的location不是table根目录，那么不能共享
                if(i % 2 == 0) {
                    partition.getSd().setLocation(partition.getSd().getLocation().replace(tableName, String.valueOf(i)));
                } else {//如果每个partition的location的cols不同，那么不能共享
                    partition.getSd().getCols().get(0).setName("no_sd_col"+i);
                }
            } else if (i < 15) {
                //partition 10-15共享相同列字段
                partition.getSd().getCols().get(0).setName("sd_col"+10);
            } else if (i < 17) {
                //partition 15-17共享相同列字段，location也是相同的
                partition.getSd().setLocation(partition.getSd().getLocation().replace("day", "15/day"));
            }
            //partition 17-20 共享相同的列字段
            partitionList.add(partition);
            partNames.add(table.getPartitionKeys().get(0).getName() + "=2020" + "" + i);
        }
        client.add_partitions(partitionList, false, true);

        //getpartitionbyname调用的api接口：batchgetpartitions
        List<Partition> partitionsByNames = client.getPartitionsByNames(databaseName, tableName, partNames);
        checkPartition(partitionsByNames, partitionList);

        //listpartitions调用的api接口：listpartitions
        List<Partition> listPartitions = client.listPartitions(databaseName, tableName, (short) -1);
        checkPartition(listPartitions, partitionList);

        //listpartitions调用的api接口：listpartitions
        List<Partition> listPartitions2 = client.listPartitions(databaseName, tableName, partValues.subList(10,11), (short) -1);
        checkPartition(listPartitions2, partitionList.subList(10,11));

        //listpartitionbyfilter调用的api接口：listpartitionbyfilter
        List<Partition> listPartitionsByFilter = client.listPartitionsByFilter(databaseName, tableName, "day>=20205", (short) -1);
        checkPartition(listPartitionsByFilter, partitionList.subList(5,20));

        //listpartitionbyExpr调用的api接口：listpartitionbyFilter
        ExprBuilder e = new ExprBuilder(tableName);
        List<Partition> listPartitionsByExpr = new ArrayList<>();
        client.listPartitionsByExpr(databaseName, tableName, SerializationUtilities.serializeExpressionToKryo(e.val(20205).intCol("day").pred(">=", 2).build()), null, (short) -1, listPartitionsByExpr);
        checkPartition(listPartitionsByExpr, partitionList.subList(5,20));

        //getNumPartitionsByFilter调用的api接口listpartitionbyFilter
        int numPartitionsByFilter = client.getNumPartitionsByFilter(databaseName, tableName,
                        table.getPartitionKeys().get(0).getName() + "=20205");
        Assert.assertEquals(1, numPartitionsByFilter);

        //drop table 调用getNonSubDirectoryPartitionLocations调用的api接口listpartitions
        client.dropTable(databaseName, tableName, true, true, true);
        Path dbPath = new Path(database.getLocationUri());
        FileSystem fs = FileSystem.get(dbPath.toUri(), hiveConf);
        for (Partition partition : partitionList) {
            assertTrue(!fs.exists(new Path(partition.getSd().getLocation())));
        }
    }

    public void checkPartition(List<Partition> src, List<Partition> target) {
        Map<String, Partition> targetPartitionMap = new HashMap<>();
        for (Partition partition : target) {
            targetPartitionMap.put(partition.getValues().toString(), partition);
        }
        for (int i = 0; i < src.size(); i++) {
            Partition srcPart = src.get(i);
            Partition targetPart = targetPartitionMap.get(srcPart.getValues().toString());
            Assert.assertEquals(srcPart.getDbName().toLowerCase(), targetPart.getDbName().toLowerCase());
            Assert.assertEquals(srcPart.getTableName().toLowerCase(), targetPart.getTableName().toLowerCase());
            Assert.assertEquals(srcPart.getValues(), targetPart.getValues());
            Assert.assertEquals(srcPart.getSd(), targetPart.getSd());
        }
    }

    public void checkPartitionNamesInResult(List<Partition> results, List<String> partitionNames) throws MetaException {
        Assert.assertEquals(results.size(), partitionNames.size());

        List<List<String>> partitionValues = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            partitionValues.add(Warehouse.makeSpecFromName(partitionName).values().stream().collect(Collectors.toList()));
        }

        List<List<String>> partitionResultValues = Lists.newArrayList();
        for (Partition result : results) {
            partitionResultValues.add(result.getValues());
        }

        for (List<String> partitionValue : partitionValues) {
            Assert.assertTrue(partitionResultValues.contains(partitionValue));
        }
    }

    @Test
    public void testListPartitionsByExpr() throws Exception {

        String nTableName = "testListPartitionsByExpr";

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));
        ArrayList<FieldSchema> partCols = Lists.newArrayList(
                new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""),
                new FieldSchema("p2", serdeConstants.INT_TYPE_NAME, ""));

        Table tbl = new Table();
        tbl.setDbName(databaseName);
        tbl.setTableName(nTableName);
        TestUtil.addSd(cols, tbl);

        tbl.setPartitionKeys(partCols);
        client.createTable(tbl);
        tbl = client.getTable(databaseName, nTableName);

        TestUtil.addPartition(client, tbl, Lists.newArrayList("p11", "32"), "part1");
        TestUtil.addPartition(client, tbl, Lists.newArrayList("p12", "32"), "part2");
        TestUtil.addPartition(client, tbl, Lists.newArrayList("p13", "31"), "part3");
        TestUtil.addPartition(client, tbl, Lists.newArrayList("p14", "-33"), "part4");

        ExprBuilder e = new ExprBuilder(nTableName);

        checkExpr(3, databaseName, nTableName, e.val(0).intCol("p2").pred(">", 2).build());
        checkExpr(3, databaseName, nTableName, e.intCol("p2").val(0).pred("<", 2).build());
        checkExpr(1, databaseName, nTableName, e.intCol("p2").val(0).pred(">", 2).build());
        checkExpr(2, databaseName, nTableName, e.val(31).intCol("p2").pred("<=", 2).build());
        checkExpr(3, databaseName, nTableName, e.val("p11").strCol("p1").pred(">", 2).build());
        checkExpr(1, databaseName, nTableName, e.val("p11").strCol("p1").pred(">", 2)
                .intCol("p2").val(31).pred("<", 2).pred("and", 2).build());
        checkExpr(3, databaseName, nTableName,
                e.val(32).val(31).intCol("p2").val(false).pred("between", 4).build());

        // Apply isnull and instr (not supported by pushdown) via name filtering.
        checkExpr(4, databaseName, nTableName, e.val("p").strCol("p1")
                .fn("instr", TypeInfoFactory.intTypeInfo, 2).val(0).pred("<=", 2).build());
        checkExpr(0, databaseName, nTableName, e.intCol("p2").pred("isnull", 1).build());

        // Cannot deserialize => throw the specific exception.
        try {
            client.listPartitionsByExpr(databaseName, nTableName,
                    new byte[]{'f', 'o', 'o'}, null, (short) -1, new ArrayList<Partition>());
            fail("Should have thrown IncompatibleMetastoreException");
        } catch (IMetaStoreClient.IncompatibleMetastoreException ignore) {
        }

        // Invalid expression => throw some exception, but not incompatible metastore.
        try {
            checkExpr(-1, databaseName, nTableName, e.val(31).intCol("p3").pred(">", 2).build());
            fail("Should have thrown");
        } catch (IMetaStoreClient.IncompatibleMetastoreException ignore) {
            fail("Should not have thrown IncompatibleMetastoreException");
        } catch (Exception ignore) {
        }
        client.dropTable(databaseName, nTableName);
        // extra test
        List<Partition> partitionList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partitionList.add(partition);
        }

        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("__HIVE_DEFAULT_PARTITION__"));
        partitionList.add(partition);

        client.add_partitions(partitionList, false, true);

        e = new ExprBuilder(tableName);
        checkExpr(1, databaseName, tableName, e.intCol("day").pred("isnull", 1).build());
        checkExpr(20, databaseName, tableName, e.intCol("day").pred("isnotnull", 1).build());
    }

    @Test
    public void testListPartitions() throws TException {
        List<Partition> partitionList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partitionList.add(partition);
        }

        client.add_partitions(partitionList, false, true);

        List<Partition> ret = client.listPartitions(databaseName, tableName, (short) 100);

        for (Partition partition : ret) {
            partition.setCreateTime(0);
            partition.setLastAccessTime(1);
            partition.setParameters(partitionList.get(0).getParameters());
        }

        Set<Partition> source = Sets.newHashSet(partitionList);
        Set<Partition> retSet = Sets.newHashSet(ret);

        Assert.assertEquals(source.size(), retSet.size());

        for (Partition partition : source) {
            Assert.assertTrue(retSet.contains(partition));
        }

        ret = client.listPartitions(databaseName, tableName, (short) 2);
        Assert.assertEquals(2, ret.size());
    }

    @Test
    public void testListPartitionNames() throws TException {
        List<Partition> partitionList = Lists.newArrayList();
        List<String> names = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partitionList.add(partition);
            names.add(table.getPartitionKeys().get(0).getName() + "=2020" + i);
        }

        client.add_partitions(partitionList, false, true);

        List<String> ret = client.listPartitionNames(databaseName, tableName, (short) 100);
        Collections.sort(names);
        Collections.sort(ret);
        Assert.assertEquals(names, ret);

        ret = client.listPartitionNames(databaseName, tableName, (short) 2);
        Assert.assertEquals(2, ret.size());
    }

    @Test
    public void testListPartitionNamesMultiLevel() throws TException {
        List<Partition> partitionList = Lists.newArrayList();

        // create a partition table which has multi partition keys.
        String tblName = "multiLevelPartTable";
        Map<String, String> cols = ImmutableMap.of("id", "int");
        Map<String, String> partCols = ImmutableMap.of("yr", "string", "mo", "string", "dt", "string");
        Table partTable = TestUtil.getPartitionTable(databaseName, tblName, cols, partCols);
        client.createTable(partTable);
        //table's location in testutil.warehouse_path+databaseName+".db"+tablename
        // create partitions
        Partition part1 = TestUtil.getPartition(databaseName, tblName, cols, partCols,
                Lists.newArrayList("2020", "07", "01"));
        Partition part2 = TestUtil.getPartition(databaseName, tblName, cols, partCols,
                Lists.newArrayList("2020", "07", "02"));
        Partition part3 = TestUtil.getPartition(databaseName, tblName, cols, partCols,
                Lists.newArrayList("2020", "08", "01"));
        partitionList.add(part1);
        partitionList.add(part2);
        partitionList.add(part3);
        client.add_partitions(partitionList, false, true);

        // get partition names with partial partition spec
        List<String> ret = client.listPartitionNames(databaseName, tblName, Lists.newArrayList("2020", "", ""), (short) 100);
        List<String> expected = Lists.newArrayList("yr=2020/mo=07/dt=01", "yr=2020/mo=07/dt=02", "yr=2020/mo=08/dt=01");
        Collections.sort(ret);
        Collections.sort(expected);
        Assert.assertEquals(expected, ret);

        ret = client.listPartitionNames(databaseName, tblName, Lists.newArrayList("2020", "07", ""), (short) 100);
        expected = Lists.newArrayList("yr=2020/mo=07/dt=01", "yr=2020/mo=07/dt=02");
        Collections.sort(ret);
        Collections.sort(expected);
        Assert.assertEquals(expected, ret);

        // clean
        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testDropPartitions() throws Exception {
        List<Partition> partitionList = Lists.newArrayList();
        List<String> names = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1,
                    Lists.newArrayList(2020 + "" + i));
            partitionList.add(partition);
            names.add(table.getPartitionKeys().get(0).getName() + "=2020" + i);
            String location = partition.getSd().getLocation();
            warehouse.mkdirs(new Path(location + "/test1"), true);
            warehouse.mkdirs(new Path(location + "/test1/test1_1"), true);
        }

        client.add_partitions(partitionList, false, true);

        PartitionDropOptions instance = PartitionDropOptions.instance();
        instance.deleteData(true);
        instance.ifExists(false);
        instance.purgeData(true);

        // Drop partition Need exist
        client.dropPartition(databaseName, tableName, partitionList.get(0).getValues(), instance);
        //check data file has been deleted
        FileSystem fs = warehouse.getFs(new Path(partitionList.get(0).getSd().getLocation()));
        Assert.assertFalse(fs.exists(new Path(partitionList.get(0).getSd().getLocation())));

        // Drop Partition Need exist, but actually not
        Assert.assertThrows(NoSuchObjectException.class, () -> client.dropPartition(databaseName, tableName,
                partitionList.get(0).getValues(), instance));

        // Drop Partition Don't Need exist, but actually not
        instance.ifExists(true);
        client.dropPartition(databaseName, tableName, partitionList.get(0).getValues(), instance);

        ExprBuilder e = new ExprBuilder(tableName);
        ExprNodeGenericFuncDesc partitionExpression = e.val(20201).intCol("day").pred(">", 2).build();
        ObjectPair<Integer, byte[]> serializedPartitionExpression = new ObjectPair<>(1,
                SerializationUtilities.serializeExpressionToKryo(partitionExpression));
        List<Partition> partitions = client.dropPartitions(table.getDbName(), table.getTableName(),
                Arrays.asList(serializedPartitionExpression), true, true, true);
        Assert.assertEquals(18, partitions.size());
        //check data file has been deleted
        for (int i = 3; i < 20; i++) {
            Assert.assertFalse(fs.exists(new Path(partitionList.get(0).getSd().getLocation())));
        }

        List<Partition> result = client.listPartitions(table.getDbName(), table.getTableName(), (short) 100);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getValues().get(0), "20201");
    }

    @Test
    public void testShowPartitionsAfterAlterTable() throws TException {
        Assert.assertThrows(NoSuchObjectException.class, () -> client.getPartition(databaseName, tableName,
                Lists.newArrayList("2020")));
        Partition partition = TestUtil.buildPartition(databaseName, tableName, table, 1, Lists.newArrayList("2020"));
        Partition addResult = client.appendPartition(databaseName, tableName, partition.getValues());
        Partition result = client.getPartition(databaseName, tableName, Lists.newArrayList("2020"));
        Assert.assertEquals(addResult.getValues(), result.getValues());

        Table t = client.getTable(databaseName, tableName);
        Map<String, String> newParams = new HashMap<>(t.getParameters());
        newParams.put("newKey", "newValue");
        t.setParameters(newParams);

        client.alter_table(databaseName, tableName, t);

        result = client.getPartition(databaseName, tableName, Lists.newArrayList("2020"));
        Assert.assertEquals(addResult.getValues(), result.getValues());
    }

    @Test
    public void testNameMethods() {
        Map<String, String> spec = new LinkedHashMap<String, String>();
        spec.put("ds", "2008-07-01 14:13:12");
        spec.put("hr", "14");
        List<String> vals = new ArrayList<String>();
        for (String v : spec.values()) {
            vals.add(v);
        }
        String partName = "ds=2008-07-01 14%3A13%3A12/hr=14";

        try {
            List<String> testVals = client.partitionNameToVals(partName);
            Assert.assertTrue("Values from name are incorrect", vals.equals(testVals));

            Map<String, String> testSpec = client.partitionNameToSpec(partName);
            Assert.assertTrue("Spec from name is incorrect", spec.equals(testSpec));

            List<String> emptyVals = client.partitionNameToVals("");
            Assert.assertTrue("Values should be empty", emptyVals.size() == 0);

            Map<String, String> emptySpec = client.partitionNameToSpec("");
            Assert.assertTrue("Spec should be empty", emptySpec.size() == 0);
        } catch (Exception e) {
            assert (false);
        }
    }

    @Test
    public void testPartitionTester()
            throws Exception {
        try {
            String tblName = "comptbl";
            String typeName = "Person";
            List<String> vals = makeVals("2008-07-01 14:13:12", "14");
            List<String> vals2 = makeVals("2008-07-01 14:13:12", "15");
            List<String> vals3 = makeVals("2008-07-02 14:13:12", "15");
            List<String> vals4 = makeVals("2008-07-03 14:13:12", "151");

            Path dbPath = new Path(database.getLocationUri());
            FileSystem fs = FileSystem.get(dbPath.toUri(), hiveConf);
            boolean inheritPerms = hiveConf.getBoolVar(
                    HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
            FsPermission dbPermission = fs.getFileStatus(dbPath).getPermission();
            if (inheritPerms) {
                //Set different perms for the database dir for further tests
                dbPermission = new FsPermission((short) 488);
                fs.setPermission(dbPath, dbPermission);
            }

            Type typ1 = new Type();
            typ1.setName(typeName);
            typ1.setFields(new ArrayList<FieldSchema>(2));
            typ1.getFields().add(
                    new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            typ1.getFields().add(
                    new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));


            Table tbl = new Table();
            tbl.setDbName(this.databaseName);
            tbl.setTableName(tblName);
            StorageDescriptor sd = new StorageDescriptor();
            tbl.setSd(sd);
            sd.setCols(typ1.getFields());
            sd.setCompressed(false);
            sd.setNumBuckets(1);
            sd.setParameters(new HashMap<String, String>());
            sd.getParameters().put("test_param_1", "Use this for comments etc");
            sd.setBucketCols(new ArrayList<String>(2));
            sd.getBucketCols().add("name");
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(tbl.getTableName());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());
            sd.getSerdeInfo().getParameters()
                    .put(serdeConstants.SERIALIZATION_FORMAT, "1");
            sd.setSortCols(new ArrayList<Order>());
            sd.setStoredAsSubDirectories(false);
            sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
            sd.setInputFormat(HiveInputFormat.class.getName());
            sd.setOutputFormat(HiveOutputFormat.class.getName());

            //skewed information
            SkewedInfo skewInfor = new SkewedInfo();
            skewInfor.setSkewedColNames(Arrays.asList("name"));
            List<String> skv = Arrays.asList("1");
            skewInfor.setSkewedColValues(Arrays.asList(skv));
            Map<List<String>, String> scvlm = new HashMap<List<String>, String>();
            scvlm.put(skv, "location1");
            skewInfor.setSkewedColValueLocationMaps(scvlm);
            sd.setSkewedInfo(skewInfor);

            tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
            tbl.getPartitionKeys().add(
                    new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
            tbl.getPartitionKeys().add(
                    new FieldSchema("hr", serdeConstants.STRING_TYPE_NAME, ""));

            client.createTable(tbl);

            tbl = client.getTable(databaseName, tblName);

            Assert.assertEquals(dbPermission, fs.getFileStatus(new Path(tbl.getSd().getLocation()))
                    .getPermission());

            Partition part = makePartitionObject(databaseName, tblName, vals, tbl, "/part1");
            Partition part2 = makePartitionObject(databaseName, tblName, vals2, tbl, "/part2");
            Partition part3 = makePartitionObject(databaseName, tblName, vals3, tbl, "/part3");
            Partition part4 = makePartitionObject(databaseName, tblName, vals4, tbl, "/part4");

            // check if the partition exists (it shouldn't)
            boolean exceptionThrown = false;
            try {
                Partition p = client.getPartition(databaseName, tblName, vals);
            } catch (Exception e) {
                Assert.assertEquals("partition should not have existed",
                        NoSuchObjectException.class, e.getClass());
                exceptionThrown = true;
            }
            Assert.assertTrue("getPartition() should have thrown NoSuchObjectException", exceptionThrown);
            Partition retp = client.add_partition(part);
            Assert.assertNotNull("Unable to create partition " + part, retp);
            Assert.assertEquals(dbPermission, fs.getFileStatus(new Path(retp.getSd().getLocation()))
                    .getPermission());
            Partition retp2 = client.add_partition(part2);
            Assert.assertNotNull("Unable to create partition " + part2, retp2);
            Assert.assertEquals(dbPermission, fs.getFileStatus(new Path(retp2.getSd().getLocation()))
                    .getPermission());
            Partition retp3 = client.add_partition(part3);
            Assert.assertNotNull("Unable to create partition " + part3, retp3);
            Assert.assertEquals(dbPermission, fs.getFileStatus(new Path(retp3.getSd().getLocation()))
                    .getPermission());
            Partition retp4 = client.add_partition(part4);
            Assert.assertNotNull("Unable to create partition " + part4, retp4);
            Assert.assertEquals(dbPermission, fs.getFileStatus(new Path(retp4.getSd().getLocation()))
                    .getPermission());

            Partition part_get = client.getPartition(databaseName, tblName, part.getValues());
            adjust(part, databaseName, tblName);
            adjust(part2, databaseName, tblName);
            adjust(part3, databaseName, tblName);
            Assert.assertTrue("Partitions are not same", part.equals(part_get));

            // check null cols schemas for a partition
            List<String> vals6 = makeVals("2016-02-22 00:00:00", "16");
            Partition part6 = makePartitionObject(databaseName, tblName, vals6, tbl, "/part5");
            part6.getSd().setCols(null);
            client.add_partition(part6);
            final List<Partition> partitions = client.listPartitions(databaseName, tblName, (short) -1);
            boolean foundPart = false;
            for (Partition p : partitions) {
                if (p.getValues().equals(vals6)) {
                    Assert.assertTrue(p.getSd().getCols().size() == 0);
                    foundPart = true;
                }
            }
            Assert.assertTrue(foundPart);

            String partName = "ds=" + FileUtils.escapePathName("2008-07-01 14:13:12") + "/hr=14";
            String part2Name = "ds=" + FileUtils.escapePathName("2008-07-01 14:13:12") + "/hr=15";
            String part3Name = "ds=" + FileUtils.escapePathName("2008-07-02 14:13:12") + "/hr=15";
            String part4Name = "ds=" + FileUtils.escapePathName("2008-07-03 14:13:12") + "/hr=151";

            part_get = client.getPartition(databaseName, tblName, partName);
            Assert.assertTrue("Partitions are not the same", part.equals(part_get));

            // Test partition listing with a partial spec - ds is specified but hr is not
            List<String> partialVals = new ArrayList<String>();
            partialVals.add(vals.get(0));
            Set<Partition> parts = new HashSet<Partition>();
            parts.add(part);
            parts.add(part2);

            List<Partition> partial = client.listPartitions(databaseName, tblName, partialVals,
                    (short) -1);
            Assert.assertTrue("Should have returned 2 partitions", partial.size() == 2);
            Assert.assertTrue("Not all parts returned", partial.containsAll(parts));

            Set<String> partNames = new HashSet<String>();
            partNames.add(partName);
            partNames.add(part2Name);
            List<String> partialNames = client.listPartitionNames(databaseName, tblName, partialVals,
                    (short) -1);
            Assert.assertTrue("Should have returned 2 partition names", partialNames.size() == 2);
            Assert.assertTrue("Not all part names returned", partialNames.containsAll(partNames));

            partNames.add(part3Name);
            partNames.add(part4Name);
            partialVals.clear();
            partialVals.add("");
            partialNames = client.listPartitionNames(databaseName, tblName, partialVals, (short) -1);
            Assert.assertTrue("Should have returned 5 partition names", partialNames.size() == 5);
            Assert.assertTrue("Not all part names returned", partialNames.containsAll(partNames));

            // Test partition listing with a partial spec - hr is specified but ds is not
            parts.clear();
            parts.add(part2);
            parts.add(part3);

            partialVals.clear();
            partialVals.add("");
            partialVals.add(vals2.get(1));

            partial = client.listPartitions(databaseName, tblName, partialVals, (short) -1);
            Assert.assertEquals("Should have returned 2 partitions", 2, partial.size());
            Assert.assertTrue("Not all parts returned", partial.containsAll(parts));

            partNames.clear();
            partNames.add(part2Name);
            partNames.add(part3Name);
            partialNames = client.listPartitionNames(databaseName, tblName, partialVals,
                    (short) -1);
            Assert.assertEquals("Should have returned 2 partition names", 2, partialNames.size());
            Assert.assertTrue("Not all part names returned", partialNames.containsAll(partNames));

            // Verify escaped partition names don't return partitions
            exceptionThrown = false;
            try {
                String badPartName = "ds=2008-07-01 14%3A13%3A12/hrs=14";
                client.getPartition(databaseName, tblName, badPartName);
            } catch (NoSuchObjectException e) {
                exceptionThrown = true;
            }
            Assert.assertTrue("Bad partition spec should have thrown an exception", exceptionThrown);

            Path partPath = new Path(part.getSd().getLocation());


            Assert.assertTrue(fs.exists(partPath));
            client.dropPartition(databaseName, tblName, part.getValues(), true);
            Assert.assertFalse(fs.exists(partPath));

            // Test append_partition_by_name
            client.appendPartition(databaseName, tblName, partName);
            Partition part5 = client.getPartition(databaseName, tblName, part.getValues());
            Assert.assertTrue("Append partition by name failed", part5.getValues().equals(vals));
            ;
            Path part5Path = new Path(part5.getSd().getLocation());
            Assert.assertTrue(fs.exists(part5Path));

            // Test drop_partition_by_name
            Assert.assertTrue("Drop partition by name failed",
                    client.dropPartition(databaseName, tblName, partName, true));
            Assert.assertFalse(fs.exists(part5Path));

            // add the partition again so that drop table with a partition can be
            // tested
            retp = client.add_partition(part);
            Assert.assertNotNull("Unable to create partition " + part, retp);
            Assert.assertEquals(dbPermission, fs.getFileStatus(new Path(retp.getSd().getLocation()))
                    .getPermission());

            // test add_partitions

            List<String> mvals1 = makeVals("2008-07-04 14:13:12", "14641");
            List<String> mvals2 = makeVals("2008-07-04 14:13:12", "14642");
            List<String> mvals3 = makeVals("2008-07-04 14:13:12", "14643");
            List<String> mvals4 = makeVals("2008-07-04 14:13:12", "14643"); // equal to 3
            List<String> mvals5 = makeVals("2008-07-04 14:13:12", "14645");

            Exception savedException;

            // add_partitions(empty list) : ok, normal operation
            client.add_partitions(new ArrayList<Partition>());

            // add_partitions(1,2,3) : ok, normal operation
            Partition mpart1 = makePartitionObject(databaseName, tblName, mvals1, tbl, "/mpart1");
            Partition mpart2 = makePartitionObject(databaseName, tblName, mvals2, tbl, "/mpart2");
            Partition mpart3 = makePartitionObject(databaseName, tblName, mvals3, tbl, "/mpart3");
            client.add_partitions(Arrays.asList(mpart1, mpart2, mpart3));

            // do DDL time munging if thrift mode
            adjust(mpart1, databaseName, tblName);
            adjust(mpart2, databaseName, tblName);
            adjust(mpart3, databaseName, tblName);
            verifyPartitionsPublished(databaseName, tblName,
                    Arrays.asList(mvals1.get(0)),
                    Arrays.asList(mpart1, mpart2, mpart3));

            Partition mpart4 = makePartitionObject(databaseName, tblName, mvals4, tbl, "/mpart4");
            Partition mpart5 = makePartitionObject(databaseName, tblName, mvals5, tbl, "/mpart5");

            // create dir for /mpart5
            Path mp5Path = new Path(mpart5.getSd().getLocation());
            warehouse.mkdirs(mp5Path, true);
            Assert.assertTrue(fs.exists(mp5Path));
            Assert.assertEquals(dbPermission, fs.getFileStatus(mp5Path).getPermission());

            // add_partitions(5,4) : err = duplicate keyvals on mpart4
            savedException = null;
            try {
                client.add_partitions(Arrays.asList(mpart5, mpart4));
            } catch (Exception e) {
                savedException = e;
            } finally {
                Assert.assertNotNull(savedException);
            }

            // check that /mpart4 does not exist, but /mpart5 still does.
            Assert.assertTrue(fs.exists(mp5Path));
            Assert.assertFalse(fs.exists(new Path(mpart4.getSd().getLocation())));

            // add_partitions(5) : ok
            client.add_partitions(Arrays.asList(mpart5));

            // do DDL time munging if thrift mode
            adjust(mpart5, databaseName, tblName);

            verifyPartitionsPublished(databaseName, tblName,
                    Arrays.asList(mvals1.get(0)),
                    Arrays.asList(mpart1, mpart2, mpart3, mpart5));

            //// end add_partitions tests

            client.dropTable(databaseName, tblName);

            //client.dropType(typeName);

            // recreate table as external, drop partition and it should
            // still exist
            tbl.setParameters(new HashMap<String, String>());
            tbl.getParameters().put("EXTERNAL", "TRUE");
            client.createTable(tbl);
            retp = client.add_partition(part);
            Assert.assertTrue(fs.exists(partPath));
            client.dropPartition(databaseName, tblName, part.getValues(), true);
            Assert.assertTrue(fs.exists(partPath));

            for (String tableName : client.getTables(databaseName, "*")) {
                client.dropTable(databaseName, tableName);
            }

            client.dropDatabase(databaseName);

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testPartition() failed.");
            throw e;
        }
    }

    private StorageDescriptor createStorageDescriptor(String tableName,
                                                      List<FieldSchema> cols, Map<String, String> params, Map<String, String> serdParams) {
        StorageDescriptor sd = new StorageDescriptor();

        sd.setCols(cols);
        sd.setCompressed(false);
        sd.setNumBuckets(1);
        sd.setParameters(params);
        sd.setBucketCols(new ArrayList<String>(2));
        sd.getBucketCols().add("name");
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tableName);
        sd.getSerdeInfo().setParameters(serdParams);
        sd.getSerdeInfo().getParameters()
                .put(serdeConstants.SERIALIZATION_FORMAT, "1");
        sd.setSortCols(new ArrayList<Order>());
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        sd.setInputFormat(HiveInputFormat.class.getName());
        sd.setOutputFormat(HiveOutputFormat.class.getName());

        return sd;
    }

    private Table createTable(String dbName, String tblName, String owner,
                              Map<String, String> tableParams, Map<String, String> partitionKeys,
                              StorageDescriptor sd, int lastAccessTime) throws Exception {
        Table tbl = new Table();
        tbl.setDbName(dbName);
        tbl.setTableName(tblName);
        if (tableParams != null) {
            tbl.setParameters(tableParams);
        }

        if (owner != null) {
            tbl.setOwner(owner);
        }

        if (partitionKeys != null) {
            tbl.setPartitionKeys(new ArrayList<FieldSchema>(partitionKeys.size()));
            for (String key : partitionKeys.keySet()) {
                tbl.getPartitionKeys().add(
                        new FieldSchema(key, partitionKeys.get(key), ""));
            }
        }

        tbl.setSd(sd);
        tbl.setLastAccessTime(lastAccessTime);
        tbl.setTableType(TableType.MANAGED_TABLE.toString());

        client.createTable(tbl);

        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and ALTER TABLE relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);

        return tbl;
    }

    private List<Partition> createPartitions(String dbName, Table tbl,
                                             List<List<String>> values) throws Throwable {
        int i = 1;
        List<Partition> partitions = new ArrayList<Partition>();
        for (List<String> vals : values) {
            Partition part = makePartitionObject(dbName, tbl.getTableName(), vals, tbl, "/part" + i);
            i++;
            // check if the partition exists (it shouldn't)
            boolean exceptionThrown = false;
            try {
                Partition p = client.getPartition(dbName, tbl.getTableName(), vals);
            } catch (Exception e) {
                Assert.assertEquals("partition should not have existed",
                        NoSuchObjectException.class, e.getClass());
                exceptionThrown = true;
            }
            Assert.assertTrue("getPartition() should have thrown NoSuchObjectException", exceptionThrown);
            Partition retp = client.add_partition(part);
            Assert.assertNotNull("Unable to create partition " + part, retp);
            partitions.add(retp);
        }
        return partitions;
    }

    private Type createType(String typeName, Map<String, String> fields) throws Throwable {
        Type typ1 = new Type();
        typ1.setName(typeName);
        typ1.setFields(new ArrayList<FieldSchema>(fields.size()));
        for (String fieldName : fields.keySet()) {
            typ1.getFields().add(
                    new FieldSchema(fieldName, fields.get(fieldName), ""));
        }
        //client.createType(typ1);
        return typ1;
    }

    private void createTable(String dbName, String tableName)
            throws Exception {
        List<FieldSchema> columns = new ArrayList<FieldSchema>();
        columns.add(new FieldSchema("foo", "string", ""));
        columns.add(new FieldSchema("bar", "string", ""));

        Map<String, String> serdParams = new HashMap<String, String>();
        serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

        StorageDescriptor sd = createStorageDescriptor(tableName, columns, null, serdParams);

        createTable(dbName, tableName, null, null, null, sd, 0);
    }

    private void createMultiPartitionTableSchema(String dbName, String tblName,
                                                 String typeName, List<List<String>> values)
            throws Throwable, MetaException, TException, NoSuchObjectException {

        Map<String, String> fields = new HashMap<String, String>();
        fields.put("name", serdeConstants.STRING_TYPE_NAME);
        fields.put("income", serdeConstants.INT_TYPE_NAME);

        Type typ1 = createType(typeName, fields);

        Map<String, String> partitionKeys = new HashMap<String, String>();
        partitionKeys.put("ds", serdeConstants.STRING_TYPE_NAME);
        partitionKeys.put("hr", serdeConstants.STRING_TYPE_NAME);

        Map<String, String> params = new HashMap<String, String>();
        params.put("test_param_1", "Use this for comments etc");

        Map<String, String> serdParams = new HashMap<String, String>();
        serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

        StorageDescriptor sd = createStorageDescriptor(tblName, typ1.getFields(), params, serdParams);

        Table tbl = createTable(dbName, tblName, null, null, partitionKeys, sd, 0);
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);

        createPartitions(dbName, tbl, values);
    }

    @Test
    public void testListPartitionsWithMultiplePartitions() throws Throwable {
        // create a table with multiple partitions
        String tblName = "comptbl";
        String typeName = "Person";
        List<List<String>> values = new ArrayList<List<String>>();
        values.add(makeVals("2008-07-01 14:13:12", "14"));
        values.add(makeVals("2008-07-01 14:13:12", "15"));
        values.add(makeVals("2008-07-02 14:13:12", "15"));
        values.add(makeVals("2008-07-03 14:13:12", "151"));

        createMultiPartitionTableSchema(databaseName, tblName, typeName, values);

        List<Partition> partitions = client.listPartitions(databaseName, tblName, (short) -1);
        Assert.assertNotNull("should have returned partitions", partitions);
        Assert.assertEquals(" should have returned " + values.size() +
                " partitions", values.size(), partitions.size());

        partitions = client.listPartitions(databaseName, tblName, (short) (values.size() / 2));

        Assert.assertNotNull("should have returned partitions", partitions);
        Assert.assertEquals(" should have returned " + values.size() / 2 +
                " partitions", values.size() / 2, partitions.size());


        partitions = client.listPartitions(databaseName, tblName, (short) (values.size() * 2));

        Assert.assertNotNull("should have returned partitions", partitions);
        Assert.assertEquals(" should have returned " + values.size() +
                " partitions", values.size(), partitions.size());
        client.dropTable(databaseName, tblName);

    }

    @Test
    public void testListPartitionsWihtLimitEnabled() throws Throwable {
        // create a table with multiple partitions
        String tblName = "comptbl";
        String typeName = "Person";

        // Create too many partitions, just enough to validate over limit requests
        List<List<String>> values = new ArrayList<List<String>>();
        for (int i = 0; i < DEFAULT_LIMIT_PARTITION_REQUEST + 1; i++) {
            values.add(makeVals("2008-07-01 14:13:12", Integer.toString(i)));
        }

        createMultiPartitionTableSchema(databaseName, tblName, typeName, values);

        List<Partition> partitions;
        short maxParts;

        // Requesting more partitions than allowed should throw an exception
        try {
            maxParts = -1;
            partitions = client.listPartitions(databaseName, tblName, maxParts);
            Assert.assertTrue(partitions.size() == DEFAULT_LIMIT_PARTITION_REQUEST + 1);
            //not support limit now
            //fail("should have thrown MetaException about partition limit");
        } catch (MetaException e) {
            Assert.assertTrue(true);
        }

        // Requesting more partitions than allowed should throw an exception
        try {
            maxParts = DEFAULT_LIMIT_PARTITION_REQUEST + 1;
            partitions = client.listPartitions(databaseName, tblName, maxParts);
            Assert.assertTrue(partitions.size() == DEFAULT_LIMIT_PARTITION_REQUEST + 1);
//            fail("should have thrown MetaException about partition limit");
        } catch (MetaException e) {
            Assert.assertTrue(true);
        }

        // Requesting less partitions than allowed should work
        maxParts = DEFAULT_LIMIT_PARTITION_REQUEST / 2;
        partitions = client.listPartitions(databaseName, tblName, maxParts);
        Assert.assertNotNull("should have returned partitions", partitions);
        Assert.assertEquals(" should have returned 50 partitions", maxParts, partitions.size());
        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testAlterViewParititon() throws Throwable {
        String tblName = "comptbl";
        String viewName = "compView";

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

        Table tbl = new Table();
        tbl.setDbName(databaseName);
        tbl.setTableName(tblName);
        StorageDescriptor sd = new StorageDescriptor();
        tbl.setSd(sd);
        sd.setCols(cols);
        sd.setCompressed(false);
        sd.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tbl.getTableName());
        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
        sd.getSerdeInfo().getParameters()
                .put(serdeConstants.SERIALIZATION_FORMAT, "1");
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        sd.setInputFormat(HiveInputFormat.class.getName());
        sd.setOutputFormat(HiveOutputFormat.class.getName());
        sd.setSortCols(new ArrayList<Order>());

        client.createTable(tbl);

        //if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(databaseName, tblName);
        //}

        ArrayList<FieldSchema> viewCols = new ArrayList<FieldSchema>(1);
        viewCols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

        ArrayList<FieldSchema> viewPartitionCols = new ArrayList<FieldSchema>(1);
        viewPartitionCols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));

        Table view = new Table();
        view.setDbName(databaseName);
        view.setTableName(viewName);
        view.setTableType(TableType.VIRTUAL_VIEW.name());
        view.setPartitionKeys(viewPartitionCols);
        view.setViewOriginalText("SELECT income, name FROM " + tblName);
        view.setViewExpandedText("SELECT `" + tblName + "`.`income`, `" + tblName +
                "`.`name` FROM `" + databaseName + "`.`" + tblName + "`");
        view.setRewriteEnabled(false);
        StorageDescriptor viewSd = new StorageDescriptor();
        view.setSd(viewSd);
        viewSd.setCols(viewCols);
        viewSd.setCompressed(false);
        viewSd.setParameters(new HashMap<String, String>());
        viewSd.setSerdeInfo(new SerDeInfo());
        viewSd.getSerdeInfo().setParameters(new HashMap<String, String>());

        client.createTable(view);

        //if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        view = client.getTable(databaseName, viewName);
        //}

        List<String> vals = new ArrayList<String>(1);
        vals.add("abc");

        Partition part = new Partition();
        part.setDbName(databaseName);
        part.setTableName(viewName);
        part.setValues(vals);
        part.setParameters(new HashMap<String, String>());

        client.add_partition(part);

        Partition part2 = client.getPartition(databaseName, viewName, part.getValues());

        part2.getParameters().put("a", "b");

        client.alter_partition(databaseName, viewName, part2, null);

        Partition part3 = client.getPartition(databaseName, viewName, part.getValues());
        Assert.assertEquals("couldn't view alter partition", part3.getParameters().get(
                "a"), "b");

        client.dropTable(databaseName, tblName);
        client.dropTable(databaseName, viewName);

    }

    @Test
    public void testAlterPartition() throws Throwable {

        try {
            String tblName = "comptbl";
            List<String> vals = new ArrayList<String>(2);
            vals.add("2008-07-01");
            vals.add("14");

            ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
            cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

            Table tbl = new Table();
            tbl.setDbName(databaseName);
            tbl.setTableName(tblName);
            StorageDescriptor sd = new StorageDescriptor();
            tbl.setSd(sd);
            sd.setCols(cols);
            sd.setCompressed(false);
            sd.setNumBuckets(1);
            sd.setParameters(new HashMap<String, String>());
            sd.getParameters().put("test_param_1", "Use this for comments etc");
            sd.setBucketCols(new ArrayList<String>(2));
            sd.getBucketCols().add("name");
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(tbl.getTableName());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());
            sd.getSerdeInfo().getParameters()
                    .put(serdeConstants.SERIALIZATION_FORMAT, "1");
            sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
            sd.setInputFormat(HiveInputFormat.class.getName());
            sd.setOutputFormat(HiveOutputFormat.class.getName());
            sd.setSortCols(new ArrayList<Order>());

            tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
            tbl.getPartitionKeys().add(
                    new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
            tbl.getPartitionKeys().add(
                    new FieldSchema("hr", serdeConstants.INT_TYPE_NAME, ""));

            client.createTable(tbl);

            //if (isThriftClient) {
            // the createTable() above does not update the location in the 'tbl'
            // object when the client is a thrift client and the code below relies
            // on the location being present in the 'tbl' object - so get the table
            // from the metastore
            tbl = client.getTable(databaseName, tblName);
            //}

            Partition part = new Partition();
            part.setDbName(databaseName);
            part.setTableName(tblName);
            part.setValues(vals);
            part.setParameters(new HashMap<String, String>());
            part.setSd(tbl.getSd());
            part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
            part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");

            client.add_partition(part);

            Partition part2 = client.getPartition(databaseName, tblName, part.getValues());

            part2.getParameters().put("retention", "10");
            part2.getSd().setNumBuckets(12);
            part2.getSd().getSerdeInfo().getParameters().put("abc", "1");
            client.alter_partition(databaseName, tblName, part2, null);

            Partition part3 = client.getPartition(databaseName, tblName, part.getValues());
            Assert.assertEquals("couldn't alter partition", part3.getParameters().get(
                    "retention"), "10");
            Assert.assertEquals("couldn't alter partition", part3.getSd().getSerdeInfo()
                    .getParameters().get("abc"), "1");
            Assert.assertEquals("couldn't alter partition", part3.getSd().getNumBuckets(),
                    12);
            client.dropTable(databaseName, tblName);

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testPartition() failed.");
            throw e;
        }

    }

    @Test
    public void testRenamePartitionExctend() throws Throwable {

        try {
            String tblName = "comptbl1";
            List<String> vals = new ArrayList<String>(2);
            vals.add("2011-07-11");
            vals.add("8");
            String part_path = "/ds=2011-07-11/hr=8";
            List<String> tmp_vals = new ArrayList<String>(2);
            tmp_vals.add("tmp_2011-07-11");
            tmp_vals.add("-8");
            String part2_path = "/ds=tmp_2011-07-11/hr=-8";

            ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
            cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

            Table tbl = new Table();
            tbl.setDbName(databaseName);
            tbl.setTableName(tblName);
            StorageDescriptor sd = new StorageDescriptor();
            tbl.setSd(sd);
            sd.setCols(cols);
            sd.setCompressed(false);
            sd.setNumBuckets(1);
            sd.setParameters(new HashMap<String, String>());
            sd.getParameters().put("test_param_1", "Use this for comments etc");
            sd.setBucketCols(new ArrayList<String>(2));
            sd.getBucketCols().add("name");
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(tbl.getTableName());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());
            sd.getSerdeInfo().getParameters()
                    .put(serdeConstants.SERIALIZATION_FORMAT, "1");
            sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
            sd.setInputFormat(HiveInputFormat.class.getName());
            sd.setOutputFormat(HiveOutputFormat.class.getName());
            sd.setSortCols(new ArrayList<Order>());

            tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
            tbl.getPartitionKeys().add(
                    new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
            tbl.getPartitionKeys().add(
                    new FieldSchema("hr", serdeConstants.INT_TYPE_NAME, ""));

            client.createTable(tbl);

            //if (isThriftClient) {
            // the createTable() above does not update the location in the 'tbl'
            // object when the client is a thrift client and the code below relies
            // on the location being present in the 'tbl' object - so get the table
            // from the metastore
            tbl = client.getTable(databaseName, tblName);
            //}

            Partition part = new Partition();
            part.setDbName(databaseName);
            part.setTableName(tblName);
            part.setValues(vals);
            part.setParameters(new HashMap<String, String>());
            part.setSd(tbl.getSd().deepCopy());
            part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
            part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");
            part.getParameters().put("retention", "10");
            part.getSd().setNumBuckets(12);
            part.getSd().getSerdeInfo().getParameters().put("abc", "1");

            client.add_partition(part);

            part.setValues(tmp_vals);
            client.renamePartition(databaseName, tblName, vals, part);

            boolean exceptionThrown = false;
            try {
                Partition p = client.getPartition(databaseName, tblName, vals);
            } catch (Exception e) {
                Assert.assertEquals("partition should not have existed",
                        NoSuchObjectException.class, e.getClass());
                exceptionThrown = true;
            }
            Assert.assertTrue("Expected NoSuchObjectException", exceptionThrown);

            Partition part3 = client.getPartition(databaseName, tblName, tmp_vals);
            Assert.assertEquals("couldn't rename partition", part3.getParameters().get(
                    "retention"), "10");
            Assert.assertEquals("couldn't rename partition", part3.getSd().getSerdeInfo()
                    .getParameters().get("abc"), "1");
            Assert.assertEquals("couldn't rename partition", part3.getSd().getNumBuckets(),
                    12);
            Assert.assertEquals("new partition sd matches", part3.getSd().getLocation(),
                    tbl.getSd().getLocation() + part2_path);

            part.setValues(vals);
            client.renamePartition(databaseName, tblName, tmp_vals, part);

            exceptionThrown = false;
            try {
                Partition p = client.getPartition(databaseName, tblName, tmp_vals);
            } catch (Exception e) {
                Assert.assertEquals("partition should not have existed",
                        NoSuchObjectException.class, e.getClass());
                exceptionThrown = true;
            }
            Assert.assertTrue("Expected NoSuchObjectException", exceptionThrown);

            part3 = client.getPartition(databaseName, tblName, vals);
            Assert.assertEquals("couldn't rename partition", part3.getParameters().get(
                    "retention"), "10");
            Assert.assertEquals("couldn't rename partition", part3.getSd().getSerdeInfo()
                    .getParameters().get("abc"), "1");
            Assert.assertEquals("couldn't rename partition", part3.getSd().getNumBuckets(),
                    12);
            Assert.assertEquals("new partition sd matches", part3.getSd().getLocation(),
                    tbl.getSd().getLocation() + part_path);

            client.dropTable(databaseName, tblName);
        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testRenamePartition() failed.");
            throw e;
        }
    }

    private void checkFilter(String dbName,
                             String tblName, String filter, int expectedCount)
            throws MetaException, NoSuchObjectException, TException {
        List<Partition> partitions = client.listPartitionsByFilter(dbName,
                tblName, filter, (short) -1);

        Assert.assertEquals("Partition count expected for filter " + filter,
                expectedCount, partitions.size());
    }


    private void add_partition(Table table,
                               List<String> vals, String location) throws InvalidObjectException,
            AlreadyExistsException, MetaException, TException {

        Partition part = new Partition();
        part.setDbName(table.getDbName());
        part.setTableName(table.getTableName());
        part.setValues(vals);
        part.setParameters(new HashMap<String, String>());
        part.setSd(table.getSd().deepCopy());
        part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
        part.getSd().setLocation(table.getSd().getLocation() + location);

        client.add_partition(part);
    }

    @Test
    public void testPartitionFilter() throws Exception {
        String tblName = "filtertbl";

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

        ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(3);
        partCols.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));
        partCols.add(new FieldSchema("p2", serdeConstants.STRING_TYPE_NAME, ""));
        partCols.add(new FieldSchema("p3", serdeConstants.INT_TYPE_NAME, ""));
        partCols.add(new FieldSchema("p4", serdeConstants.DATE_TYPE_NAME, ""));

        Table tbl = new Table();
        tbl.setDbName(databaseName);
        tbl.setTableName(tblName);
        StorageDescriptor sd = new StorageDescriptor();
        tbl.setSd(sd);
        sd.setCols(cols);
        sd.setCompressed(false);
        sd.setNumBuckets(1);
        sd.setParameters(new HashMap<String, String>());
        sd.setBucketCols(new ArrayList<String>());
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tbl.getTableName());
        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
        sd.getSerdeInfo().getParameters()
                .put(serdeConstants.SERIALIZATION_FORMAT, "1");
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
        sd.setInputFormat(HiveInputFormat.class.getName());
        sd.setOutputFormat(HiveOutputFormat.class.getName());
        sd.setSortCols(new ArrayList<Order>());

        tbl.setPartitionKeys(partCols);
        client.createTable(tbl);

        tbl = client.getTable(databaseName, tblName);

        add_partition(tbl, Lists.newArrayList("p11", "p21", "31", "2020-01-01"), "part1");
        add_partition(tbl, Lists.newArrayList("p11", "p22", "32", "2020-01-02"), "part2");
        add_partition(tbl, Lists.newArrayList("p12", "p21", "31", "2020-01-03"), "part3");
        add_partition(tbl, Lists.newArrayList("p12", "p23", "32", "2020-01-04"), "part4");
        add_partition(tbl, Lists.newArrayList("p13", "p24", "31", "2020-01-05"), "part5");
        add_partition(tbl, Lists.newArrayList("p13", "p25", "-33", "2020-01-06"), "part6");
        add_partition(tbl, Lists.newArrayList("p13", "p25", "SDF", "2020-01-07"), "part7");
        add_partition(tbl, Lists.newArrayList("p13", "p25", "__HIVE_DEFAULT_PARTITION__", "2020-01-08"), "part7");
        add_partition(tbl, Lists.newArrayList("__HIVE_DEFAULT_PARTITION__", "p25", "__HIVE_DEFAULT_PARTITION__",
                "__HIVE_DEFAULT_PARTITION__"), "part8");

        // Test equals operator for strings and integers.
        checkFilter(databaseName, tblName, "p1 = \"__HIVE_DEFAULT_PARTITION__\"", 1);
        checkFilter(databaseName, tblName, "p1 = \"p11\"", 2);
        checkFilter(databaseName, tblName, "p1 = \"p12\"", 2);
        checkFilter(databaseName, tblName, "p2 = \"p21\"", 2);
        checkFilter(databaseName, tblName, "p2 = \"p23\"", 1);
        checkFilter(databaseName, tblName, "p3 = 31", 3);
        checkFilter(databaseName, tblName, "p3 = 33", 0);
        checkFilter(databaseName, tblName, "p3 = -33", 1);
        checkFilter(databaseName, tblName, "p1 = \"p11\" and p2=\"p22\"", 1);
        checkFilter(databaseName, tblName, "p1 = \"p11\" or p2=\"p23\"", 3);
        checkFilter(databaseName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
        checkFilter(databaseName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
        checkFilter(databaseName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
        checkFilter(databaseName, tblName, "p1 = \"p11\" and p3 = 31", 1);
        checkFilter(databaseName, tblName, "p3 = -33 or p1 = \"p12\"", 3);

        // Test equals operator for date composite
        checkFilter(databaseName, tblName, "p3 = -33 or p1 = \"p12\" or p4 = '2020-01-01'", 4);
        checkFilter(databaseName, tblName, "p3 = -33 and p4 = '2020-01-06'", 1);
        checkFilter(databaseName, tblName, "p3 = -33 and p4 = '2020-01-01'", 0);

        // Test not-equals operator for strings and integers.
        checkFilter(databaseName, tblName, "p1 != \"p11\"", 7);
        checkFilter(databaseName, tblName, "p2 != \"p23\"", 8);
        checkFilter(databaseName, tblName, "p2 != \"p33\"", 9);
        checkFilter(databaseName, tblName, "p3 != 32", 4);
        checkFilter(databaseName, tblName, "p3 != 8589934592", 6);
        checkFilter(databaseName, tblName, "p1 != \"p11\" and p1 != \"p12\"", 5);
        checkFilter(databaseName, tblName, "p1 != \"p11\" and p2 != \"p22\"", 7);
        checkFilter(databaseName, tblName, "p1 != \"p11\" or p2 != \"p22\"", 8);
        checkFilter(databaseName, tblName, "p1 != \"p12\" and p2 != \"p25\"", 3);
        checkFilter(databaseName, tblName, "p1 != \"p12\" or p2 != \"p25\"", 9);
        checkFilter(databaseName, tblName, "p3 != -33 or p1 != \"p13\"", 5);
        checkFilter(databaseName, tblName, "p1 != \"p11\" and p3 = 31", 2);
        checkFilter(databaseName, tblName, "p3 != 31 and p1 = \"p12\"", 1);

        // Test not-equals operator for date composite
        checkFilter(databaseName, tblName, "p3 != 31 and p1 = \"p12\" and p4 = '2020-01-04'", 1);
        checkFilter(databaseName, tblName, "p3 != 31 and p1 = \"p12\" and p4 = '2020-01-05'", 0);
        checkFilter(databaseName, tblName, "p3 != -33 and p4 = '2020-01-06'", 0);
        checkFilter(databaseName, tblName, "p3 != -33 and p4 = '2020-01-01'", 1);
        checkFilter(databaseName, tblName, "p3 != -33 or p4 = '2020-01-01'", 5);

        // Test reverse order.
        checkFilter(databaseName, tblName, "31 != p3 and p1 = \"p12\"", 1);
        checkFilter(databaseName, tblName, "\"p23\" = p2", 1);

        // Test and/or more...
        checkFilter(databaseName, tblName,
                "p1 = \"p11\" or (p1=\"p12\" and p2=\"p21\") or p4 =\"2020-01-07\"", 4);
        checkFilter(databaseName, tblName,
                "p1 = \"p11\" or (p1=\"p12\" and p2=\"p21\") Or " +
                        "(p1=\"p13\" aNd p2=\"p24\")", 4);
        //test for and or precedence
        checkFilter(databaseName, tblName,
                "p1=\"p12\" and (p2=\"p27\" Or p2=\"p21\")", 1);
        checkFilter(databaseName, tblName,
                "p1=\"p12\" and p2=\"p27\" Or p2=\"p21\"", 2);

        // Test gt/lt/lte/gte/like for strings.
        checkFilter(databaseName, tblName, "p1 like \"__HIVE_DEFAULT_PARTITION__\"", 0);
        checkFilter(databaseName, tblName, "p1 > \"p12\"", 4);
        checkFilter(databaseName, tblName, "p1 >= \"p12\"", 6);
        checkFilter(databaseName, tblName, "p1 < \"p12\"", 3);
        checkFilter(databaseName, tblName, "p1 <= \"p12\"", 5);
        checkFilter(databaseName, tblName, "p1 like \"p1_%\"", 8);
        checkFilter(databaseName, tblName, "p2 like \"p_%3\"", 1);

        // Test gt/lt/lte/gte for numbers.
        checkFilter(databaseName, tblName, "p3 < 0", 1);
        checkFilter(databaseName, tblName, "p3 >= -33", 6);
        checkFilter(databaseName, tblName, "p3 > -33", 5);
        checkFilter(databaseName, tblName, "p3 > 31 and p3 < 32", 0);
        checkFilter(databaseName, tblName, "p3 > 31 or p3 < 31", 3);
        checkFilter(databaseName, tblName, "p3 > 30 or p3 < 30", 6);
        checkFilter(databaseName, tblName, "p3 >= 31 or p3 < -32", 6);
        checkFilter(databaseName, tblName, "p3 >= 32", 2);
        checkFilter(databaseName, tblName, "p3 > 32", 0);

        // Test between
        checkFilter(databaseName, tblName, "p1 between \"p11\" and \"p12\"", 4);
        checkFilter(databaseName, tblName, "p1 not between \"p11\" and \"p12\"", 5);
        checkFilter(databaseName, tblName, "p3 not between 0 and 2", 6);
        checkFilter(databaseName, tblName, "p3 between 31 and 32", 5);
        checkFilter(databaseName, tblName, "p3 between 32 and 31", 0);
        checkFilter(databaseName, tblName, "p3 between 31 and 32 and p4 between \"2020-01-01\" and \"2020-01-03\"", 3);

        checkFilter(databaseName, tblName, "p3 between -32 and 34 and p3 not between 31 and 32", 0);
        checkFilter(databaseName, tblName, "p3 between 1 and 3 or p3 not between 1 and 3", 6);
        checkFilter(databaseName, tblName,
                "p3 between 31 and 32 and p1 between \"p12\" and \"p14\"", 3);

        //Test for setting the maximum partition count
        List<Partition> partitions = client.listPartitionsByFilter(databaseName,
                tblName, "p1 >= \"p12\"", (short) 2);
        Assert.assertEquals("User specified row limit for partitions",
                2, partitions.size());

        //Negative tests
        Exception me = null;
        try {
            client.listPartitionsByFilter(databaseName,
                    tblName, "p3 >= \"p12\"", (short) -1);
        } catch (TException e) {
            me = e;
        }
        Assert.assertNotNull(me);
        Assert.assertTrue("Filter on int partition key", me.getMessage().contains(
                "Filtering is supported only on partition keys of type string"));

        me = null;
        try {
            client.listPartitionsByFilter(databaseName,
                    tblName, "c1 >= \"p12\"", (short) -1);
        } catch (TException e) {
            me = e;
        }
        Assert.assertNotNull(me);
        Assert.assertTrue("Filter on invalid key", me.getMessage().contains(
                "<c1> is not a partitioning key for the table"));

        me = null;
        try {
            client.listPartitionsByFilter(databaseName,
                    tblName, "c1 >= ", (short) -1);
        } catch (TException e) {
            me = e;
        }
        Assert.assertNotNull(me);
        Assert.assertTrue("Invalid filter string", me.getMessage().contains(
                "Error parsing partition filter"));

        me = null;
        try {
            client.listPartitionsByFilter("invDBName",
                    "invTableName", "p1 = \"p11\"", (short) -1);
        } catch (NoSuchObjectException e) {
            me = e;
        }
        Assert.assertNotNull(me);
        Assert.assertTrue("NoSuchObject exception", me.getMessage().contains("Table not found - invtablename") || me.getMessage().contains("Table 'invdbname.invtablename' does not exist"));
        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testFilterSingleStringPartition() throws Exception {
        String tblName = "testFilterSingleStringPartition";

        List<String> vals = new ArrayList<String>(1);
        vals.add("p11");
        List<String> vals2 = new ArrayList<String>(1);
        vals2.add("p12");
        List<String> vals3 = new ArrayList<String>(1);
        vals3.add("p13");
        List<String> vals4 = new ArrayList<String>(1);
        vals4.add("__HIVE_DEFAULT_PARTITION__");

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

        ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
        partCols.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));

        Table tbl = TestUtil.buildTable(databaseName, tblName);
        tbl.setPartitionKeys(partCols);
        client.createTable(tbl);
        tbl = client.getTable(databaseName, tblName);

        add_partition(tbl, vals, "part1");
        add_partition(tbl, vals2, "part2");
        add_partition(tbl, vals3, "part3");
        add_partition(tbl, vals4, "part3");

        checkFilter(databaseName, tblName, "p1 = \"p12\"", 1);
        checkFilter(databaseName, tblName, "p1 < \"p12\"", 2);
        checkFilter(databaseName, tblName, "p1 > \"p12\"", 1);
        checkFilter(databaseName, tblName, "p1 >= \"p12\"", 2);
        checkFilter(databaseName, tblName, "p1 <= \"p12\"", 3);
        checkFilter(databaseName, tblName, "p1 <> \"p12\"", 3);
        checkFilter(databaseName, tblName, "p1 like \"p1_%\"", 3);
        checkFilter(databaseName, tblName, "p1 like \"p_%2\"", 1);

        ExprBuilder e = new ExprBuilder(tblName);
        checkExpr(1, databaseName, tblName, e.val("p12").strCol("p1").pred("=", 2).build());
        checkExpr(2, databaseName, tblName, e.val("p12").strCol("p1").pred("<", 2).build());
        checkExpr(1, databaseName, tblName, e.val("p12").strCol("p1").pred(">", 2).build());
        checkExpr(2, databaseName, tblName, e.val("p12").strCol("p1").pred(">=", 2).build());
        checkExpr(3, databaseName, tblName, e.val("p12").strCol("p1").pred("<=", 2).build());
        checkExpr(3, databaseName, tblName, e.val("p12").strCol("p1").pred("<>", 2).build());
        checkExpr(3, databaseName, tblName, e.val("p1_%").strCol("p1").pred("like", 2).build());
        checkExpr(1, databaseName, tblName, e.val("p_%2").strCol("p1").pred("like", 2).build());

        checkExpr(1, databaseName, tblName, e.strCol("p1").pred("isnull", 1).build());
        checkExpr(3, databaseName, tblName, e.strCol("p1").pred("isnotnull", 1).build());

        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testFilterSingleIntegerPartition() throws Exception {
        String tblName = "testFilterSingleIntegerPartition";

        List<String> vals = new ArrayList<String>(1);
        vals.add("31");
        List<String> vals2 = new ArrayList<String>(1);
        vals2.add("32");
        List<String> vals3 = new ArrayList<String>(1);
        vals3.add("-33");
        List<String> vals4 = new ArrayList<String>(1);
        vals4.add("SDF");
        List<String> vals5 = new ArrayList<String>(1);
        vals5.add("__HIVE_DEFAULT_PARTITION__");
        List<String> vals6 = new ArrayList<String>(1);
        vals6.add("34");

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

        ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
        partCols.add(new FieldSchema("p1", serdeConstants.INT_TYPE_NAME, ""));

        Table tbl = TestUtil.buildTable(databaseName, tblName);
        tbl.setPartitionKeys(partCols);
        client.createTable(tbl);
        tbl = client.getTable(databaseName, tblName);

        add_partition(tbl, vals, "part1");
        add_partition(tbl, vals2, "part2");
        add_partition(tbl, vals3, "part3");
        add_partition(tbl, vals4, "part3");
        add_partition(tbl, vals5, "part3");
        add_partition(tbl, vals6, "part3");

        checkFilter(databaseName, tblName, "p1 = 32", 1);
        checkFilter(databaseName, tblName, "p1 < 32", 2);
        checkFilter(databaseName, tblName, "p1 > 32", 1);
        checkFilter(databaseName, tblName, "p1 >= 32", 2);
        checkFilter(databaseName, tblName, "p1 <= 32", 3);
        checkFilter(databaseName, tblName, "p1 <> 32", 3);

        //reverse test
        checkFilter(databaseName, tblName, "32 = p1", 1);
        checkFilter(databaseName, tblName, "32 > p1", 2);
        checkFilter(databaseName, tblName, "32 < p1", 1);
        checkFilter(databaseName, tblName, "32 <= p1", 2);
        checkFilter(databaseName, tblName, "32 >= p1", 3);
        checkFilter(databaseName, tblName, "32 <> p1", 3);

        Assert.assertThrows(TException.class, () -> checkFilter(databaseName, tblName, "p1 like \"p1_%\"", 3));
        Assert.assertThrows(TException.class, () -> checkFilter(databaseName, tblName, "p1 like \"p_%2\"", 1));

        ExprBuilder e = new ExprBuilder(tblName);
        checkExpr(1, databaseName, tblName, e.val(32).strCol("p1").pred("=", 2).build());
        checkExpr(2, databaseName, tblName, e.val(32).strCol("p1").pred("<", 2).build());
        checkExpr(1, databaseName, tblName, e.val(32).strCol("p1").pred(">", 2).build());
        checkExpr(2, databaseName, tblName, e.val(32).strCol("p1").pred(">=", 2).build());
        checkExpr(3, databaseName, tblName, e.val(32).strCol("p1").pred("<=", 2).build());
        checkExpr(3, databaseName, tblName, e.val(32).strCol("p1").pred("<>", 2).build());

        // Unknown partition(DLF as the same like MYSQL will Return Unknown
        checkExpr(1, databaseName, tblName, e.val("p1_%").strCol("p1").pred("like", 2).build());
        checkExpr(1, databaseName, tblName, e.val("p_%2").strCol("p1").pred("like", 2).build());
        checkExpr(2, databaseName, tblName, e.val("%2").strCol("p1").pred("like", 2).build());

        checkExpr(2, databaseName, tblName, e.strCol("p1").pred("isnull", 1).build());
        checkExpr(4, databaseName, tblName, e.strCol("p1").pred("isnotnull", 1).build());

        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testFilterSingleDatePartition() throws Exception {
        String tblName = "testFilterSingleDatePartition";

        List<String> vals = new ArrayList<String>(1);
        vals.add("2020-01-01");
        List<String> vals2 = new ArrayList<String>(1);
        vals2.add("2020-02-03");
        List<String> vals3 = new ArrayList<String>(1);
        vals3.add("2019-01-01");
        List<String> vals4 = new ArrayList<String>(1);
        vals4.add("2019-01-02");
        List<String> vals5 = new ArrayList<String>(1);
        vals5.add("sdf");
        List<String> vals6 = new ArrayList<String>(1);
        vals6.add("__HIVE_DEFAULT_PARTITION__");
        List<String> vals7 = new ArrayList<String>(1);
        vals7.add("2020");

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

        ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
        partCols.add(new FieldSchema("p1", serdeConstants.DATE_TYPE_NAME, ""));

        Table tbl = TestUtil.buildTable(databaseName, tblName);
        tbl.setPartitionKeys(partCols);
        client.createTable(tbl);
        tbl = client.getTable(databaseName, tblName);

        add_partition(tbl, vals, "part1");
        add_partition(tbl, vals2, "part2");
        add_partition(tbl, vals3, "part3");
        add_partition(tbl, vals4, "part1");
        add_partition(tbl, vals5, "part2");
        add_partition(tbl, vals6, "part3");
        add_partition(tbl, vals7, "part3");

        checkFilter(databaseName, tblName, "p1 = \"2020-01-01\"", 1);
        checkFilter(databaseName, tblName, "p1 < \"2020-01-01\"", 2);
        checkFilter(databaseName, tblName, "p1 <= \"2020-01-01\"", 3);
        checkFilter(databaseName, tblName, "p1 <> \"2020-01-01\"", 3);
        checkFilter(databaseName, tblName, "", 7);

        // reverse test
        checkFilter(databaseName, tblName, "\"2020-01-01\" = p1", 1);
        checkFilter(databaseName, tblName, "\"2020-01-01\" > p1 ", 2);
        checkFilter(databaseName, tblName, "\"2020-01-01\" >= p1", 3);
        checkFilter(databaseName, tblName, "\"2020-01-01\" <> p1", 3);
        checkFilter(databaseName, tblName, "", 7);

        // exception test
        Assert.assertThrows(TException.class, () -> checkFilter(databaseName, tblName, "p1 like \"p_%2\"", 1));
        Assert.assertThrows(TException.class, () -> checkFilter(databaseName, tblName, "p1 > \"sdf\"", 0));
        Assert.assertThrows(TException.class, () -> checkFilter(databaseName, tblName, "p1 >= \"sdf\"", 0));

        ExprBuilder e = new ExprBuilder(tblName);
        checkExpr(1, databaseName, tblName, e.val("2020-01-01").strCol("p1").pred("=", 2).build());
        checkExpr(2, databaseName, tblName, e.val("2020-01-01").strCol("p1").pred("<", 2).build());
        checkExpr(3, databaseName, tblName, e.val("2020-01-01").strCol("p1").pred("<=", 2).build());
        checkExpr(3, databaseName, tblName, e.val("2020-01-01").strCol("p1").pred("<>", 2).build());

        // Unknown partition(DLF(will Return Unknown) VS MYSQL(will not))
        checkExpr(2, databaseName, tblName, e.val("p1_%").strCol("p1").pred("like", 2).build());
        checkExpr(2, databaseName, tblName, e.val("p_%2").strCol("p1").pred("like", 2).build());
        checkExpr(3, databaseName, tblName, e.val("%2").strCol("p1").pred("like", 2).build());

        checkExpr(3, databaseName, tblName, e.strCol("p1").pred("isnull", 1).build());
        checkExpr(4, databaseName, tblName, e.strCol("p1").pred("isnotnull", 1).build());

        client.dropTable(databaseName, tblName);
    }

    /**
     * Test filtering based on the value of the last partition
     *
     * @throws Exception
     */
    @Test
    public void testFilterLastPartition() throws Exception {
        String tblName = "filtertbl";

        List<String> vals = new ArrayList<String>(2);
        vals.add("p11");
        vals.add("p21");
        List<String> vals2 = new ArrayList<String>(2);
        vals2.add("p11");
        vals2.add("p22");
        List<String> vals3 = new ArrayList<String>(2);
        vals3.add("p12");
        vals3.add("p21");

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

        ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(2);
        partCols.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));
        partCols.add(new FieldSchema("p2", serdeConstants.STRING_TYPE_NAME, ""));

        Map<String, String> serdParams = new HashMap<String, String>();
        serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");
        StorageDescriptor sd = createStorageDescriptor(tblName, cols, null, serdParams);

        Table tbl = new Table();
        tbl.setDbName(databaseName);
        tbl.setTableName(tblName);
        tbl.setSd(sd);
        tbl.setPartitionKeys(partCols);
        client.createTable(tbl);
        tbl = client.getTable(databaseName, tblName);

        add_partition(tbl, vals, "part1");
        add_partition(tbl, vals2, "part2");
        add_partition(tbl, vals3, "part3");

        checkFilter(databaseName, tblName, "p2 = \"p21\"", 2);
        checkFilter(databaseName, tblName, "p2 < \"p23\"", 3);
        checkFilter(databaseName, tblName, "p2 > \"p21\"", 1);
        checkFilter(databaseName, tblName, "p2 >= \"p21\"", 3);
        checkFilter(databaseName, tblName, "p2 <= \"p21\"", 2);
        checkFilter(databaseName, tblName, "p2 <> \"p12\"", 3);
        checkFilter(databaseName, tblName, "p2 != \"p12\"", 3);
        checkFilter(databaseName, tblName, "p2 like \"p2%\"", 3);
        checkFilter(databaseName, tblName, "p2 like \"p%2\"", 1);
        checkFilter(databaseName, tblName, "p2 like \"p%2%\"", 3);
        checkFilter(databaseName, tblName, "p2 like \"p_%2%\"", 1);
        checkFilter(databaseName, tblName, "p2 like \"p_%2_\"", 0);

        // test escape
        List<String> vals4 = new ArrayList<String>(2);
        vals4.add("p12");
        vals4.add("p%1");
        add_partition(tbl, vals4, "part3");
        checkFilter(databaseName, tblName, "p2 like \"p\\%1\"", 1);

        try {
            checkFilter(databaseName, tblName, "p2 !< 'dd'", 0);
            fail("Invalid operator not detected");
        } catch (TException e) {
            // expected exception due to lexer error
            System.out.println("error in lex");
        }
        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testTableColumnStatistics() throws TException {
        // create table
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = new HashMap<>();
        columns.put("ID", "int");
        columns.put("NAME", "string");
        columns.put("BIRTH", "date");
        columns.put("HEIGHT", "double");
        columns.put("WEIGHT", "decimal(10,3)");
        columns.put("IS_MALE", "boolean");
        columns.put("SHOPPING", "binary");
        Map<String, String> partitionKeys = ImmutableMap.of("DS", "string", "ID2", "string");
        Table table = TestUtil.getPartitionTable(databaseName, tblName, columns, partitionKeys);
        // create the table
        client.createTable(table);
        // add partitions
        String partitionName = "DS=20201130/ID2=12A";
        Partition partition = TestUtil.buildPartition(databaseName, tblName, table, 1, Lists.newArrayList("20201130", "12A"));
        Partition addResult = client.add_partition(partition);
        // update statistics
        ColumnStatistics columnStatistics = new ColumnStatistics();
        ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc();
        columnStatisticsDesc.setIsTblLevel(false);
        columnStatisticsDesc.setPartName(partitionName);
        columnStatisticsDesc.setTableName(tblName.toUpperCase());
        columnStatisticsDesc.setDbName(databaseName.toUpperCase());
        columnStatisticsDesc.setLastAnalyzed(System.currentTimeMillis() / 1000);
        List<ColumnStatisticsObj> columnStatisticsObjs = new ArrayList<>();
        ColumnStatisticsObj longColumnStatisticsObj = createLongColumnStatisticsObj();
        columnStatisticsObjs.add(longColumnStatisticsObj);
        ColumnStatisticsObj stringColumnStatisticsObj = createStringColumnStatisticsObj();
        columnStatisticsObjs.add(stringColumnStatisticsObj);
        ColumnStatisticsObj dateColumnStatisticsObj = createDateColumnStatisticsObj();
        columnStatisticsObjs.add(dateColumnStatisticsObj);
        ColumnStatisticsObj doubleColumnStatisticsObj = createDoubleColumnStatisticsObj();
        columnStatisticsObjs.add(doubleColumnStatisticsObj);
        ColumnStatisticsObj decimalColumnStatisticsObj = createDecimalColumnStatisticsObj();
        columnStatisticsObjs.add(decimalColumnStatisticsObj);
        ColumnStatisticsObj booleanColumnStatisticsObj = createBooleanColumnStatisticsObj();
        columnStatisticsObjs.add(booleanColumnStatisticsObj);
        ColumnStatisticsObj binaryColumnStatisticsObj = createBinaryColumnStatisticsObj();
        columnStatisticsObjs.add(binaryColumnStatisticsObj);
        columnStatistics.setStatsDesc(columnStatisticsDesc);
        columnStatistics.setStatsObj(columnStatisticsObjs);

        //client.updatePartitionColumnStatistics(columnStatistics);
        SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
        request.setColStats(ImmutableList.of(columnStatistics));
        client.setPartitionColumnStatistics(request);

        // get statistics
        List<String> colNames = new ArrayList<>();
        colNames.add("ID");
        colNames.add("NAME");
        colNames.add("BIRTH");
        colNames.add("HEIGHT");
        colNames.add("WEIGHT");
        colNames.add("IS_MALE");
        colNames.add("SHOPPING");
        Map<String, List<ColumnStatisticsObj>> statisticsObjMap = client.getPartitionColumnStatistics(databaseName, tblName, ImmutableList.of(partitionName), colNames);
        // partition name assert
        Assert.assertTrue(statisticsObjMap.containsKey(Utils.lowerCaseConvertPartName(partitionName)));
        List<ColumnStatisticsObj> statisticsObjs = statisticsObjMap.get(Utils.lowerCaseConvertPartName(partitionName));
        for (ColumnStatisticsObj obj : statisticsObjs) {
            if ("id".equals(obj.getColName())) {
                assertLongEqual(obj, longColumnStatisticsObj);
            } else if ("name".equals(obj.getColName())) {
                assertStringEqual(obj, stringColumnStatisticsObj);
            } else if ("birth".equals(obj.getColName())) {
                assertDateEqual(obj, dateColumnStatisticsObj);
            } else if ("height".equals(obj.getColName())) {
                assertDoubleEqual(obj, doubleColumnStatisticsObj);
            } else if ("weight".equals(obj.getColName())) {
                assertDecimalEqual(obj, decimalColumnStatisticsObj);
            } else if ("is_male".equals(obj.getColName())) {
                assertBooleanEqual(obj, booleanColumnStatisticsObj);
            } else if ("shopping".equals(obj.getColName())) {
                assertBinaryEqual(obj, binaryColumnStatisticsObj);
            } else {
                Assert.assertTrue(1 == 0);
            }
        }
        // delete statistics
        for (String colName : colNames) {
            boolean ret = client.deletePartitionColumnStatistics(databaseName, tblName, partitionName, colName);
            Assert.assertTrue("it's ok to delete", ret);
        }
        // get statistics
        Map<String, List<ColumnStatisticsObj>> statisticsObjsAfterDelete = client.getPartitionColumnStatistics(databaseName, tblName, ImmutableList.of(partitionName), colNames);
        Assert.assertTrue("after delete to get stats size:" + statisticsObjsAfterDelete.size(), statisticsObjsAfterDelete.size() == 0);
        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testStatsMerge() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = new HashMap<>();
        columns.put("A", "string");
        Map<String, String> partitionKeys = ImmutableMap.of("DS", "string");
        Table table = TestUtil.getPartitionTable(databaseName, tblName, columns, partitionKeys);
        // create the table
        client.createTable(table);

        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, databaseName, tblName);
        cs.setStatsDesc(desc);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("a");
        obj.setColType("string");
        ColumnStatisticsData data = new ColumnStatisticsData();
        StringColumnStatsData scsd = new StringColumnStatsData();
        scsd.setAvgColLen(10);
        scsd.setMaxColLen(20);
        scsd.setNumNulls(30);
        scsd.setNumDVs(123);
        scsd.setBitVectors("{0, 4, 5, 7}{0, 1}{0, 1, 2}{0, 1, 4}{0}{0, 2}{0, 3}{0, 2, 3, 4}{0, 1, 4}{0, 1}{0}{0, 1, 3, 8}{0, 2}{0, 2}{0, 9}{0, 1, 4}");
        data.setStringStats(scsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);

        List<ColumnStatistics> colStats = new ArrayList<>();
        colStats.add(cs);

        SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStats);
        client.setPartitionColumnStatistics(request);

        List<String> colNames = new ArrayList<>();
        colNames.add("A");

        StringColumnStatsData getScsd = client.getTableColumnStatistics(databaseName, tblName, colNames).get(0)
                .getStatsData().getStringStats();
        assertEquals(getScsd.getNumDVs(), 123);

        cs = new ColumnStatistics();
        scsd = new StringColumnStatsData();
        scsd.setAvgColLen(20);
        scsd.setMaxColLen(5);
        scsd.setNumNulls(70);
        scsd.setNumDVs(456);
        scsd.setBitVectors("{0, 1}{0, 1}{1, 2, 4}{0, 1, 2}{0, 1, 2}{0, 2}{0, 1, 3, 4}{0, 1}{0, 1}{3, 4, 6}{2}{0, 1}{0, 3}{0}{0, 1}{0, 1, 4}");
        data.setStringStats(scsd);
        obj.setStatsData(data);
        cs.addToStatsObj(obj);

        request = new SetPartitionsStatsRequest(colStats);
        request.setNeedMerge(true);
        client.setPartitionColumnStatistics(request);

        getScsd = client.getTableColumnStatistics(databaseName, tblName, colNames).get(0)
                .getStatsData().getStringStats();
        assertTrue(Math.abs(getScsd.getAvgColLen() - 20.0) <= 0.01);
        assertEquals(getScsd.getMaxColLen(), 20);
        assertEquals(getScsd.getNumNulls(), 100);
        // since metastore is ObjectStore, we use the max function to merge.
        assertEquals(getScsd.getNumDVs(), 456);
        client.dropTable(databaseName, tblName);
    }

    public void assertLongEqual(ColumnStatisticsObj obj, ColumnStatisticsObj longColumnStatisticsObj) {
        Assert.assertTrue("longColName", obj.getColName().equals(longColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("longColType:", obj.getColType().equals(longColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("longDataType:", obj.getStatsData().isSetLongStats());
        LongColumnStatsData getLongColumnStatsData = obj.getStatsData().getLongStats();
        Assert.assertTrue("longBitVectors", getLongColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("longHighValue", getLongColumnStatsData.getHighValue() == longColumnStatisticsObj.getStatsData().getLongStats().getHighValue());
        Assert.assertTrue("longLowValue", getLongColumnStatsData.getLowValue() == longColumnStatisticsObj.getStatsData().getLongStats().getLowValue());
        Assert.assertTrue("longNumDVs", getLongColumnStatsData.getNumDVs() == longColumnStatisticsObj.getStatsData().getLongStats().getNumDVs());
        Assert.assertTrue("longNumNulls", getLongColumnStatsData.getNumNulls() == longColumnStatisticsObj.getStatsData().getLongStats().getNumNulls());
    }

    public void assertStringEqual(ColumnStatisticsObj obj, ColumnStatisticsObj stringColumnStatisticsObj) {
        Assert.assertTrue("stringColName", obj.getColName().equals(stringColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("stringColType:", obj.getColType().equals(stringColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("stringDataType:", obj.getStatsData().isSetStringStats());
        StringColumnStatsData getStringColumnStatsData = obj.getStatsData().getStringStats();
        Assert.assertTrue("stringBitVector", getStringColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("stringMaxColLen", getStringColumnStatsData.getMaxColLen() == stringColumnStatisticsObj.getStatsData().getStringStats().getMaxColLen());
        Assert.assertTrue("stringAvgColLen", Math.abs(getStringColumnStatsData.getAvgColLen() - stringColumnStatisticsObj.getStatsData().getStringStats().getAvgColLen()) <= 0.01);
        Assert.assertTrue("stringNumDVs", getStringColumnStatsData.getNumDVs() == stringColumnStatisticsObj.getStatsData().getStringStats().getNumDVs());
        Assert.assertTrue("stringNumNulls", getStringColumnStatsData.getNumNulls() == stringColumnStatisticsObj.getStatsData().getStringStats().getNumNulls());
    }

    public void assertDateEqual(ColumnStatisticsObj obj, ColumnStatisticsObj dateColumnStatisticsObj) {

        Assert.assertTrue("dateColName", obj.getColName().equals(dateColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("dateColType:", obj.getColType().equals(dateColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("dateDataType:", obj.getStatsData().isSetDateStats());
        DateColumnStatsData getDateColumnStatsData = obj.getStatsData().getDateStats();
        Assert.assertTrue(getDateColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("dateLowValue", getDateColumnStatsData.getLowValue().equals(dateColumnStatisticsObj.getStatsData().getDateStats().getLowValue()));
        Assert.assertTrue("dateHighValue", getDateColumnStatsData.getHighValue().equals(dateColumnStatisticsObj.getStatsData().getDateStats().getHighValue()));
        Assert.assertTrue("dateNumDVs", getDateColumnStatsData.getNumDVs() == dateColumnStatisticsObj.getStatsData().getDateStats().getNumDVs());
        Assert.assertTrue("dateNumNulls", getDateColumnStatsData.getNumNulls() == dateColumnStatisticsObj.getStatsData().getDateStats().getNumNulls());
    }

    public void assertDoubleEqual(ColumnStatisticsObj obj, ColumnStatisticsObj doubleColumnStatisticsObj) {

        Assert.assertTrue("doubleColName", obj.getColName().equals(doubleColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("doubleColType:", obj.getColType().equals(doubleColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("doubleDataType:", obj.getStatsData().isSetDoubleStats());
        DoubleColumnStatsData getDoubleColumnStatsData = obj.getStatsData().getDoubleStats();
        Assert.assertTrue("doubleBitVectors", getDoubleColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("doubleLowValue", Math.abs(getDoubleColumnStatsData.getLowValue() - doubleColumnStatisticsObj.getStatsData().getDoubleStats().getLowValue()) <= 0.01);
        Assert.assertTrue("doubleHighValue", Math.abs(getDoubleColumnStatsData.getHighValue() - doubleColumnStatisticsObj.getStatsData().getDoubleStats().getHighValue()) <= 0.01);
        Assert.assertTrue("doubleNumDVs", getDoubleColumnStatsData.getNumDVs() == doubleColumnStatisticsObj.getStatsData().getDoubleStats().getNumDVs());
        Assert.assertTrue("doubleNumNulls", getDoubleColumnStatsData.getNumNulls() == doubleColumnStatisticsObj.getStatsData().getDoubleStats().getNumNulls());

    }

    public void assertDecimalEqual(ColumnStatisticsObj obj, ColumnStatisticsObj decimalColumnStatisticsObj) {

        Assert.assertTrue("decimalColName", obj.getColName().equals(decimalColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("decimalColType:", obj.getColType().equals(decimalColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("decimalDataType:", obj.getStatsData().isSetDecimalStats());
        DecimalColumnStatsData getDecimalColumnStatsData = obj.getStatsData().getDecimalStats();
        Assert.assertTrue("decimalBitVectors", getDecimalColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("decimalLowValue", getDecimalColumnStatsData.getLowValue().equals(decimalColumnStatisticsObj.getStatsData().getDecimalStats().getLowValue()));
        Assert.assertTrue("decimalHighValue", getDecimalColumnStatsData.getHighValue().equals(decimalColumnStatisticsObj.getStatsData().getDecimalStats().getHighValue()));
        Assert.assertTrue("decimalNumDvs", getDecimalColumnStatsData.getNumDVs() == decimalColumnStatisticsObj.getStatsData().getDecimalStats().getNumDVs());
        Assert.assertTrue("decimalNumNulls", getDecimalColumnStatsData.getNumNulls() == decimalColumnStatisticsObj.getStatsData().getDecimalStats().getNumNulls());

    }

    public void assertBooleanEqual(ColumnStatisticsObj obj, ColumnStatisticsObj booleanColumnStatisticsObj) {

        Assert.assertTrue("booleanColName", obj.getColName().equals(booleanColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("booleanColType:", obj.getColType().equals(booleanColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("booleanDataType:", obj.getStatsData().isSetBooleanStats());
        BooleanColumnStatsData getBooleanColumnStatsData = obj.getStatsData().getBooleanStats();
        Assert.assertTrue("booleanBitVectors", getBooleanColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("booleanNumFalses", getBooleanColumnStatsData.getNumFalses() == booleanColumnStatisticsObj.getStatsData().getBooleanStats().getNumFalses());
        Assert.assertTrue("booleanNumTrues", getBooleanColumnStatsData.getNumTrues() == booleanColumnStatisticsObj.getStatsData().getBooleanStats().getNumTrues());
        Assert.assertTrue("booleanNumNulls", getBooleanColumnStatsData.getNumNulls() == booleanColumnStatisticsObj.getStatsData().getBooleanStats().getNumNulls());

    }

    public void assertBinaryEqual(ColumnStatisticsObj obj, ColumnStatisticsObj binaryColumnStatisticsObj) {
        Assert.assertTrue("binaryColName", obj.getColName().equals(binaryColumnStatisticsObj.getColName().toLowerCase()));
        Assert.assertTrue("binaryColType:", obj.getColType().equals(binaryColumnStatisticsObj.getColType().toLowerCase()));
        Assert.assertTrue("binaryDataType:", obj.getStatsData().isSetBinaryStats());
        BinaryColumnStatsData getBinaryColumnStatsData = obj.getStatsData().getBinaryStats();
        Assert.assertTrue("binaryBitVectors", getBinaryColumnStatsData.getBitVectors() == null);
        Assert.assertTrue("binaryMaxColLen", getBinaryColumnStatsData.getMaxColLen() == binaryColumnStatisticsObj.getStatsData().getBinaryStats().getMaxColLen());
        Assert.assertTrue("binaryAvgColLen", Math.abs(getBinaryColumnStatsData.getAvgColLen() - binaryColumnStatisticsObj.getStatsData().getBinaryStats().getAvgColLen()) <= 0.01);
        Assert.assertTrue("binaryNumNulls", getBinaryColumnStatsData.getNumNulls() == binaryColumnStatisticsObj.getStatsData().getBinaryStats().getNumNulls());

    }

    public ColumnStatisticsObj createLongColumnStatisticsObj() {
        ColumnStatisticsObj longColumnStatisticsObj = new ColumnStatisticsObj();
        longColumnStatisticsObj.setColName("ID");
        longColumnStatisticsObj.setColType("INT");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
        longColumnStatsData.setBitVectors(null);
        longColumnStatsData.setLowValue(1);
        longColumnStatsData.setHighValue(5);
        longColumnStatsData.setNumDVs(20);
        longColumnStatsData.setNumNulls(20);
        columnStatisticsData.setLongStats(longColumnStatsData);
        longColumnStatisticsObj.setStatsData(columnStatisticsData);
        return longColumnStatisticsObj;
    }

    public ColumnStatisticsObj createStringColumnStatisticsObj() {
        ColumnStatisticsObj stringColumnStatisticsObj = new ColumnStatisticsObj();
        stringColumnStatisticsObj.setColName("NAME");
        stringColumnStatisticsObj.setColType("STRING");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
        stringColumnStatsData.setBitVectors(null);
        stringColumnStatsData.setMaxColLen(100);
        stringColumnStatsData.setAvgColLen(50);
        stringColumnStatsData.setNumDVs(21);
        stringColumnStatsData.setNumNulls(21);
        columnStatisticsData.setStringStats(stringColumnStatsData);
        stringColumnStatisticsObj.setStatsData(columnStatisticsData);
        return stringColumnStatisticsObj;
    }

    public ColumnStatisticsObj createDateColumnStatisticsObj() {
        ColumnStatisticsObj dateColumnStatisticsObj = new ColumnStatisticsObj();
        dateColumnStatisticsObj.setColName("BIRTH");
        dateColumnStatisticsObj.setColType("DATE");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData();
        dateColumnStatsData.setBitVectors(null);
        dateColumnStatsData.setLowValue(new Date(18590));
        dateColumnStatsData.setHighValue(new Date(18585));
        dateColumnStatsData.setNumDVs(22);
        dateColumnStatsData.setNumNulls(22);
        columnStatisticsData.setDateStats(dateColumnStatsData);
        dateColumnStatisticsObj.setStatsData(columnStatisticsData);
        return dateColumnStatisticsObj;
    }

    public ColumnStatisticsObj createDoubleColumnStatisticsObj() {
        ColumnStatisticsObj doubleColumnStatisticsObj = new ColumnStatisticsObj();
        doubleColumnStatisticsObj.setColName("HEIGHT");
        doubleColumnStatisticsObj.setColType("DOUBLE");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setBitVectors(null);
        doubleColumnStatsData.setLowValue(170.15);
        doubleColumnStatsData.setHighValue(190.23);
        doubleColumnStatsData.setNumDVs(23);
        doubleColumnStatsData.setNumNulls(23);
        columnStatisticsData.setDoubleStats(doubleColumnStatsData);
        doubleColumnStatisticsObj.setStatsData(columnStatisticsData);
        return doubleColumnStatisticsObj;
    }

    public ColumnStatisticsObj createDecimalColumnStatisticsObj() {
        ColumnStatisticsObj decimalColumnStatisticsObj = new ColumnStatisticsObj();
        decimalColumnStatisticsObj.setColName("WEIGHT");
        decimalColumnStatisticsObj.setColType("DECIMAL(10,3)");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData();
        decimalColumnStatsData.setBitVectors(null);
        decimalColumnStatsData.setLowValue(new Decimal(ByteBuffer.wrap(new BigDecimal("128.888").unscaledValue().toByteArray()), (short) 3));
        decimalColumnStatsData.setHighValue(new Decimal(ByteBuffer.wrap(new BigDecimal("178.888").unscaledValue().toByteArray()), (short) 3));
        decimalColumnStatsData.setNumDVs(24);
        decimalColumnStatsData.setNumNulls(24);
        columnStatisticsData.setDecimalStats(decimalColumnStatsData);
        decimalColumnStatisticsObj.setStatsData(columnStatisticsData);
        return decimalColumnStatisticsObj;
    }

    public ColumnStatisticsObj createBooleanColumnStatisticsObj() {
        ColumnStatisticsObj booleanColumnStatisticsObj = new ColumnStatisticsObj();
        booleanColumnStatisticsObj.setColName("IS_MALE");
        booleanColumnStatisticsObj.setColType("BOOLEAN");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setBitVectors(null);
        booleanColumnStatsData.setNumFalses(100);
        booleanColumnStatsData.setNumTrues(50);
        booleanColumnStatsData.setNumNulls(20);
        columnStatisticsData.setBooleanStats(booleanColumnStatsData);
        booleanColumnStatisticsObj.setStatsData(columnStatisticsData);
        return booleanColumnStatisticsObj;
    }

    public ColumnStatisticsObj createBinaryColumnStatisticsObj() {
        ColumnStatisticsObj binaryColumnStatisticsObj = new ColumnStatisticsObj();
        binaryColumnStatisticsObj.setColName("SHOPPING");
        binaryColumnStatisticsObj.setColType("BINARY");
        ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
        BinaryColumnStatsData binaryColumnStatsData = new BinaryColumnStatsData();
        binaryColumnStatsData.setBitVectors(null);
        binaryColumnStatsData.setMaxColLen(100);
        binaryColumnStatsData.setAvgColLen(50);
        binaryColumnStatsData.setNumNulls(26);
        columnStatisticsData.setBinaryStats(binaryColumnStatsData);
        binaryColumnStatisticsObj.setStatsData(columnStatisticsData);
        return binaryColumnStatisticsObj;
    }

    @Test
    public void testSimpleTable() throws Exception {
        try {
            String tblName = "simptbl";
            String tblName2 = "simptbl2";
            String typeName = "Person";

            client.dropTable(databaseName, tblName);
            Type typ1 = new Type();
            typ1.setName(typeName);
            typ1.setFields(new ArrayList<FieldSchema>(2));
            typ1.getFields().add(
                    new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            typ1.getFields().add(
                    new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));
//            client.createType(typ1);

            Table tbl = new Table();
            tbl.setDbName(databaseName);
            tbl.setTableName(tblName);
            StorageDescriptor sd = new StorageDescriptor();
            tbl.setSd(sd);
            sd.setCols(typ1.getFields());
            sd.setCompressed(false);
            sd.setNumBuckets(1);
            sd.setParameters(new HashMap<String, String>());
            sd.getParameters().put("test_param_1", "Use this for comments etc");
            sd.setBucketCols(new ArrayList<String>(2));
            sd.getBucketCols().add("name");
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(tbl.getTableName());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());
            sd.getSerdeInfo().getParameters().put(
                    org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");
            sd.getSerdeInfo().setSerializationLib(
                    org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
            sd.setInputFormat(HiveInputFormat.class.getName());
            sd.setInputFormat(HiveOutputFormat.class.getName());

            tbl.setPartitionKeys(new ArrayList<FieldSchema>());

            client.createTable(tbl);

            //if (isThriftClient) {
            // the createTable() above does not update the location in the 'tbl'
            // object when the client is a thrift client and the code below relies
            // on the location being present in the 'tbl' object - so get the table
            // from the metastore
            tbl = client.getTable(databaseName, tblName);
            //}

            Table tbl2 = client.getTable(databaseName, tblName);
            assertNotNull(tbl2);
            assertEquals(tbl2.getDbName(), databaseName);
            assertEquals(tbl2.getTableName(), tblName);
            assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
            assertEquals(tbl2.getSd().isCompressed(), false);
            assertEquals(tbl2.getSd().getNumBuckets(), 1);
            assertEquals(tbl2.getSd().getLocation(), tbl.getSd().getLocation());
            assertNotNull(tbl2.getSd().getSerdeInfo());
            sd.getSerdeInfo().setParameters(new HashMap<String, String>());
            sd.getSerdeInfo().getParameters().put(
                    org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");

            tbl2.setTableName(tblName2);
            tbl2.setParameters(new HashMap<String, String>());
            tbl2.getParameters().put("EXTERNAL", "TRUE");
            tbl2.getSd().setLocation(tbl.getSd().getLocation() + "-2");

            List<FieldSchema> fieldSchemas = client.getFields(databaseName, tblName);
            assertNotNull(fieldSchemas);
            assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
            for (FieldSchema fs : tbl.getSd().getCols()) {
                assertTrue(fieldSchemas.contains(fs));
            }

            List<FieldSchema> fieldSchemasFull = client.getSchema(databaseName, tblName);
            assertNotNull(fieldSchemasFull);
            assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size()
                    + tbl.getPartitionKeys().size());
            for (FieldSchema fs : tbl.getSd().getCols()) {
                assertTrue(fieldSchemasFull.contains(fs));
            }
            for (FieldSchema fs : tbl.getPartitionKeys()) {
                assertTrue(fieldSchemasFull.contains(fs));
            }

            client.createTable(tbl2);
            //if (isThriftClient) {
            tbl2 = client.getTable(tbl2.getDbName(), tbl2.getTableName());
            //}

            Table tbl3 = client.getTable(databaseName, tblName2);
            assertNotNull(tbl3);
            assertEquals(tbl3.getDbName(), databaseName);
            assertEquals(tbl3.getTableName(), tblName2);
            assertEquals(tbl3.getSd().getCols().size(), typ1.getFields().size());
            assertEquals(tbl3.getSd().isCompressed(), false);
            assertEquals(tbl3.getSd().getNumBuckets(), 1);
            assertEquals(tbl3.getSd().getLocation(), tbl2.getSd().getLocation());
            assertEquals(tbl3.getParameters(), tbl2.getParameters());

            fieldSchemas = client.getFields(databaseName, tblName2);
            assertNotNull(fieldSchemas);
            assertEquals(fieldSchemas.size(), tbl2.getSd().getCols().size());
            for (FieldSchema fs : tbl2.getSd().getCols()) {
                assertTrue(fieldSchemas.contains(fs));
            }

            fieldSchemasFull = client.getSchema(databaseName, tblName2);
            assertNotNull(fieldSchemasFull);
            assertEquals(fieldSchemasFull.size(), tbl2.getSd().getCols().size()
                    + tbl2.getPartitionKeys().size());
            for (FieldSchema fs : tbl2.getSd().getCols()) {
                assertTrue(fieldSchemasFull.contains(fs));
            }
            for (FieldSchema fs : tbl2.getPartitionKeys()) {
                assertTrue(fieldSchemasFull.contains(fs));
            }

            assertEquals("Use this for comments etc", tbl2.getSd().getParameters()
                    .get("test_param_1"));
            assertEquals("name", tbl2.getSd().getBucketCols().get(0));
            assertTrue("Partition key list is not empty",
                    (tbl2.getPartitionKeys() == null)
                            || (tbl2.getPartitionKeys().size() == 0));

            //test get_table_objects_by_name functionality
            ArrayList<String> tableNames = new ArrayList<String>();
            tableNames.add(tblName2);
            tableNames.add(tblName);
            tableNames.add(tblName2);
            List<Table> foundTables = client.getTableObjectsByName(databaseName, tableNames);

            assertEquals(2, foundTables.size());
            for (Table t : foundTables) {
                if (t.getTableName().equals(tblName2)) {
                    assertEquals(t.getSd().getLocation(), tbl2.getSd().getLocation());
                } else {
                    assertEquals(t.getTableName(), tblName);
                    assertEquals(t.getSd().getLocation(), tbl.getSd().getLocation());
                }
                assertEquals(t.getSd().getCols().size(), typ1.getFields().size());
                assertEquals(t.getSd().isCompressed(), false);
                assertEquals(foundTables.get(0).getSd().getNumBuckets(), 1);
                assertNotNull(t.getSd().getSerdeInfo());
                assertEquals(t.getDbName(), databaseName);
            }

            tableNames.add(1, "table_that_doesnt_exist");
            foundTables = client.getTableObjectsByName(databaseName, tableNames);
            assertEquals(foundTables.size(), 2);

            InvalidOperationException ioe = null;
            try {
                foundTables = client.getTableObjectsByName(databaseName, null);
            } catch (InvalidOperationException e) {
                ioe = e;
            }
            assertNotNull(ioe);
            assertTrue("Table not found", ioe.getMessage().contains("null tables"));

            UnknownDBException udbe = null;
            try {
                foundTables = client.getTableObjectsByName("db_that_doesnt_exist", tableNames);
            } catch (UnknownDBException e) {
                udbe = e;
            }
            assertNotNull(udbe);
            assertTrue("DB not found", udbe.getMessage().contains("not find database db_that_doesnt_exist"));

            udbe = null;
            try {
                foundTables = client.getTableObjectsByName("", tableNames);
            } catch (UnknownDBException e) {
                udbe = e;
            }
            assertNotNull(udbe);
            assertTrue("DB not found", udbe.getMessage().contains("is null or empty"));

            FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), hiveConf);
            client.dropTable(databaseName, tblName);
            assertFalse(fs.exists(new Path(tbl.getSd().getLocation())));

            client.dropTable(databaseName, tblName2);
            assertTrue(fs.exists(new Path(tbl2.getSd().getLocation())));

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testSimpleTable() failed.");
            throw e;
        }
    }

    // Tests that in the absence of stats for partitions, and/or absence of columns
    // to get stats for, the metastore does not break. See HIVE-12083 for motivation.
    @Test
    public void testStatsFastTrivial() throws Throwable {
        String tblName = "t1";
        String tblOwner = "statstester";
        String typeName = "Person";
        int lastAccessed = 12083;

        List<List<String>> values = new ArrayList<List<String>>();
        values.add(makeVals("2008-07-01 14:13:12", "14A"));
        values.add(makeVals("2008-07-01 14:13:12", "15A"));
        values.add(makeVals("2008-07-02 14:13:12", "15A"));
        values.add(makeVals("2008-07-03 14:13:12", "15A1"));

        createMultiPartitionTableSchema(databaseName, tblName, typeName, values);

        List<String> emptyColNames = new ArrayList<String>();
        List<String> emptyPartNames = new ArrayList<String>();

        List<String> colNames = new ArrayList<String>();
        colNames.add("name".toUpperCase());
        colNames.add("income".toUpperCase());
        List<String> partNames = client.listPartitionNames(databaseName, tblName, (short) -1);

        assertEquals(0, emptyColNames.size());
        assertEquals(0, emptyPartNames.size());
        assertEquals(2, colNames.size());
        assertEquals(4, partNames.size());

        // Test for both colNames and partNames being empty:
        AggrStats aggrStatsEmpty = client.getAggrColStatsFor(databaseName.toUpperCase(), tblName.toUpperCase(), emptyColNames, emptyPartNames);
        assertNotNull(aggrStatsEmpty); // short-circuited on client-side, verifying that it's an empty object, not null
        assertEquals(0, aggrStatsEmpty.getPartsFound());
        assertNotNull(aggrStatsEmpty.getColStats());
        assert (aggrStatsEmpty.getColStats().isEmpty());

        // Test for only colNames being empty
        AggrStats aggrStatsOnlyParts = client.getAggrColStatsFor(databaseName.toUpperCase(), tblName.toUpperCase(), emptyColNames, partNames);
        assertNotNull(aggrStatsOnlyParts); // short-circuited on client-side, verifying that it's an empty object, not null
        assertEquals(0, aggrStatsOnlyParts.getPartsFound());
        assertNotNull(aggrStatsOnlyParts.getColStats());
        assert (aggrStatsOnlyParts.getColStats().isEmpty());

        // Test for only partNames being empty
        AggrStats aggrStatsOnlyCols = client.getAggrColStatsFor(databaseName.toUpperCase(), tblName.toUpperCase(), colNames, emptyPartNames);
        assertNotNull(aggrStatsOnlyCols); // short-circuited on client-side, verifying that it's an empty object, not null
        assertEquals(0, aggrStatsOnlyCols.getPartsFound());
        assertNotNull(aggrStatsOnlyCols.getColStats());
        assert (aggrStatsOnlyCols.getColStats().isEmpty());

        // Test for valid values for both.
        AggrStats aggrStatsFull = client.getAggrColStatsFor(databaseName.toUpperCase(), tblName.toUpperCase(), colNames.stream().map(c -> c.toUpperCase()).collect(Collectors.toList()), partNames.stream().map(p -> p.replace("ds", "DS")).collect(Collectors.toList()));
        assertNotNull(aggrStatsFull);
        assertEquals(0, aggrStatsFull.getPartsFound()); // would still be empty, because no stats are actually populated.
        assertNotNull(aggrStatsFull.getColStats());
        assert (aggrStatsFull.getColStats().isEmpty());
        client.dropTable(databaseName, tblName);
    }

    @Test
    public void testColumnStatistics() throws Throwable {
        String tblName = "tbl";
        String typeName = "Person";
        String tblOwner = "testowner";
        int lastAccessed = 6796;

        try {
            createTableForTestFilter(databaseName, tblName, tblOwner, lastAccessed, true);

            // Create a ColumnStatistics Obj
            String[] colName = new String[]{"income".toUpperCase(), "name".toUpperCase()};
            double lowValue = 50000.21;
            double highValue = 1200000.4525;
            long numNulls = 3;
            long numDVs = 22;
            double avgColLen = 50.30;
            long maxColLen = 102;
            String[] colType = new String[]{"double", "string"};
            boolean isTblLevel = true;
            String partName = null;
            List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();

            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(databaseName.toUpperCase());
            statsDesc.setTableName(tblName.toUpperCase());
            statsDesc.setIsTblLevel(isTblLevel);
            statsDesc.setPartName(partName);

            ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[0]);
            statsObj.setColType(colType[0]);

            ColumnStatisticsData statsData = new ColumnStatisticsData();
            DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
            statsData.setDoubleStats(numericStats);

            statsData.getDoubleStats().setHighValue(highValue);
            statsData.getDoubleStats().setLowValue(lowValue);
            statsData.getDoubleStats().setNumDVs(numDVs);
            statsData.getDoubleStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            statsObj = new ColumnStatisticsObj();
            statsObj.setColName(colName[1]);
            statsObj.setColType(colType[1]);

            statsData = new ColumnStatisticsData();
            StringColumnStatsData stringStats = new StringColumnStatsData();
            statsData.setStringStats(stringStats);
            statsData.getStringStats().setAvgColLen(avgColLen);
            statsData.getStringStats().setMaxColLen(maxColLen);
            statsData.getStringStats().setNumDVs(numDVs);
            statsData.getStringStats().setNumNulls(numNulls);

            statsObj.setStatsData(statsData);
            statsObjs.add(statsObj);

            ColumnStatistics colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            // write stats objs persistently
            client.updateTableColumnStatistics(colStats);

            // retrieve the stats obj that was just written
            ColumnStatisticsObj colStats2 = client.getTableColumnStatistics(
                    databaseName, tblName, Lists.newArrayList(colName[0])).get(0);

            // compare stats obj to ensure what we get is what we wrote
            assertNotNull(colStats2);
            assertEquals(colStats2.getColName(), colName[0].toLowerCase());
            assertEquals(colStats2.getStatsData().getDoubleStats().getLowValue(), lowValue, 0.01);
            assertEquals(colStats2.getStatsData().getDoubleStats().getHighValue(), highValue, 0.01);
            assertEquals(colStats2.getStatsData().getDoubleStats().getNumNulls(), numNulls);
            assertEquals(colStats2.getStatsData().getDoubleStats().getNumDVs(), numDVs);

            // test delete column stats; if no col name is passed all column stats associated with the
            // table is deleted
            boolean status = client.deleteTableColumnStatistics(databaseName, tblName, null);
            assertTrue(status);
            // try to query stats for a column for which stats doesn't exist
            assertTrue(client.getTableColumnStatistics(
                    databaseName, tblName, Lists.newArrayList(colName[1])).isEmpty());

            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            // update table level column stats
            client.updateTableColumnStatistics(colStats);

            // query column stats for column whose stats were updated in the previous call
            colStats2 = client.getTableColumnStatistics(
                    databaseName, tblName, Lists.newArrayList(colName[0])).get(0);

            // partition level column statistics test
            // create a table with multiple partitions
            client.dropTable(databaseName, tblName);

            List<List<String>> values = new ArrayList<List<String>>();
            values.add(makeVals("2008-07-01 14:13:12", "14A"));
            values.add(makeVals("2008-07-01 14:13:12", "15A"));
            values.add(makeVals("2008-07-02 14:13:12", "15A"));
            values.add(makeVals("2008-07-03 14:13:12", "15A1"));

            createMultiPartitionTableSchema(databaseName, tblName, typeName, values);

            List<String> partitions = client.listPartitionNames(databaseName, tblName, (short) -1);

            partName = partitions.get(0);
            isTblLevel = false;

            // create a new columnstatistics desc to represent partition level column stats
            statsDesc = new ColumnStatisticsDesc();
            statsDesc.setDbName(databaseName.toUpperCase());
            statsDesc.setTableName(tblName.toUpperCase());
            statsDesc.setPartName(partName.replace("ds", "DS"));
            statsDesc.setIsTblLevel(isTblLevel);

            colStats = new ColumnStatistics();
            colStats.setStatsDesc(statsDesc);
            colStats.setStatsObj(statsObjs);

            client.updatePartitionColumnStatistics(colStats);

            Map<String, List<ColumnStatisticsObj>> objs = client.getPartitionColumnStatistics(databaseName.toUpperCase(), tblName.toUpperCase(),
                    Lists.newArrayList(partName.replace("ds", "DS")), Lists.newArrayList(colName[1].toUpperCase()));
            colStats2 = objs.get(partName).get(0);

            // compare stats obj to ensure what we get is what we wrote
            assertNotNull(colStats2);
            assertEquals(colStats.getStatsDesc().getPartName(), partName);
            assertEquals(colStats2.getColName(), colName[1].toLowerCase());
            assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
            assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen, 0.01);
            assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
            assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

            // test stats deletion at partition level
            client.deletePartitionColumnStatistics(databaseName.toUpperCase(), tblName.toUpperCase(), partName.replace("ds", "DS"), colName[1].toUpperCase());

            colStats2 = client.getPartitionColumnStatistics(databaseName, tblName,
                    Lists.newArrayList(partName), Lists.newArrayList(colName[0])).get(partName).get(0);

            // test get stats on a column for which stats doesn't exist
            Map<String, List<ColumnStatisticsObj>> sts = client.getPartitionColumnStatistics(databaseName, tblName,
                    Lists.newArrayList(partName, partitions.get(1)), Lists.newArrayList(colName[1]));
            assertTrue(sts.isEmpty());
            client.dropTable(databaseName, tblName);
        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testColumnStatistics() failed.");
            throw e;
        }
    }

    private Table createTableForTestFilter(String dbName, String tableName, String owner,
                                           int lastAccessTime, boolean hasSecondParam) throws Exception {

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("income", serdeConstants.DOUBLE_TYPE_NAME, ""));

        Map<String, String> params = new HashMap<String, String>();
        params.put("sd_param_1", "Use this for comments etc");

        Map<String, String> serdParams = new HashMap<String, String>();
        serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

        StorageDescriptor sd = createStorageDescriptor(tableName, cols, params, serdParams);

        Map<String, String> partitionKeys = new HashMap<String, String>();
        partitionKeys.put("ds", serdeConstants.STRING_TYPE_NAME);
        partitionKeys.put("hr", serdeConstants.INT_TYPE_NAME);

        Map<String, String> tableParams = new HashMap<String, String>();
        tableParams.put("test_param_1", "hi");
        if (hasSecondParam) {
            tableParams.put("test_param_2", "50");
        }

        Table tbl = createTable(dbName, tableName, owner, tableParams,
                partitionKeys, sd, lastAccessTime);

        //if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tableName);
        //}
        return tbl;
    }

    private static class ExprBuilder {
        private final String tblName;
        private final Stack<ExprNodeDesc> stack = new Stack<ExprNodeDesc>();

        public ExprBuilder(String tblName) {
            this.tblName = tblName;
        }

        public ExprNodeGenericFuncDesc build() throws Exception {
            if (stack.size() != 1) {
                throw new Exception("Bad test: " + stack.size());
            }
            return (ExprNodeGenericFuncDesc) stack.pop();
        }

        public ExprBuilder pred(String name, int args) throws Exception {
            return fn(name, TypeInfoFactory.booleanTypeInfo, args);
        }

        private ExprBuilder fn(String name, TypeInfo ti, int args) throws Exception {
            List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
            for (int i = 0; i < args; ++i) {
                children.add(stack.pop());
            }
            stack.push(new ExprNodeGenericFuncDesc(ti,
                    FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
            return this;
        }

        public ExprBuilder strCol(String col) {
            return colInternal(TypeInfoFactory.stringTypeInfo, col, true);
        }

        public ExprBuilder intCol(String col) {
            return colInternal(TypeInfoFactory.intTypeInfo, col, true);
        }

        private ExprBuilder colInternal(TypeInfo ti, String col, boolean part) {
            stack.push(new ExprNodeColumnDesc(ti, col, tblName, part));
            return this;
        }

        public ExprBuilder val(String val) {
            return valInternal(TypeInfoFactory.stringTypeInfo, val);
        }

        public ExprBuilder val(int val) {
            return valInternal(TypeInfoFactory.intTypeInfo, val);
        }

        public ExprBuilder val(boolean val) {
            return valInternal(TypeInfoFactory.booleanTypeInfo, val);
        }

        private ExprBuilder valInternal(TypeInfo ti, Object val) {
            stack.push(new ExprNodeConstantDesc(ti, val));
            return this;
        }
    }
}
