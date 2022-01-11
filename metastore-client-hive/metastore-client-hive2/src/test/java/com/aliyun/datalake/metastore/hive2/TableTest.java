package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.api.DataLakeAPIException;
import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

public class TableTest extends BaseTest {
    private static final String TEST_DB = TestUtil.TEST_DB;
    private static IMetaStoreClient client;
    private static Warehouse wh;
    private static Database testDb;
    private static FileSystem fs;
    private static Warehouse warehouse;

    private static HiveConf conf;

    @BeforeClass
    public static void setUp() throws TException, IOException {
        client = TestUtil.getDlfClient();
        // client = TestUtil.getHMSClient();

        conf = new HiveConf();
        wh = new Warehouse(conf);
        warehouse = wh;
        fs = FileSystem.get(new Configuration());

        testDb = TestUtil.getDatabase(TEST_DB);
        try {
            client.dropDatabase(testDb.getName(), true, true, true);
        } catch (NoSuchObjectException e) {
        }
        client.createDatabase(testDb);
    }

    @AfterClass
    public static void cleanUp() throws TException, IOException {
        fs.close();
        try {
            List<String> allTables = client.getTables(TEST_DB, "*");
            allTables.forEach(t -> {
                try {
                    client.dropTable(TEST_DB, t, true, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            client.dropDatabase(testDb.getName(), true, true, true);
        } catch (NoSuchObjectException e) {

        }
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

    @After
    public void cleanUpCase() throws TException {
        List<String> allTables = client.getTables(TEST_DB, "*");
        allTables.forEach(t -> {
            try {
                client.dropTable(TEST_DB, t, true, true);
            } catch (UnsupportedOperationException e) {
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testCreatTable() throws TException, IOException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);

        // create a table
        client.createTable(table);
        assertTrue(table.getParameters().size() == 0);

        // check the info about the created table
        Table check = client.getTable(TEST_DB, tblName);
        Assert.assertEquals(TEST_DB, check.getDbName());
        Assert.assertEquals(tblName, check.getTableName());
        Assert.assertEquals(wh.getDefaultTablePath(testDb, tblName).toString(), check.getSd().getLocation());
        Assert.assertTrue(fs.exists(wh.getDefaultTablePath(testDb, tblName)));
    }

    @Test
    public void testTableCloseLog() throws TException, IOException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        conf.set(DataLakeConfig.CATALOG_LOG_ENABLED, "false");
        ProxyLogUtils originProxyLogUtils = ProxyLogUtils.getProxyLogUtils();
        try {
            ProxyLogUtils.setProxyLogUtils(null);
            IMetaStoreClient newClient = new ProxyMetaStoreClient(conf);
            // create a table
            System.out.println("close log start ++++++++++++++++++++++");
            //关闭记录dlf的日志
            Assert.assertFalse(ProxyLogUtils.getProxyLogUtils().isRecordLog());
            //测试createtable,mkdir
            newClient.createTable(table);
            assertTrue(table.getParameters().size() == 0);

            // check the info about the created table
            Table check = newClient.getTable(TEST_DB, tblName);
            Assert.assertEquals(TEST_DB, check.getDbName());
            Assert.assertEquals(tblName, check.getTableName());
            Assert.assertEquals(wh.getDefaultTablePath(testDb, tblName).toString(), check.getSd().getLocation());
            Assert.assertTrue(fs.exists(wh.getDefaultTablePath(testDb, tblName)));
            String renameTable = check.getTableName() + "_renamed";
            check.setTableName(renameTable);
            //测试rename,rename dir
            newClient.alter_table(TEST_DB, tblName, check);
            //测试drop,delete dir
            newClient.dropTable(TEST_DB, renameTable, true, true, true);

        } finally {
            ProxyLogUtils.setProxyLogUtils(originProxyLogUtils);
            System.out.println("close log end ++++++++++++++++++++++");
        }
    }

    @Test
    public void testTableCloseActionLog() throws TException, IOException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        conf.set(DataLakeConfig.CATALOG_ACTION_LOG_ENABLED, "true");
        ProxyLogUtils originProxyLogUtils = ProxyLogUtils.getProxyLogUtils();
        try {
            ProxyLogUtils.setProxyLogUtils(null);
            IMetaStoreClient newClient = new ProxyMetaStoreClient(conf);
            // create a table
            System.out.println("close log start ++++++++++++++++++++++");
            //打开记录action parameter的日志
            Assert.assertTrue(ProxyLogUtils.getProxyLogUtils().isRecordActionLog());
            //生成一个较大的字段，测试日志截断
            StringBuffer buf = new StringBuffer();
            for(int i = 0; i < 100; i++) {
                buf.append("0123456789");
            }
            table.getParameters().put("test", buf.toString());
            //测试createtable,mkdir
            newClient.createTable(table);

            // check the info about the created table
            Table check = newClient.getTable(TEST_DB, tblName);
            Assert.assertEquals(TEST_DB, check.getDbName());
            Assert.assertEquals(tblName, check.getTableName());
            Assert.assertEquals(wh.getDefaultTablePath(testDb, tblName).toString(), check.getSd().getLocation());
            Assert.assertTrue(fs.exists(wh.getDefaultTablePath(testDb, tblName)));
            String renameTable = check.getTableName() + "_renamed";
            check.setTableName(renameTable);
            //测试rename,rename dir
            newClient.alter_table(TEST_DB, tblName, check);
            //测试drop,delete dir
            newClient.dropTable(TEST_DB, renameTable, true, true, true);

        } finally {
            ProxyLogUtils.setProxyLogUtils(originProxyLogUtils);
            System.out.println("close log end ++++++++++++++++++++++");
        }
    }

    @Test
    public void testDefaultDbCreatTable() throws TException, IOException {
        String dbName = "default";
        String tblName = "default_table_test";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(dbName, tblName, columns);

        Database defaultDb = client.getDatabase(dbName);
        ////table don't use default db current location, but use whroot
        String originDbLocation = defaultDb.getLocationUri();
        boolean isModifyDbLocation = false;
        if (defaultDb.getLocationUri().equals(wh.getWhRoot().toString()) || defaultDb.getLocationUri().startsWith("hdfs://")) {
            defaultDb.setLocationUri("file:///tmp/");
            client.alterDatabase(dbName, defaultDb);
            isModifyDbLocation = true;
        }

        // create a table
        try {
            client.dropTable(dbName, tblName, true, true, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        table.getSd().setLocation(null);
        client.createTable(table);

        // check the info about the created table
        Table check = client.getTable(dbName, tblName);
        Assert.assertTrue(check.getSd().getLocation().equals(new Path(wh.getWhRoot(), MetaStoreUtils.encodeTableName(table.getTableName().toLowerCase())).toString()));

        try {
            client.dropTable(dbName, tblName, true, true, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        table.getSd().setLocation(null);
        //table location use default db current location
        HiveConf newConf = new HiveConf();
        newConf.set(DataLakeConfig.CATALOG_DEFAULT_DB_CREATE_TABLE_USE_CURRENT_DB_LOCATION, "true");
        IMetaStoreClient newClient = new ProxyMetaStoreClient(newConf);
        newClient.createTable(table);
        // check the info about the created table
        check = newClient.getTable(dbName, tblName);
        Assert.assertTrue(check.getSd().getLocation().equals(new Path(new Path(defaultDb.getLocationUri()), MetaStoreUtils.encodeTableName(table.getTableName().toLowerCase())).toString()));

        if (isModifyDbLocation) {
            defaultDb.setLocationUri(originDbLocation);
            client.alterDatabase(dbName, defaultDb);
        }

    }

    @Test
    public void testCreatTableWithStats() throws TException, IOException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        fs.mkdirs(new Path(table.getSd().getLocation()));
        //mkdir manually first, so mkdir false, then gather stats
        client.createTable(table);
        assertEquals(2, table.getParameters().size());
        assertEquals(0, Integer.valueOf(table.getParameters().get("numFiles")).intValue());
        assertEquals(0L, Long.valueOf(table.getParameters().get("totalSize")).longValue());
        Table check = client.getTable(TEST_DB, tblName);
        assertEquals(0, Integer.valueOf(check.getParameters().get("numFiles")).intValue());
        assertEquals(0L, Long.valueOf(check.getParameters().get("totalSize")).longValue());
        client.dropTable(TEST_DB, tblName, true, true, true);
        //mkdir true, so don't gather stats
        table.setParameters(null);
        client.createTable(table);
        assertTrue(table.getParameters() == null || table.getParameters().size() == 0);
        check = client.getTable(TEST_DB, tblName);
        assertTrue(check.getParameters() == null || (check.getParameters().get("totalSize") == null && check.getParameters().get("numFiles") == null));
        client.dropTable(TEST_DB, tblName, true, true, true);
        //HIVESTATSAUTOGATHER is false, so don't gather stats
        conf.set(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, "false");
        table.setParameters(null);
        client.createTable(table);
        assertTrue(table.getParameters() == null || table.getParameters().size() == 0);
        check = client.getTable(TEST_DB, tblName);
        assertTrue(check.getParameters() == null || (check.getParameters().get("totalSize") == null && check.getParameters().get("numFiles") == null));
        client.dropTable(TEST_DB, tblName, true, true, true);
        conf.set(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, "true");
        // table is viw, so don't gather stats
        table.setTableType(TableType.VIRTUAL_VIEW.toString());
        table.setParameters(null);
        client.createTable(table);
        assertTrue(table.getParameters() == null || table.getParameters().size() == 0);
        check = client.getTable(TEST_DB, tblName);
        assertTrue(check.getParameters() == null || (check.getParameters().get("totalSize") == null && check.getParameters().get("numFiles") == null));
        client.dropTable(TEST_DB, tblName, true, true, true);
    }

    @Test
    public void testCreateTableRepeatedly() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);

        // create a table
        client.createTable(table);

        // repeatedly create table
        Assert.assertThrows("already exists", AlreadyExistsException.class, () -> client.createTable(table));
    }

    @Test
    public void testCreatePartitionTable() throws TException {
        // create a partition table
        String partTblName = TestUtil.TEST_TABLE + "part";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partCols = ImmutableMap.of("dt", "date");
        Table partTable = TestUtil.getPartitionTable(TEST_DB, partTblName, columns, partCols);
        client.createTable(partTable);

        // check the partition table
        Table checkPartTable = client.getTable(TEST_DB, partTblName);
        Map<String, String> returned = checkPartTable.getPartitionKeys().stream()
                .collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getType));
        Assert.assertEquals(partCols, returned);
    }

    @Test
    public void testDropNonExistTable() throws TException, IOException {
        // drop a non exist table
        String nonExist = "non_exist";
        client.dropTable(TEST_DB, nonExist, true, true);
        Assert.assertThrows("table not found", NoSuchObjectException.class,
                () -> client.dropTable(TEST_DB, nonExist, true, false));

        // drop a non exist table but not throw exception
        client.dropTable(TEST_DB, nonExist, true, true);
    }

    @Test
    public void testDropTableWhenMetaException() throws Throwable {
        String partTblName = "testDropTableWhenMetaException";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partCols = ImmutableMap.of("dt", "date");
        Table partTable = TestUtil.getPartitionTable(TEST_DB, partTblName, columns, partCols);
        client.createTable(partTable);

        DlfMetaStoreClientDelegate clientDelegate = ((DlfMetaStoreClient)((ProxyMetaStoreClient)client).getDlfSessionMetaStoreClient()).getClientDelegate();
        IDataLakeMetaStore dataLakeMetaStore = clientDelegate.getDataLakeMetaStore();
        IDataLakeMetaStore spy = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(DataLakeAPIException.class).when(spy).dropTable(any(String.class), any(String.class),
                any(String.class), any(Boolean.class));

        clientDelegate.setDataLakeMetaStore(spy);

        Assert.assertThrows(MetaException.class,
                () -> client.dropTable(TEST_DB, partTblName, true, true));

        clientDelegate.setDataLakeMetaStore(dataLakeMetaStore);
    }

    @Test
    public void testDeleteDirWhenDropTable() throws TException, IOException {
        // create a table
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        client.createTable(table);
        Path tblPath = wh.getDefaultTablePath(testDb, tblName);

        // drop the created table and delete the data
        client.dropTable(TEST_DB, tblName, true, false);

        // asert the data has been deleted
        Assert.assertFalse(fs.exists(tblPath));

        // assert the table has been dropped successfully
        Assert.assertThrows("table not found", NoSuchObjectException.class,
                () -> client.getTable(TEST_DB, tblName));
    }

    @Test
    public void testDropPartitionTable() throws TException {
        // create a partition table
        String partTblName = TestUtil.TEST_TABLE + "part";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partCols = ImmutableMap.of("dt", "date");
        Table partTable = TestUtil.getPartitionTable(TEST_DB, partTblName, columns, partCols);
        client.createTable(partTable);

        // drop a partition table
        client.dropTable(TEST_DB, partTblName, true, false);

        // assert the partition table has been dropped successfully
        Assert.assertThrows("Table not found", NoSuchObjectException.class,
                () -> client.getTable(TEST_DB, partTblName));
    }

    @Test
    public void testTableExists() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        String unknownDb = "unknownDb";
        String unknownTbl = "unknownTbl";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);

        // create a table
        client.createTable(table);

        // check a table from unknown database
        Assert.assertFalse(client.tableExists(unknownDb, tblName));

        // check table exist or not
        Assert.assertFalse(client.tableExists(unknownTbl));
        Assert.assertTrue(client.tableExists(TEST_DB, tblName));
    }

    // Not supported now
    //@Test
    //public void testGetTableMeta() throws TException {
    //    String tbl1 = "test_tbl1";
    //    String tbl2 = "test_tbl2";
    //    String tbl3 = "tbl3";
    //    String tbl4 = "test_tbl4";
    //    Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
    //    Table table1 = TestUtil.getTable(DB_NAME, tbl1, columns);
    //    Table table2 = TestUtil.getTable(DB_NAME, tbl2, columns);
    //    Table table3 = TestUtil.getTable(DB_NAME, tbl3, columns);
    //    Table table4 = TestUtil.getTable(DB_NAME, tbl4, TableType.EXTERNAL_TABLE, columns);
    //
    //    // create the tables
    //    client.createTable(table1);
    //    client.createTable(table2);
    //    client.createTable(table3);
    //    client.createTable(table4);
    //
    //    // get all tables those match the table pattern and table types
    //    String pattern = "test_tbl*";
    //    List<String> tableTypes =
    //        ImmutableList.of(TableType.MANAGED_TABLE.name(), TableType.EXTERNAL_TABLE.name());
    //    List<TableMeta> tableMetas = client.getTableMeta(DB_NAME, pattern, tableTypes);
    //    tableMetas.sort(Comparator.comparing(TableMeta::getTableName));
    //    List<TableMeta> expected = ImmutableList.of(
    //        new TableMeta(DB_NAME, tbl1, TableType.MANAGED_TABLE.name()),
    //        new TableMeta(DB_NAME, tbl2, TableType.MANAGED_TABLE.name()),
    //        new TableMeta(DB_NAME, tbl4, TableType.EXTERNAL_TABLE.name()));
    //    Assert.assertEquals(expected, tableMetas);
    //
    //    // get all managed tables those match the table pattern
    //    tableMetas = client.getTableMeta(DB_NAME, pattern, ImmutableList.of(TableType.MANAGED_TABLE.name()));
    //    tableMetas.sort(Comparator.comparing(TableMeta::getTableName));
    //    expected = ImmutableList.of(
    //        new TableMeta(DB_NAME, tbl1, TableType.MANAGED_TABLE.name()),
    //        new TableMeta(DB_NAME, tbl2, TableType.MANAGED_TABLE.name()));
    //    Assert.assertEquals(expected, tableMetas);
    //}

    @Test
    public void testGetNonExistTable() throws TException {
        // get a non exist table
        Assert.assertThrows("table not found", NoSuchObjectException.class,
                () -> client.getTable("non_exist_db", "non_exist"));
        Assert.assertThrows("table not found", NoSuchObjectException.class,
                () -> client.getTable(TEST_DB, "non_exist"));
    }

    @Test
    public void testGetInvalidTable() throws TException {
        Assert.assertThrows("table not found", NoSuchObjectException.class,
                () -> client.getTable(TEST_DB, "oss://not_valid"));
    }

    @Test
    public void testGetPrivilegedTable() throws TException, IOException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);

        Map<String, List<PrivilegeGrantInfo>> userPrivileges =
                ImmutableMap.of("Mt.EMR",
                        ImmutableList.of(new PrivilegeGrantInfo("INSERT", -1, "Mt.EMR", PrincipalType.USER, true)));
        PrincipalPrivilegeSet privilegeSet = new PrincipalPrivilegeSet();
        privilegeSet.setUserPrivileges(userPrivileges);

        table.setPrivileges(privilegeSet);

        // create a table
        client.createTable(table);

        // check the info about the created table
        Table check = client.getTable(TEST_DB, tblName);
        Assert.assertEquals(TEST_DB, check.getDbName());
        Assert.assertEquals(tblName, check.getTableName());
        Assert.assertEquals(wh.getDefaultTablePath(testDb, tblName).toString(), check.getSd().getLocation());
        Assert.assertTrue(fs.exists(wh.getDefaultTablePath(testDb, tblName)));
    }

    @Test
    public void testGetTables() throws TException {
        String tbl1 = TestUtil.TEST_TABLE + "1";
        String tbl2 = TestUtil.TEST_TABLE + "2";
        String tbl3 = "tbl3";
        String tbl4 = TestUtil.TEST_TABLE + "4";
        String tbl5 = TestUtil.TEST_TABLE + "5";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table1 = TestUtil.getTable(TEST_DB, tbl1, columns);
        Table table2 = TestUtil.getTable(TEST_DB, tbl2, TableType.MANAGED_TABLE, columns);
        Table table3 = TestUtil.getTable(TEST_DB, tbl3, TableType.MATERIALIZED_VIEW, columns);
        Table table4 = TestUtil.getTable(TEST_DB, tbl4, TableType.EXTERNAL_TABLE, columns);
        Table table5 = TestUtil.getTable(TEST_DB, tbl5, TableType.VIRTUAL_VIEW, columns);
        table5.getSd().setLocation(null);
        // create the tables
        client.createTable(table1);
        client.createTable(table2);
        client.createTable(table3);
        client.createTable(table4);
        client.createTable(table5);
        Assert.assertTrue(client.getTable(TEST_DB, tbl1).getTableType().equals("MANAGED_TABLE"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl2).getTableType().equals("MANAGED_TABLE"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl3).getTableType().equals("MATERIALIZED_VIEW"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl4).getTableType().equals("EXTERNAL_TABLE"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl5).getTableType().equals("VIRTUAL_VIEW"));

        // get exact pattern for table
        List<String> exactTable1 = client.getTables(TEST_DB, TestUtil.TEST_TABLE.toUpperCase() + ".",
                TableType.MANAGED_TABLE);
        Assert.assertEquals(exactTable1.size(), 2);

        List<String> exactTable2 = client.getTables(TEST_DB, TestUtil.TEST_TABLE + ".");
        Assert.assertEquals(exactTable2.size(), 4);

        // test sub pattern
        List<String> subPatternTable = client.getTables(TEST_DB, TestUtil.TEST_TABLE.toUpperCase() + ".|" + tbl3 ,
                TableType.MANAGED_TABLE);
        Assert.assertEquals(subPatternTable.size(), 2);

        subPatternTable = client.getTables(TEST_DB, TestUtil.TEST_TABLE.toUpperCase() + ".|" + tbl3);
        Assert.assertEquals(subPatternTable.size(), 5);

        // get all tables those match the table pattern
        String pattern = TestUtil.TEST_TABLE + "*";
        List<String> expected = ImmutableList.of(tbl1, tbl2, tbl4, tbl5);
        List<String> tables = client.getTables(TEST_DB, pattern);
        tables.sort(String::compareTo);
        Assert.assertEquals(expected, tables);

        // get all tables those match the table pattern and table type
        List<String> tableNamesManaged = client.getTables(TEST_DB, pattern, TableType.MANAGED_TABLE);
        Assert.assertTrue("tableNamesManaged.size()" + tableNamesManaged.size(), tableNamesManaged.size() == 2);

        // all pattern
        List<String> tableNamesMaterial = client.getTables(TEST_DB, "*", TableType.MATERIALIZED_VIEW);
        List<String> tableNamesMaterial1 = client.getTables(TEST_DB, ".*", TableType.MATERIALIZED_VIEW);
        List<String> tableNamesMaterial2 = client.getTables(TEST_DB, "*.", TableType.MATERIALIZED_VIEW);
        Assert.assertTrue(tableNamesMaterial.size() == 1 && 1 == tableNamesMaterial1.size() && tableNamesMaterial2.size() == 1);

        //get other pattern
        List<String> otherPattern = client.getTables(TEST_DB, "_0sr*9sdf0_erDn.");
        Assert.assertTrue(otherPattern.size() == 0);

        List<String> tableNamesVirtual = client.getTables(TEST_DB, pattern, TableType.VIRTUAL_VIEW);
        Assert.assertTrue(tableNamesVirtual.size() == 1);
        List<String> tableNamesExternal = client.getTables(TEST_DB, pattern, TableType.EXTERNAL_TABLE);
        Assert.assertTrue(tableNamesExternal.size() == 1);

        // get tables from unkown database
        List<String> empty = client.getTables("unknownDb", pattern);
        Assert.assertEquals(0, empty.size());
    }

    @Test
    public void testCreateAndDropView() throws TException {
        String viewName = TestUtil.TEST_TABLE + "view";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table view = TestUtil.getView(TEST_DB, viewName, columns);
        client.createTable(view);

        Table check = client.getTable(TEST_DB, viewName);
        Assert.assertEquals(check.getTableName(), viewName);

        client.dropTable(TEST_DB, viewName);

        Assert.assertThrows("Table not found", NoSuchObjectException.class,
                () -> client.getTable(TEST_DB, viewName));
    }

    @Test
    public void testGetAllTables() throws TException {
        String tbl1 = TestUtil.TEST_TABLE + "1";
        String tbl2 = TestUtil.TEST_TABLE + "2";
        String tbl3 = TestUtil.TEST_TABLE + "3";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table1 = TestUtil.getTable(TEST_DB, tbl1, columns);
        Table table2 = TestUtil.getTable(TEST_DB, tbl2, columns);
        Table table3 = TestUtil.getTable(TEST_DB, tbl3, columns);

        // create the tables
        client.createTable(table1);
        client.createTable(table2);
        client.createTable(table3);

        List<String> tables = client.getAllTables(TEST_DB);
        List<String> expected = ImmutableList.of(tbl1, tbl2, tbl3);
        tables.sort(String::compareTo);
        Assert.assertEquals(expected, tables);
    }

    @Test
    public void testGetTableObjectsByName() throws TException {
        String tbl1 = TestUtil.TEST_TABLE + "1";
        String tbl2 = TestUtil.TEST_TABLE + "2";
        String tbl3 = TestUtil.TEST_TABLE + "3";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table1 = TestUtil.getTable(TEST_DB, tbl1, columns);
        Table table2 = TestUtil.getTable(TEST_DB, tbl2, columns);
        Table table3 = TestUtil.getTable(TEST_DB, tbl3, columns);

        // create the tables
        client.createTable(table1);
        client.createTable(table2);
        client.createTable(table3);

        // get tables
        List<String> tablesToGet = ImmutableList.of(tbl1, tbl3);
        List<Table> tables = client.getTableObjectsByName(TEST_DB, tablesToGet);
        tables.sort(Comparator.comparing(Table::getTableName));
        Assert.assertEquals(2, tables.size());
        Assert.assertEquals(tbl1, tables.get(0).getTableName());
        Assert.assertEquals(tbl3, tables.get(1).getTableName());
    }

    @Test
    public void testGetTableObjectsWithNonExistInIt() throws TException {
        String tbl = TestUtil.TEST_TABLE;
        String nonExist = "non_exist";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table1 = TestUtil.getTable(TEST_DB, tbl, columns);

        // create the tables
        client.createTable(table1);

        // non exist table will be ignored
        List<String> tablesToGet = ImmutableList.of(tbl, nonExist);
        List<Table> tables = client.getTableObjectsByName(TEST_DB, tablesToGet);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(tbl, tables.get(0).getTableName());
    }

    @Test
    public void testListTableNamesByFilter() throws TException {
        // interface not implemented
        Assert.assertTrue(true);
    }

    @Test
    public void testAlterNonExistTable() throws TException {
        String nonExist = "non_exist";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, nonExist, columns);
        // alter a non exist table
        Assert.assertThrows("doesn't exist", InvalidOperationException.class,
                () -> client.alter_table(TEST_DB, nonExist, table));
    }

    @Test
    public void testAlterTableRenameTable() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        String newTblName = "new" + TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);

        // create the table
        client.createTable(table);

        // alter table: rename
        Table renamed = TestUtil.getTable(TEST_DB, newTblName, columns);
        renamed.getSd().setLocation(table.getSd().getLocation());
        client.alter_table(TEST_DB, tblName, renamed);

        // check renamed table
        Table checkRenamed = client.getTable(TEST_DB, newTblName);
        Assert.assertEquals(newTblName, checkRenamed.getTableName());

        //rename operation will not gather stats
        Assert.assertTrue("it's impossible that rename operation gathers stats", checkRenamed.getParameters() == null || checkRenamed.getParameters().get("numFiles") == null);
        Assert.assertTrue("it's impossible that rename operation gathers stats", checkRenamed.getParameters() == null || checkRenamed.getParameters().get("totalSize") == null);
    }

    @Test
    public void testAlterTableAddColumns() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);

        // create the table
        client.createTable(table);

        // alter table: add a new column
        Map<String, String> newColumns = ImmutableMap.of("id", "int", "name", "string", "phone", "string");
        Table colAdded = TestUtil.getTable(TEST_DB, tblName, newColumns);
        client.alter_table(TEST_DB, tblName, colAdded);

        // check the new columns
        Table checkColAdded = client.getTable(TEST_DB, tblName);
        List<FieldSchema> newCols = checkColAdded.getSd().getCols();
        List<FieldSchema> expected = ImmutableMap.of("id", "int", "name", "string", "phone", "string")
                .entrySet().stream()
                .map(e -> new FieldSchema(e.getKey(), e.getValue(), "test"))
                .collect(Collectors.toList());
        Assert.assertEquals(expected, newCols);

        //alter operation will gather stats
        Assert.assertTrue("it's impossible that alter talbe operation not gather stats", checkColAdded.getParameters().get("numFiles") != null);
        Assert.assertTrue("it's impossible that atler table operation not gather stats", checkColAdded.getParameters().get("totalSize") != null);
    }

    @Test
    public void testAlterTableModifyPartitionColumns() throws TException {
        String tblName = TestUtil.TEST_TABLE + "_testAlterTableModifyPartitionColumns".toLowerCase();
        Map<String, String> columns = ImmutableMap.of("id_d", "int", "name_d", "string");
        Map<String, String> partColumns = ImmutableMap.of("id", "int", "name", "string");

        Table ctable = TestUtil.getPartitionTable(TEST_DB, tblName, columns, partColumns);
        // create partition table
        client.createTable(ctable);
        final Table table = client.getTable(TEST_DB, tblName);

        // allow incompatible partition cols change
        partColumns = ImmutableMap.of("id", "bigint", "name", "string");
        table.setPartitionKeys(TestUtil.getFieldSchemas(partColumns, "xxx"));
        client.alter_table(TEST_DB, tblName, table);
        Table newTable = client.getTable(TEST_DB, tblName);
        ValidatorUtils.validateTables(table, newTable);

        partColumns = ImmutableMap.of("id", "string", "name", "string");
        table.setPartitionKeys(TestUtil.getFieldSchemas(partColumns, "xxx1"));
        client.alter_table(TEST_DB, tblName, table);
        newTable = client.getTable(TEST_DB, tblName);
        ValidatorUtils.validateTables(table, newTable);

        partColumns = ImmutableMap.of("id", "date", "name", "string");
        table.setPartitionKeys(TestUtil.getFieldSchemas(partColumns, "xxx2"));
        client.alter_table(TEST_DB, tblName, table);
        newTable = client.getTable(TEST_DB, tblName);
        ValidatorUtils.validateTables(table, newTable);

        // alter partition key names
        partColumns = ImmutableMap.of("idxxxx", "date", "name", "string");
        table.setPartitionKeys(TestUtil.getFieldSchemas(partColumns, "xxx2"));
        Assert.assertThrows("partition keys can not be changed.", InvalidOperationException.class, () -> client.alter_table(TEST_DB, tblName, table));

        // alter partition key length
        partColumns = ImmutableMap.of("id", "date", "name", "string", "tt", "string");
        table.setPartitionKeys(TestUtil.getFieldSchemas(partColumns, "xxx2"));
        Assert.assertThrows("partition keys can not be changed.", InvalidOperationException.class, () -> client.alter_table(TEST_DB, tblName, table));

        //alter partition table operation will not gather stats
        Assert.assertTrue("it's impossible that alter partition table operation gathers table stats", newTable.getParameters() == null || newTable.getParameters().get("numFiles") == null);
        Assert.assertTrue("it's impossible that alter partition table operation gathers table stats", newTable.getParameters() == null || newTable.getParameters().get("totalSize") == null);
    }

    @Test
    public void testAlterTableModifyTableParams() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        table.getParameters().put("key1", "value1");
        table.getParameters().put("key2", "value2");

        // create the table
        client.createTable(table);

        // alter table: set properties
        // table parameters should be mutable map.
        Map<String, String> newParams = new HashMap<>();
        newParams.put("key1", "value3");
        Table newParamsTable = TestUtil.getTable(TEST_DB, tblName, columns, newParams);
        client.alter_table(TEST_DB, tblName, newParamsTable);

        // check the parameters
        Table checkTableWithNewParams = client.getTable(TEST_DB, tblName);
        Assert.assertTrue(checkTableWithNewParams.getParameters().get("key1").equals("value3"));
        Assert.assertTrue(!checkTableWithNewParams.getParameters().containsKey("key2"));
    }

    @Test
    public void testGetTableMeta() throws TException {
        String tbl1 = TestUtil.TEST_TABLE + "1";
        String tbl2 = TestUtil.TEST_TABLE + "2";
        String tbl3 = "tbl3";
        String tbl4 = TestUtil.TEST_TABLE + "4";
        String tbl5 = TestUtil.TEST_TABLE + "5";
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table1 = TestUtil.getTable(TEST_DB, tbl1, columns);
        Table table2 = TestUtil.getTable(TEST_DB, tbl2, TableType.MANAGED_TABLE, columns);
        Table table3 = TestUtil.getTable(TEST_DB, tbl3, TableType.MATERIALIZED_VIEW, columns);
        Table table4 = TestUtil.getTable(TEST_DB, tbl4, TableType.EXTERNAL_TABLE, columns);
        Table table5 = TestUtil.getTable(TEST_DB, tbl5, TableType.VIRTUAL_VIEW, columns);
        table5.getSd().setLocation(null);
        // create the tables
        client.createTable(table1);
        client.createTable(table2);
        client.createTable(table3);
        client.createTable(table4);
        client.createTable(table5);
        Assert.assertTrue(client.getTable(TEST_DB, tbl1).getTableType().equals("MANAGED_TABLE"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl2).getTableType().equals("MANAGED_TABLE"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl3).getTableType().equals("MATERIALIZED_VIEW"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl4).getTableType().equals("EXTERNAL_TABLE"));
        Assert.assertTrue(client.getTable(TEST_DB, tbl5).getTableType().equals("VIRTUAL_VIEW"));
        // get all tables those match the table pattern
        String pattern = TestUtil.TEST_TABLE + "*";
        List<String> expected = ImmutableList.of(tbl1, tbl2, tbl4, tbl5);
        List<String> tables = client.getTables(TEST_DB, pattern);
        tables.sort(String::compareTo);
        Assert.assertEquals(expected, tables);

        List<TableMeta> metas1 = client.getTableMeta(TEST_DB, TestUtil.TEST_TABLE + "*", null);
        Assert.assertTrue("metas.size" + metas1.size(), metas1.size() == 4);

        List<TableMeta> metas2 = client.getTableMeta(TEST_DB, TestUtil.TEST_TABLE + "*|tbl*", null);
        Assert.assertTrue("metas.size" + metas2.size(), metas2.size() == 5);

        List<String> tableTypes = new ArrayList<>();
        tableTypes.add("MANAGED_TABLE");
        List<TableMeta> metas3 = client.getTableMeta(TEST_DB, TestUtil.TEST_TABLE + "*|tbl*", tableTypes);
        Assert.assertTrue("metas.size" + metas3.size(), metas3.size() == 2);

        tableTypes = new ArrayList<>();
        tableTypes.add("MANAGED_TABLE");
        tableTypes.add("EXTERNAL_TABLE");
        List<TableMeta> metas4 = client.getTableMeta(TEST_DB, TestUtil.TEST_TABLE + "*|tbl*", tableTypes);
        Assert.assertTrue("metas.size" + metas4.size(), metas4.size() == 3);

        List<TableMeta> metas5 = client.getTableMeta("*", "no_founded_metastore_ut*", null);
        Assert.assertTrue("metas.size" + metas5.size(), metas5.size() == 0);
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
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        // create the table
        client.createTable(table);
        // update statistics
        ColumnStatistics columnStatistics = new ColumnStatistics();
        ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc();
        columnStatisticsDesc.setIsTblLevel(true);
        columnStatisticsDesc.setTableName(tblName.toUpperCase());
        columnStatisticsDesc.setDbName(TEST_DB.toUpperCase());
        columnStatisticsDesc.setLastAnalyzed(System.currentTimeMillis() / 1000);
        List<ColumnStatisticsObj> columnStatisticsObjs = new ArrayList<>();
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
        columnStatisticsObjs.add(longColumnStatisticsObj);
        ColumnStatisticsObj stringColumnStatisticsObj = new ColumnStatisticsObj();
        stringColumnStatisticsObj.setColName("NAME");
        stringColumnStatisticsObj.setColType("STRING");
        columnStatisticsData = new ColumnStatisticsData();
        StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
        stringColumnStatsData.setBitVectors(null);
        stringColumnStatsData.setMaxColLen(100);
        stringColumnStatsData.setAvgColLen(50.33);
        stringColumnStatsData.setNumDVs(21);
        stringColumnStatsData.setNumNulls(21);
        columnStatisticsData.setStringStats(stringColumnStatsData);
        stringColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(stringColumnStatisticsObj);
        ColumnStatisticsObj dateColumnStatisticsObj = new ColumnStatisticsObj();
        dateColumnStatisticsObj.setColName("BIRTH");
        dateColumnStatisticsObj.setColType("DATE");
        columnStatisticsData = new ColumnStatisticsData();
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData();
        dateColumnStatsData.setBitVectors(null);
        dateColumnStatsData.setLowValue(new Date(18590));
        dateColumnStatsData.setHighValue(new Date(18585));
        dateColumnStatsData.setNumDVs(22);
        dateColumnStatsData.setNumNulls(22);
        columnStatisticsData.setDateStats(dateColumnStatsData);
        dateColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(dateColumnStatisticsObj);
        ColumnStatisticsObj doubleColumnStatisticsObj = new ColumnStatisticsObj();
        doubleColumnStatisticsObj.setColName("HEIGHT");
        doubleColumnStatisticsObj.setColType("DOUBLE");
        columnStatisticsData = new ColumnStatisticsData();
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setBitVectors(null);
        doubleColumnStatsData.setLowValue(170.15);
        doubleColumnStatsData.setHighValue(190.23);
        doubleColumnStatsData.setNumDVs(23);
        doubleColumnStatsData.setNumNulls(23);
        columnStatisticsData.setDoubleStats(doubleColumnStatsData);
        doubleColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(doubleColumnStatisticsObj);
        ColumnStatisticsObj decimalColumnStatisticsObj = new ColumnStatisticsObj();
        decimalColumnStatisticsObj.setColName("WEIGHT");
        decimalColumnStatisticsObj.setColType("DECIMAL(10,3)");
        columnStatisticsData = new ColumnStatisticsData();
        DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData();
        decimalColumnStatsData.setBitVectors(null);
        decimalColumnStatsData.setLowValue(new Decimal(ByteBuffer.wrap(new BigDecimal("128.888").unscaledValue().toByteArray()), (short) 3));
        decimalColumnStatsData.setHighValue(new Decimal(ByteBuffer.wrap(new BigDecimal("178.888").unscaledValue().toByteArray()), (short) 3));
        decimalColumnStatsData.setNumDVs(24);
        decimalColumnStatsData.setNumNulls(24);
        columnStatisticsData.setDecimalStats(decimalColumnStatsData);
        decimalColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(decimalColumnStatisticsObj);
        ColumnStatisticsObj booleanColumnStatisticsObj = new ColumnStatisticsObj();
        booleanColumnStatisticsObj.setColName("IS_MALE");
        booleanColumnStatisticsObj.setColType("boolean");
        columnStatisticsData = new ColumnStatisticsData();
        BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setBitVectors(null);
        booleanColumnStatsData.setNumFalses(100);
        booleanColumnStatsData.setNumTrues(50);
        booleanColumnStatsData.setNumNulls(20);
        columnStatisticsData.setBooleanStats(booleanColumnStatsData);
        booleanColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(booleanColumnStatisticsObj);
        ColumnStatisticsObj binaryColumnStatisticsObj = new ColumnStatisticsObj();
        binaryColumnStatisticsObj.setColName("SHOPPING");
        binaryColumnStatisticsObj.setColType("BINARY");
        columnStatisticsData = new ColumnStatisticsData();
        BinaryColumnStatsData binaryColumnStatsData = new BinaryColumnStatsData();
        binaryColumnStatsData.setBitVectors(null);
        binaryColumnStatsData.setMaxColLen(100);
        binaryColumnStatsData.setAvgColLen(50.33);
        binaryColumnStatsData.setNumNulls(26);
        columnStatisticsData.setBinaryStats(binaryColumnStatsData);
        binaryColumnStatisticsObj.setStatsData(columnStatisticsData);
        columnStatisticsObjs.add(binaryColumnStatisticsObj);
        columnStatistics.setStatsDesc(columnStatisticsDesc);
        columnStatistics.setStatsObj(columnStatisticsObjs);
        client.updateTableColumnStatistics(columnStatistics);
        // get statistics
        List<String> colNames = new ArrayList<>();
        colNames.add("ID");
        colNames.add("NAME");
        colNames.add("BIRTH");
        colNames.add("HEIGHT");
        colNames.add("WEIGHT");
        colNames.add("IS_MALE");
        colNames.add("SHOPPING");

        List<ColumnStatisticsObj> statisticsObjs = client.getTableColumnStatistics(TEST_DB, tblName, colNames);
        for (ColumnStatisticsObj obj : statisticsObjs) {
            if ("id".equals(obj.getColName())) {
                Assert.assertTrue("longColName", obj.getColName().equals(longColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("longColType:", obj.getColType().equals(longColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("longDataType:", obj.getStatsData().isSetLongStats());
                LongColumnStatsData getLongColumnStatsData = obj.getStatsData().getLongStats();
                Assert.assertTrue("longBitVectors", getLongColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("longHighValue", getLongColumnStatsData.getHighValue() == longColumnStatsData.getHighValue());
                Assert.assertTrue("longLowValue", getLongColumnStatsData.getLowValue() == longColumnStatsData.getLowValue());
                Assert.assertTrue("longNumDVs", getLongColumnStatsData.getNumDVs() == longColumnStatsData.getNumDVs());
                Assert.assertTrue("longNumNulls", getLongColumnStatsData.getNumNulls() == longColumnStatsData.getNumNulls());
            } else if ("name".equals(obj.getColName())) {
                Assert.assertTrue("stringColName", obj.getColName().equals(stringColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("stringColType:", obj.getColType().equals(stringColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("stringDataType:", obj.getStatsData().isSetStringStats());
                StringColumnStatsData getStringColumnStatsData = obj.getStatsData().getStringStats();
                Assert.assertTrue("stringBitVector", getStringColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("stringMaxColLen", getStringColumnStatsData.getMaxColLen() == stringColumnStatsData.getMaxColLen());
                Assert.assertTrue("stringAvgColLen", Math.abs(getStringColumnStatsData.getAvgColLen() - stringColumnStatsData.getAvgColLen()) <= 0.01);
                Assert.assertTrue("stringNumDVs", getStringColumnStatsData.getNumDVs() == stringColumnStatsData.getNumDVs());
                Assert.assertTrue("stringNumNulls", getStringColumnStatsData.getNumNulls() == stringColumnStatsData.getNumNulls());
            } else if ("birth".equals(obj.getColName())) {
                Assert.assertTrue("dateColName", obj.getColName().equals(dateColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("dateColType:", obj.getColType().equals(dateColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("dateDataType:", obj.getStatsData().isSetDateStats());
                DateColumnStatsData getDateColumnStatsData = obj.getStatsData().getDateStats();
                Assert.assertTrue(getDateColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("dateLowValue", getDateColumnStatsData.getLowValue().equals(dateColumnStatsData.getLowValue()));
                Assert.assertTrue("dateHighValue", getDateColumnStatsData.getHighValue().equals(dateColumnStatsData.getHighValue()));
                Assert.assertTrue("dateNumDVs", getDateColumnStatsData.getNumDVs() == dateColumnStatsData.getNumDVs());
                Assert.assertTrue("dateNumNulls", getDateColumnStatsData.getNumNulls() == dateColumnStatsData.getNumNulls());
            } else if ("height".equals(obj.getColName())) {
                Assert.assertTrue("doubleColName", obj.getColName().equals(doubleColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("doubleColType:", obj.getColType().equals(doubleColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("doubleDataType:", obj.getStatsData().isSetDoubleStats());
                DoubleColumnStatsData getDoubleColumnStatsData = obj.getStatsData().getDoubleStats();
                Assert.assertTrue("doubleBitVectors", getDoubleColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("doubleLowValue", Math.abs(getDoubleColumnStatsData.getLowValue() - doubleColumnStatsData.getLowValue()) <= 0.01);
                Assert.assertTrue("doubleHighValue", Math.abs(getDoubleColumnStatsData.getHighValue() - doubleColumnStatsData.getHighValue()) <= 0.01);
                Assert.assertTrue("doubleNumDVs", getDoubleColumnStatsData.getNumDVs() == doubleColumnStatsData.getNumDVs());
                Assert.assertTrue("doubleNumNulls", getDoubleColumnStatsData.getNumNulls() == doubleColumnStatsData.getNumNulls());
            } else if ("weight".equals(obj.getColName())) {
                Assert.assertTrue("decimalColName", obj.getColName().equals(decimalColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("decimalColType:", obj.getColType().equals(decimalColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("decimalDataType:", obj.getStatsData().isSetDecimalStats());
                DecimalColumnStatsData getDecimalColumnStatsData = obj.getStatsData().getDecimalStats();
                Assert.assertTrue("decimalBitVectors", getDecimalColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("decimalLowValue", getDecimalColumnStatsData.getLowValue().equals(decimalColumnStatsData.getLowValue()));
                Assert.assertTrue("decimalHighValue", getDecimalColumnStatsData.getHighValue().equals(decimalColumnStatsData.getHighValue()));
                Assert.assertTrue("decimalNumDvs", getDecimalColumnStatsData.getNumDVs() == decimalColumnStatsData.getNumDVs());
                Assert.assertTrue("decimalNumNulls", getDecimalColumnStatsData.getNumNulls() == decimalColumnStatsData.getNumNulls());
            } else if ("is_male".equals(obj.getColName())) {
                Assert.assertTrue("booleanColName", obj.getColName().equals(booleanColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("booleanColType:", obj.getColType().equals(booleanColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("booleanDataType:", obj.getStatsData().isSetBooleanStats());
                BooleanColumnStatsData getBooleanColumnStatsData = obj.getStatsData().getBooleanStats();
                Assert.assertTrue("booleanBitVectors", getBooleanColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("booleanNumFalses", getBooleanColumnStatsData.getNumFalses() == booleanColumnStatsData.getNumFalses());
                Assert.assertTrue("booleanNumTrues", getBooleanColumnStatsData.getNumTrues() == booleanColumnStatsData.getNumTrues());
                Assert.assertTrue("booleanNumNulls", getBooleanColumnStatsData.getNumNulls() == booleanColumnStatsData.getNumNulls());
            } else if ("shopping".equals(obj.getColName())) {
                Assert.assertTrue("binaryColName", obj.getColName().equals(binaryColumnStatisticsObj.getColName().toLowerCase()));
                Assert.assertTrue("binaryColType:", obj.getColType().equals(binaryColumnStatisticsObj.getColType().toLowerCase()));
                Assert.assertTrue("binaryDataType:", obj.getStatsData().isSetBinaryStats());
                BinaryColumnStatsData getBinaryColumnStatsData = obj.getStatsData().getBinaryStats();
                Assert.assertTrue("binaryBitVectors", getBinaryColumnStatsData.getBitVectors() == null);
                Assert.assertTrue("binaryMaxColLen", getBinaryColumnStatsData.getMaxColLen() == binaryColumnStatsData.getMaxColLen());
                Assert.assertTrue("binaryAvgColLen", Math.abs(getBinaryColumnStatsData.getAvgColLen() - binaryColumnStatsData.getAvgColLen()) <= 0.01);
                Assert.assertTrue("binaryNumNulls", getBinaryColumnStatsData.getNumNulls() == binaryColumnStatsData.getNumNulls());
            } else {
                Assert.assertTrue(1 == 0);
            }
        }
        // delete statistics
        for (String colName : colNames) {
            boolean ret = client.deleteTableColumnStatistics(TEST_DB, tblName, colName);
            Assert.assertTrue("it's ok to delete", ret);
        }
        // get statistics
        List<ColumnStatisticsObj> statisticsObjsAfterDelete = client.getTableColumnStatistics(TEST_DB, tblName, colNames);
        Assert.assertTrue("after delete to get stats size:", statisticsObjsAfterDelete.size() == 0);
    }

    @Test
    public void testGetTableColumnStaticsOverhead() throws TException {
        String tblName = TestUtil.TEST_TABLE;
        String baseColumnName = "abcedefghijklmnopqrstuvwxyz";
        Map<String, String> columns = new HashMap<>();
        //test too many columns will cause "uri too long"
        for (int i = 0; i < 1000; i++) {
            columns.put(baseColumnName + i, "int");
        }
        Table table = TestUtil.getTable(TEST_DB, tblName, columns);
        // create the table
        client.createTable(table);
        ColumnStatistics columnStatistics = new ColumnStatistics();
        ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc();
        columnStatisticsDesc.setIsTblLevel(true);
        columnStatisticsDesc.setTableName(tblName.toUpperCase());
        columnStatisticsDesc.setDbName(TEST_DB.toUpperCase());
        columnStatisticsDesc.setLastAnalyzed(System.currentTimeMillis() / 1000);
        List<ColumnStatisticsObj> columnStatisticsObjs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ColumnStatisticsObj longColumnStatisticsObj = new ColumnStatisticsObj();
            longColumnStatisticsObj.setColName(baseColumnName + i);
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
            columnStatisticsObjs.add(longColumnStatisticsObj);
        }
        columnStatistics.setStatsDesc(columnStatisticsDesc);
        columnStatistics.setStatsObj(columnStatisticsObjs);
        client.updateTableColumnStatistics(columnStatistics);

        List<ColumnStatisticsObj> statisticsObjs = client.getTableColumnStatistics(TEST_DB, tblName, new ArrayList(columns.keySet()));
        assertEquals("can't get all table column stats", statisticsObjs.size(), 1000);
        Set<String> cols = new HashSet<>();
        for (ColumnStatisticsObj obj : statisticsObjs) {
            assertEquals("can't get right column stats", obj.getStatsData().getLongStats().getHighValue(), 5);
            assertEquals("can't get right column stats", columns.get(obj.getColName()), "int");
            cols.add(obj.getColName());
        }
        assertEquals(cols.size(), 1000);
    }

    @Test
    //not support cascade
    public void testAlterTableCascade() throws Throwable {
        // create a table with multiple partitions
        String tblName = "comptbl";
        String typeName = "Person";

        List<List<String>> values = new ArrayList<List<String>>();
        values.add(makeVals("2008-07-01 14:13:12", "14"));
        values.add(makeVals("2008-07-01 14:13:12", "15"));
        values.add(makeVals("2008-07-02 14:13:12", "15"));
        values.add(makeVals("2008-07-03 14:13:12", "151"));

        createMultiPartitionTableSchema(TEST_DB, tblName, typeName, values);
        Table tbl = client.getTable(TEST_DB, tblName);
        List<FieldSchema> cols = tbl.getSd().getCols();
        cols.add(new FieldSchema("new_col", serdeConstants.STRING_TYPE_NAME, ""));
        tbl.getSd().setCols(cols);
        //add new column with cascade option
        client.alter_table(TEST_DB, tblName, tbl, true);
        //
        Table tbl2 = client.getTable(TEST_DB, tblName);
        Assert.assertEquals("Unexpected number of cols", 3, tbl2.getSd().getCols().size());
        Assert.assertEquals("Unexpected column name", "new_col", tbl2.getSd().getCols().get(2).getName());
        //get a partition
        List<String> pvalues = new ArrayList<>(2);
        pvalues.add("2008-07-01 14:13:12");
        pvalues.add("14");
        Partition partition = client.getPartition(TEST_DB, tblName, pvalues);
        Assert.assertEquals("Unexpected number of cols", 3, partition.getSd().getCols().size());
        Assert.assertEquals("Unexpected column name", "new_col", partition.getSd().getCols().get(2).getName());

        //add another column
        cols = tbl.getSd().getCols();
        cols.add(new FieldSchema("new_col2", serdeConstants.STRING_TYPE_NAME, ""));
        tbl.getSd().setCols(cols);
        //add new column with no cascade option
        client.alter_table(TEST_DB, tblName, tbl, false);
        tbl2 = client.getTable(TEST_DB, tblName);
        Assert.assertEquals("Unexpected number of cols", 4, tbl2.getSd().getCols().size());
        Assert.assertEquals("Unexpected column name", "new_col2", tbl2.getSd().getCols().get(3).getName());
        //get partition, this partition should not have the newly added column since cascade option
        //was false
        partition = client.getPartition(TEST_DB, tblName, pvalues);
        Assert.assertEquals("Unexpected number of cols", 3, partition.getSd().getCols().size());
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

    @Test
    public void testDropTable() throws Throwable {
        // create a table with multiple partitions
        String tblName = "comptbl";
        String typeName = "Person";

        List<List<String>> values = new ArrayList<List<String>>();
        values.add(makeVals("2008-07-01 14:13:12", "14"));
        values.add(makeVals("2008-07-01 14:13:12", "15"));
        values.add(makeVals("2008-07-02 14:13:12", "15"));
        values.add(makeVals("2008-07-03 14:13:12", "151"));

        createMultiPartitionTableSchema(TEST_DB, tblName, typeName, values);

        client.dropTable(TEST_DB, tblName);

        boolean exceptionThrown = false;
        try {
            client.getTable(TEST_DB, tblName);
        } catch (Exception e) {
            assertEquals("table should not have existed",
                    NoSuchObjectException.class, e.getClass());
            exceptionThrown = true;
        }
        assertTrue("Table " + tblName + " should have been dropped ", exceptionThrown);

    }

    @Test
    @Ignore
    public void testTableFilter() throws Exception {
        try {
            String owner1 = "testOwner1";
            String owner2 = "testOwner2";
            int lastAccessTime1 = 90;
            int lastAccessTime2 = 30;
            String tableName1 = "table1";
            String tableName2 = "table2";
            String tableName3 = "table3";

            client.dropTable(TEST_DB, tableName1);
            client.dropTable(TEST_DB, tableName2);
            client.dropTable(TEST_DB, tableName3);


            Table table1 = createTableForTestFilter(TEST_DB, tableName1, owner1, lastAccessTime1, true);
            Table table2 = createTableForTestFilter(TEST_DB, tableName2, owner2, lastAccessTime2, true);
            Table table3 = createTableForTestFilter(TEST_DB, tableName3, owner1, lastAccessTime2, false);

            List<String> tableNames;
            String filter;
            //test owner
            //owner like ".*Owner.*" and owner like "test.*"
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
                    " like \".*Owner.*\" and " +
                    org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
                    " like  \"test.*\"";
            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(tableNames.size(), 3);
            assert (tableNames.contains(table1.getTableName()));
            assert (tableNames.contains(table2.getTableName()));
            assert (tableNames.contains(table3.getTableName()));

            //owner = "testOwner1"
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
                    " = \"testOwner1\"";

            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(2, tableNames.size());
            assert (tableNames.contains(table1.getTableName()));
            assert (tableNames.contains(table3.getTableName()));

            //lastAccessTime < 90
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
                    " < 90";

            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(2, tableNames.size());
            assert (tableNames.contains(table2.getTableName()));
            assert (tableNames.contains(table3.getTableName()));

            //lastAccessTime > 90
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
                    " > 90";

            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(0, tableNames.size());

            //test params
            //test_param_2 = "50"
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
                    "test_param_2 LIKE \"50\"";

            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(2, tableNames.size());
            assert (tableNames.contains(table1.getTableName()));
            assert (tableNames.contains(table2.getTableName()));

            //test_param_2 = "75"
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
                    "test_param_2 LIKE \"75\"";

            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(0, tableNames.size());

            //key_dne = "50"
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
                    "key_dne LIKE \"50\"";

            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            assertEquals(0, tableNames.size());

            //test_param_1 != "yellow"
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
                    "test_param_1 NOT LIKE \"yellow\"";

            // Commenting as part of HIVE-12274 != and <> are not supported for CLOBs
            // tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
            // assertEquals(2, tableNames.size());

            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
                    "test_param_1 NOT LIKE \"yellow\"";

            // tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
            // assertEquals(2, tableNames.size());

            //owner = "testOwner1" and (lastAccessTime = 30 or test_param_1 = "hi")
            filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
                    " = \"testOwner1\" and (" +
                    org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
                    " = 30 or " +
                    org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
                    "test_param_1 LIKE \"hi\")";
            tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);

            assertEquals(2, tableNames.size());
            assert (tableNames.contains(table1.getTableName()));
            assert (tableNames.contains(table3.getTableName()));

            //Negative tests
            Exception me = null;
            try {
                filter = "badKey = \"testOwner1\"";
                tableNames = client.listTableNamesByFilter(TEST_DB, filter, (short) -1);
            } catch (MetaException e) {
                me = e;
            }
            assertNotNull(me);
            assertTrue("Bad filter key test", me.getMessage().contains(
                    "Invalid key name in filter"));

            client.dropTable(TEST_DB, tableName1);
            client.dropTable(TEST_DB, tableName2);
            client.dropTable(TEST_DB, tableName3);
            client.dropDatabase(TEST_DB);
        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testTableFilter() failed.");
            throw e;
        }
    }

    private Table createTableForTestFilter(String dbName, String tableName, String owner,
                                           int lastAccessTime, boolean hasSecondParam) throws Exception {

        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
        cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
        cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

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

    @Test
    public void testGetTableObjects() throws Exception {
        List<String> tableNames = Arrays.asList("table1", "table2", "table3", "table4", "table5");


        for (String tableName : tableNames) {
            createTable(TEST_DB, tableName);
        }

        // Test
        List<Table> tableObjs = client.getTableObjectsByName(TEST_DB, tableNames);

        // Verify
        assertEquals(tableNames.size(), tableObjs.size());
        for (Table table : tableObjs) {
            assertTrue(tableNames.contains(table.getTableName().toLowerCase()));
        }

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

    @Test
    public void testValidateTableCols() throws Throwable {

        try {
            String tblName = "comptbl";

            ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
            cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

            Table tbl = new Table();
            tbl.setDbName(TEST_DB);
            tbl.setTableName(tblName);
            StorageDescriptor sd = new StorageDescriptor();
            tbl.setSd(sd);
            sd.setCols(cols);
            sd.setCompressed(false);
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
            tbl = client.getTable(TEST_DB, tblName);
            //}

            List<String> expectedCols = Lists.newArrayList();
            expectedCols.add("name");
            ObjectStore objStore = new ObjectStore();
            try {
                objStore.validateTableCols(tbl, expectedCols);
            } catch (MetaException ex) {
                throw new RuntimeException(ex);
            }

            expectedCols.add("doesntExist");
            boolean exceptionFound = false;
            try {
                objStore.validateTableCols(tbl, expectedCols);
            } catch (MetaException ex) {
                assertEquals(ex.getMessage(),
                        "Column doesntExist doesn't exist in table comptbl in database " + TEST_DB);
                exceptionFound = true;
            }
            assertTrue(exceptionFound);

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testValidateTableCols() failed.");
            throw e;
        }
    }

    @Test
    public void testLocks() throws Exception {
        LockRequestBuilder rqstBuilder = new LockRequestBuilder(null);
        rqstBuilder.addLockComponent(new LockComponentBuilder()
                .setDbName(TEST_DB)
                .setTableName("mytable")
                .setPartitionName("mypartition")
                .setExclusive()
                .setOperationType(DataOperationType.NO_TXN)
                .build());
//        rqstBuilder.addLockComponent(new LockComponentBuilder()
//                .setDbName(TEST_DB)
//                .setTableName("yourtable")
//                .setSemiShared()
//                .setOperationType(DataOperationType.NO_TXN)
//                .build());
        rqstBuilder.setUser("fred");

        LockResponse res = client.lock(rqstBuilder.build());
        Assert.assertTrue(res.getLockid() > 0);
        Assert.assertEquals(LockState.ACQUIRED, res.getState());

//        res = client.checkLock(res.getLockid());
//        Assert.assertEquals(LockState.ACQUIRED, res.getState());

        client.heartbeat(0, res.getLockid());

        client.unlock(res.getLockid());
    }

    @Test
    public void testAlterTable() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String invTblName = TestUtil.TEST_TABLE + "alter-tbl";
        String tblName = TestUtil.TEST_TABLE + "altertbl";

        try {
            client.dropTable(dbName, tblName);

            ArrayList<FieldSchema> invCols = new ArrayList<FieldSchema>(2);
            invCols.add(new FieldSchema("n-ame", serdeConstants.STRING_TYPE_NAME, ""));
            invCols.add(new FieldSchema("in.come", serdeConstants.INT_TYPE_NAME, ""));

            Table tbl = new Table();
            tbl.setDbName(dbName);
            tbl.setTableName(invTblName);
            StorageDescriptor sd = new StorageDescriptor();
            tbl.setSd(sd);
            sd.setCols(invCols);
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
            sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
            sd.setInputFormat(HiveInputFormat.class.getName());
            sd.setOutputFormat(HiveOutputFormat.class.getName());

            boolean failed = false;
            try {
                client.createTable(tbl);
            } catch (InvalidObjectException ex) {
                failed = true;
            }
            if (!failed) {
                assertTrue("Able to create table with invalid name: " + invTblName,
                        false);
            }

            // create an invalid table which has wrong column type
            ArrayList<FieldSchema> invColsInvType = new ArrayList<FieldSchema>(2);
            invColsInvType.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            invColsInvType.add(new FieldSchema("income", "xyz", ""));
            tbl.setTableName(tblName);
            tbl.getSd().setCols(invColsInvType);
            boolean failChecker = false;
            try {
                client.createTable(tbl);
            } catch (InvalidObjectException ex) {
                failChecker = true;
            }
            if (!failChecker) {
                assertTrue("Able to create table with invalid column type: " + invTblName,
                        false);
            }

            ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
            cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
            cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

            // create a valid table
            tbl.setTableName(tblName);
            tbl.getSd().setCols(cols);
            client.createTable(tbl);
            tbl = client.getTable(tbl.getDbName(), tbl.getTableName());

            // now try to invalid alter table
            Table tbl2 = client.getTable(dbName, tblName);
            failed = false;
            try {
                tbl2.setTableName(invTblName);
                tbl2.getSd().setCols(invCols);
                client.alter_table(dbName, tblName, tbl2);
            } catch (InvalidObjectException ex) {
                failed = true;
            }
            if (!failed) {
                assertTrue("Able to rename table with invalid name: " + invTblName,
                        false);
            }

            //try an invalid alter table with partition key name
            Table tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
            List<FieldSchema> partitionKeys = tbl_pk.getPartitionKeys();
            for (FieldSchema fs : partitionKeys) {
                fs.setName("invalid_to_change_name");
                fs.setComment("can_change_comment");
            }
            tbl_pk.setPartitionKeys(partitionKeys);
            try {
                client.alter_table(dbName, tblName, tbl_pk);
            } catch (InvalidOperationException ex) {
                failed = true;
            }
            assertTrue("Should not have succeeded in altering partition key name", failed);

            //try a valid alter table partition key comment
            failed = false;
            tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
            partitionKeys = tbl_pk.getPartitionKeys();
            for (FieldSchema fs : partitionKeys) {
                fs.setComment("can_change_comment");
            }
            tbl_pk.setPartitionKeys(partitionKeys);
            try {
                client.alter_table(dbName, tblName, tbl_pk);
            } catch (InvalidObjectException ex) {
                failed = true;
            }
            assertFalse("Should not have failed alter table partition comment", failed);
            Table newT = client.getTable(tbl.getDbName(), tbl.getTableName());
            assertEquals(partitionKeys, newT.getPartitionKeys());

            // try a valid alter table
            tbl2.setTableName(tblName);
            tbl2.getSd().setCols(cols);
            tbl2.getSd().setNumBuckets(32);
            client.alter_table(dbName, tblName, tbl2);
            Table tbl3 = client.getTable(dbName, tbl2.getTableName());
            assertEquals("Alter table didn't succeed. Num buckets is different ",
                    tbl2.getSd().getNumBuckets(), tbl3.getSd().getNumBuckets());
            // check that data has moved
            tbl2.setTableName(tblName + "_renamed");
            client.alter_table(dbName, tblName, tbl2);
            tbl3 = client.getTable(dbName, tbl2.getTableName());
            FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
            assertFalse("old table location still exists", fs.exists(new Path(tbl
                    .getSd().getLocation())));
            assertTrue("data did not move to new location", fs.exists(new Path(tbl3
                    .getSd().getLocation())));

            assertEquals("alter table didn't move data correct location", tbl3.getSd().getLocation(), testDb.getLocationUri() + "/" + tbl3.getTableName());

            // alter table with invalid column type
            tbl_pk.getSd().setCols(invColsInvType);
            failed = false;
            try {
                client.alter_table(dbName, tbl2.getTableName(), tbl_pk);
            } catch (InvalidObjectException ex) {
                failed = true;
            }
            assertTrue("Should not have succeeded in altering column", failed);

            //alter table cols imcompatible
            Table tbl4 = client.getTable(dbName, tbl2.getTableName());
            tbl4.getSd().getCols().get(1).setType(serdeConstants.BOOLEAN_TYPE_NAME);
            try {
                client.alter_table(dbName, tbl4.getTableName(), tbl4);
                assertFalse("it's impossible to alter table cols imcompatible ", true);
            } catch (InvalidOperationException e) {
                System.out.println("alaer table invalid ---------");
            }
        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testSimpleTable() failed.");
            throw e;
        }
    }

    @Test
    public void testAlterTableNameAcrossDb() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String newDbName = dbName + "_renamed";
        client.dropDatabase(newDbName, true, true, true);
        testAlterTableName(dbName, newDbName);
    }

    @Test
    public void testAlterTableNameSameDb() throws Exception {
        String dbName = TestUtil.TEST_DB;
        testAlterTableName(dbName, dbName);
    }

    public void testAlterTableName(String dbName, String newDbName) throws Exception {

        String tblName = TestUtil.TEST_TABLE;

        try {
            client.dropTable(dbName, tblName);

            Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
            Table tbl = TestUtil.getTable(TEST_DB, tblName, columns);

            client.createTable(tbl);
            Table oldTbl = client.getTable(tbl.getDbName(), tbl.getTableName());


            Database newTestDb = null;
            if (newDbName.equals(dbName)) {
                newTestDb = client.getDatabase(newDbName);
            } else {
                newTestDb = TestUtil.getDatabase(newDbName);
                client.createDatabase(newTestDb);
            }
            String newTableName = tblName + "altertbl";
            tbl.setDbName(newDbName);
            tbl.setTableName(newTableName);

            //check table already exits
            checkRenameAlreadyExists(tbl, dbName, tblName, newDbName, newTableName);

            client.alter_table(dbName, tblName, tbl);
            Table newTable = client.getTable(newDbName, newTableName);
            FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
            assertFalse("old table location still exists", fs.exists(new Path(oldTbl
                    .getSd().getLocation())));
            assertTrue("data did not move to new location", fs.exists(new Path(newTable
                    .getSd().getLocation())));

            assertNotEquals("alter table didn't move data correct location", newTable
                    .getSd().getLocation(), oldTbl.getSd().getLocation());
            assertEquals("alter table didn't move data correct location", newTable.getSd().getLocation(), newTestDb.getLocationUri() + "/" + newTableName);

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testSimpleTable() failed.");
            throw e;
        } finally {
            if (!newDbName.equals(dbName)){
                client.dropDatabase(newDbName, true, true, true);
            }
        }
    }

    @Test
    public void testAlterPartitionTableNameAcrossDb() throws Exception {

        String dbName = TestUtil.TEST_DB;
        String newDbName = dbName + "_renamed";
        client.dropDatabase(newDbName, true, true, true);
        testAlterPartitionTableName(dbName, newDbName);
    }

    @Test
    public void testAlterPartitionTableNameSameDb() throws Exception {

        String dbName = TestUtil.TEST_DB;
        testAlterPartitionTableName(dbName, dbName);
    }

    public void testAlterPartitionTableName(String dbName, String newDbName) throws Exception {

        String tblName = TestUtil.TEST_TABLE;
        try {
            client.dropTable(dbName, tblName);
            Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
            Map<String, String> partCols = ImmutableMap.of("dt", "string");
            Table tbl = TestUtil.getPartitionTable(TEST_DB, tblName, columns, partCols);

            client.createTable(tbl);
            Table oldTbl = client.getTable(tbl.getDbName(), tbl.getTableName());
            String[] partValues = {"2020-20-23 12:34:15/1234", "2021-03-03 12:34:15/1234"};
            List<Partition> partitions = new ArrayList<>();
            Partition partition = TestUtil.buildPartition(dbName, tblName, tbl, 1, Lists.newArrayList(partValues[0]));
            partitions.add(partition);
            Partition partition2 = TestUtil.buildPartition(dbName, tblName, tbl, 1, Lists.newArrayList(partValues[1]));
            partition2.getSd().setLocation(partition2.getSd().getLocation().replace("/" + dbName, ""));
            partitions.add(partition2);

            client.add_partitions(partitions);

            Database newTestDb = null;
            if (newDbName.equals(dbName)) {
                newTestDb = client.getDatabase(newDbName);
            } else {
                newTestDb = TestUtil.getDatabase(newDbName);
                client.createDatabase(newTestDb);
            }
            String newTableName = tblName + "altertbl";
            tbl.setDbName(newDbName);
            tbl.setTableName(newTableName);
            //check table already exits
            checkRenameAlreadyExists(tbl, dbName, tblName, newDbName, newTableName);

            checkRenamedOldClientCompatible(tbl, tblName, newDbName, dbName, partValues, partition, partition2);

            //tbl rename
            client.alter_table(dbName, tblName, tbl);
            Table newTable = client.getTable(newDbName, newTableName);
            FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
            assertFalse("old table location still exists", fs.exists(new Path(oldTbl
                    .getSd().getLocation())));
            assertTrue("data did not move to new location", fs.exists(new Path(newTable
                    .getSd().getLocation())));

            assertNotEquals("alter table didn't move data correct location", newTable
                    .getSd().getLocation(), oldTbl.getSd().getLocation());
            assertEquals("alter table didn't move data correct location", newTable.getSd().getLocation(), newTestDb.getLocationUri() + "/" + newTableName);

            //partition check
            Partition partitionNew = client.getPartition(newDbName, newTableName, Lists.newArrayList(partValues[0]));
            Partition partitionNew2 = client.getPartition(newDbName, newTableName, Lists.newArrayList(partValues[1]));

            assertFalse("old partition location still exists", fs.exists(new Path(partition
                    .getSd().getLocation())));
            assertTrue("old partition2 location isn't exists", fs.exists(new Path(partition2
                    .getSd().getLocation())));

            assertEquals("alter partition didn't move data correct location", partition.getSd().getLocation().replace(dbName, newDbName).replace(tblName, newTableName), partitionNew.getSd().getLocation());

            assertEquals("alter partition2  move data correct location", partition2.getSd().getLocation(), partitionNew2.getSd().getLocation());

        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testSimpleTable() failed.");
            throw e;
        } finally {
            if (!newDbName.equals(dbName)){
                client.dropDatabase(newDbName, true, true, true);
            }
        }
    }

    public void checkRenameAlreadyExists(Table tbl, String dbName, String tblName, String newDbName, String newTableName) throws TException, IOException {
        String preTableTlocation = tbl.getSd().getLocation();
        String alreadyExitsTableLocation = tbl.getSd().getLocation().replace(tblName, "xxxx");
        try {
            tbl.getSd().setLocation(alreadyExitsTableLocation);
            client.createTable(tbl);
            //新老表的location相同
            tbl.getSd().setLocation(preTableTlocation);
            client.alter_table(dbName, tblName, tbl);
            assertEquals("expected failed in alter table,but not", true, false);
        } catch (Exception e) {
            //already table的location没有变更，说明没有rename成功，覆盖表的location
            assertEquals(client.getTable(newDbName, newTableName).getSd().getLocation(), alreadyExitsTableLocation);
            //新表的location没有出现，回滚rename后的location
            assertFalse(fs.exists(new Path(preTableTlocation.replace(dbName, newDbName).replace(tblName, newTableName))));
            //删除创建的表，保证下面alter name不会出错
            client.dropTable(newDbName, newTableName, true, true, true);
        }
    }

    public void checkRenamedOldClientCompatible(Table tbl, String tblName, String newDbName, String oldDbName, String[] partValues, Partition partition, Partition partition2) throws TException {
        if (newDbName.equals(oldDbName)) {
            //check oldclient is ok. oldclient will set renametablelocation
            String newTableName = tblName + "altertbl_old";
            tbl.setDbName(newDbName);
            tbl.setTableName(newTableName);
            tbl.getSd().setLocation(tbl.getSd().getLocation().replace(tblName, newTableName));
            //rename
            client.alter_table(newDbName, tblName, tbl);
            try {
                Table getTable = client.getTable(newDbName, newTableName);
                //table's location should be renamed
                assertEquals(getTable.getSd().getLocation(), tbl.getSd().getLocation());
                FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
                //in new client's logic, it should not mkdir
                assertFalse("new table location exists", fs.exists(new Path(getTable.getSd().getLocation())));

                //partition's location should be auto renamed, but not mkdir
                Partition partitionNew = client.getPartition(newDbName, newTableName, Lists.newArrayList(partValues[0]));
                Partition partitionNew2 = client.getPartition(newDbName, newTableName, Lists.newArrayList(partValues[1]));
                assertFalse("new partition location exists", fs.exists(new Path(partitionNew.getSd().getLocation())));
                assertTrue("old partition2 location isn't exists", fs.exists(new Path(partition2.getSd().getLocation())));

                assertEquals("alter partition didn't move data correct location", partition.getSd().getLocation().replace(oldDbName, newDbName).replace(tblName, newTableName), partitionNew.getSd().getLocation());
                assertEquals("alter partition2  move data correct location", partition2.getSd().getLocation(), partitionNew2.getSd().getLocation());
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue("failed to rename", false);
            }

            //rollback
            tbl.setTableName(tblName);
            tbl.getSd().setLocation(tbl.getSd().getLocation().replace(newTableName, tblName));
            client.alter_table(newDbName, newTableName, tbl);
            try {
                Table getTable = client.getTable(newDbName, tblName);
                tbl.setDbName(newDbName);
                tbl.setTableName(tblName + "altertbl");
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue("failed to rollback rename", false);
            }
        }
    }
}
