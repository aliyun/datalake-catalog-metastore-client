package com.aliyun.datalake.metastore.hive2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

public class StatsTest {
    private static final Logger LOG = LoggerFactory.getLogger(StatsTest.class);

    private static final String NO_CAT = "";

    private IMetaStoreClient client;
    private HiveConf conf;

    @Before
    public void setUp() throws MetaException {
        conf = new HiveConf();
//        MetaStoreTestUtils.setConfForStandloneMode(conf);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AGGREGATE_STATS_CACHE_ENABLED, false);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
        // Get new client
        client = new ProxyMetaStoreClient(conf);
    }

    @After
    public void tearDown() throws TException {
        // Drop any left over catalogs
//        List<String> catalogs = new ArrayList<String>(){{add(NO_CAT);}};
//        for (String catName : catalogs) {
//            if (!catName.equalsIgnoreCase(NO_CAT)) {
//                // First drop any databases in catalog
//                List<String> databases = client.getAllDatabases(catName);
//                for (String db : databases) {
//                    if (!db.equals("default")) {
//                        client.dropDatabase(catName, db, true, false, true);
//                    }
//                }
//                //client.dropCatalog(catName);
//            } else {
//                List<String> databases = client.getAllDatabases(catName);
//                for (String db : databases) {
//                    if (!db.equalsIgnoreCase(Warehouse.DEFAULT_DATABASE_NAME)) {
//                        client.dropDatabase(catName, db, true, false, true);
//                    }
//                }
//            }
//        }
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            client = null;
        }
    }

    private Map<String, Column> buildAllColumns() {
        Map<String, Column> colMap = new HashMap<>(6);
        Column[] cols = {new BinaryColumn(), new BooleanColumn(), new DateColumn(),
                new DoubleColumn(), new LongColumn(), new StringColumn()};
        for (Column c : cols) colMap.put(c.colName, c);
        return colMap;
    }

    private List<String> createMetadata(String catName, String dbName, String tableName,
                                        String partKey, List<String> partVals,
                                        Map<String, Column> colMap)
            throws TException {
        if (!NO_CAT.equals(catName) && !NO_CAT.equals(catName)) {
            Catalog cat = new CatalogBuilder()
                    .setName(catName)
                    .setLocation(TestUtil.getDefaultDatabasePath(dbName).toString())
                    .build();
            //client.createCatalog(cat);
        }

        Database db;
        if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
            DatabaseBuilder dbBuilder = new DatabaseBuilder()
                    .setName(dbName).setLocation(TestUtil.WAREHOUSE_PATH + dbName);
            if (!NO_CAT.equals(catName)) dbBuilder.setCatalogName(catName);
            db = dbBuilder.create(client, conf);
        } else {
            db = client.getDatabase(NO_CAT, DEFAULT_DATABASE_NAME);
        }

        TableBuilder tb = new TableBuilder()
                .inDb(db)
                .setTableName(tableName)
                .setLocation(TestUtil.WAREHOUSE_PATH + dbName + "/" + tableName);

        for (Column col : colMap.values()) tb.addCol(col.colName, col.colType);

        if (partKey != null) {
            assert partVals != null && !partVals.isEmpty() :
                    "Must provide partition values for partitioned table";
            tb.addPartCol(partKey, ColumnType.STRING_TYPE_NAME);
        }
        client.dropTable(dbName, tableName, true, true, true);
        Table table = tb.create(client, conf);

        if (partKey != null) {
            for (String partVal : partVals) {
                new PartitionBuilder()
                        .inTable(table)
                        .addValue(partVal)
                        .addToTable(client, conf);
            }
        }

        SetPartitionsStatsRequest rqst = new SetPartitionsStatsRequest();
        List<String> partNames = new ArrayList<>();
        if (partKey == null) {
            rqst.addToColStats(buildStatsForOneTableOrPartition(catName, dbName, tableName, null,
                    colMap.values()));
        } else {
            for (String partVal : partVals) {
                String partName = partKey + "=" + partVal;
                rqst.addToColStats(buildStatsForOneTableOrPartition(catName, dbName, tableName, partName,
                        colMap.values()));
                partNames.add(partName);
            }
        }
        client.setPartitionColumnStatistics(rqst);
        return partNames;
    }

    private ColumnStatistics buildStatsForOneTableOrPartition(String catName, String dbName,
                                                              String tableName, String partName,
                                                              Collection<Column> cols) {
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(partName == null, dbName, tableName);
        if (!NO_CAT.equals(catName)) desc.setCatName(catName);
        if (partName != null) desc.setPartName(partName);

        List<ColumnStatisticsObj> objs = new ArrayList<>(cols.size());

        for (Column col : cols) objs.add(col.generate());

        return new ColumnStatistics(desc, objs);
    }

    private void dropStats(String catName, String dbName, String tableName, String partName,
                           Collection<String> colNames)
            throws TException {
        for (String colName : colNames) {
            if (partName == null) {
                if (NO_CAT.equals(catName)) client.deleteTableColumnStatistics(dbName, tableName, colName);
                else client.deleteTableColumnStatistics(catName, dbName, tableName, colName);
            } else {
                if (NO_CAT.equals(catName))
                    client.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
                else client.deletePartitionColumnStatistics(catName, dbName, tableName, partName, colName);
            }
        }
    }

    private void compareStatsForTable(String catName, String dbName, String tableName,
                                      Map<String, Column> colMap) throws TException {
        List<ColumnStatisticsObj> objs = catName.equals(NO_CAT) ?
                client.getTableColumnStatistics(dbName, tableName, new ArrayList<>(colMap.keySet())) :
                client.getTableColumnStatistics(catName, dbName, tableName, new ArrayList<>(colMap.keySet()));
        compareStatsForOneTableOrPartition(objs, 0, colMap);
    }

    private void compareStatsForPartitions(String catName, String dbName, String tableName,
                                           List<String> partNames, final Map<String, Column> colMap)
            throws TException {
        Map<String, List<ColumnStatisticsObj>> partObjs = catName.equals(NO_CAT) ?
                client.getPartitionColumnStatistics(dbName, tableName, partNames, new ArrayList<>(colMap.keySet())) :
                client.getPartitionColumnStatistics(catName, dbName, tableName, partNames, new ArrayList<>(colMap.keySet()));
        for (int i = 0; i < partNames.size(); i++) {
            compareStatsForOneTableOrPartition(partObjs.get(partNames.get(i)), i, colMap);
        }
        AggrStats aggr = catName.equals(NO_CAT) ?
                client.getAggrColStatsFor(dbName, tableName, new ArrayList<>(colMap.keySet()), partNames) :
                client.getAggrColStatsFor(catName, dbName, tableName, new ArrayList<>(colMap.keySet()), partNames);
        Assert.assertEquals(partNames.size(), aggr.getPartsFound());
        Assert.assertEquals(colMap.size(), aggr.getColStatsSize());
        aggr.getColStats().forEach(cso -> colMap.get(cso.getColName()).compareAggr(cso));
    }

    private void compareStatsForOneTableOrPartition(List<ColumnStatisticsObj> objs,
                                                    final int partOffset,
                                                    final Map<String, Column> colMap)
            throws TException {
        Assert.assertEquals(objs.size(), colMap.size());
        objs.forEach(cso -> colMap.get(cso.getColName()).compare(cso, partOffset));
    }

    @Test
    public void tableInHiveCatalog() throws TException {
        String dbName = "metastore_ut_db_table_stats";
        String tableName = "metastore_ut_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        createMetadata(NO_CAT, dbName, tableName, null, null, colMap);
        compareStatsForTable(NO_CAT, dbName, tableName, colMap);
        dropStats(NO_CAT, dbName, tableName, null, colMap.keySet());
        client.dropDatabase(NO_CAT, dbName, true, true, true);
    }

    @Ignore("HIVE-19509: Disable tests that are failing continuously")
    @Test
    public void partitionedTableInHiveCatalog() throws TException {
        String dbName = "db_part_stats";
        String tableName = "partitioned_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        List<String> partNames = createMetadata(NO_CAT, dbName, tableName, "pk",
                Arrays.asList("a1", "a2", "a3"), colMap);
        compareStatsForPartitions(NO_CAT, dbName, tableName, partNames, colMap);
        for (String partName : partNames) {
            dropStats(NO_CAT, dbName, tableName, partName, colMap.keySet());
        }
        client.dropDatabase(NO_CAT, dbName, true, false, true);
    }

    @Test
    public void tableOtherCatalog() throws TException {
        String catName = NO_CAT;
        String dbName = "metastore_ut_other_cat_db_table_stats";
        String tableName = "metastore_ut_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        client.dropDatabase(NO_CAT, dbName, true, true, true);
        createMetadata(catName, dbName, tableName, null, null, colMap);
        compareStatsForTable(catName, dbName, tableName, colMap);
        dropStats(catName, dbName, tableName, null, colMap.keySet());
        client.dropDatabase(NO_CAT, dbName, true, true, true);
    }

    @Ignore("HIVE-19509: Disable tests that are failing continuously")
    @Test
    public void partitionedTableOtherCatalog() throws TException {
        String catName = "cat_table_stats";
        String dbName = "other_cat_db_part_stats";
        String tableName = "partitioned_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        List<String> partNames = createMetadata(catName, dbName, tableName, "pk",
                Arrays.asList("a1", "a2", "a3"), colMap);
        compareStatsForPartitions(catName, dbName, tableName, partNames, colMap);
        for (String partName : partNames) {
            dropStats(catName, dbName, tableName, partName, colMap.keySet());
        }
        client.dropDatabase(catName, dbName, true, false, true);
    }

    @Test
    public void tableDeprecatedCalls() throws TException {
        String dbName = "metastore_ut_old_db_table_stats";
        String tableName = "metastore_ut_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        createMetadata(NO_CAT, dbName, tableName, null, null, colMap);
        compareStatsForTable(NO_CAT, dbName, tableName, colMap);
        dropStats(NO_CAT, dbName, tableName, null, colMap.keySet());
        client.dropDatabase(NO_CAT, dbName, true, false, true);
    }

    @Ignore("HIVE-19509: Disable tests that are failing continuously")
    @Test
    public void partitionedTableDeprecatedCalls() throws TException {
        String dbName = "old_db_part_stats";
        String tableName = "partitioned_table_in_default_db_stats";
        Map<String, Column> colMap = buildAllColumns();
        List<String> partNames = createMetadata(NO_CAT, dbName, tableName, "pk",
                Arrays.asList("a1", "a2", "a3"), colMap);
        compareStatsForPartitions(NO_CAT, dbName, tableName, partNames, colMap);
        for (String partName : partNames) {
            dropStats(NO_CAT, dbName, tableName, partName, colMap.keySet());
        }
        client.dropDatabase(NO_CAT, dbName, true, false, true);
    }

    private abstract class Column {
        final String colName;
        final String colType;

        Random rand = new Random();

        List<Long> maxLens, numNulls, numDvs;
        List<Double> avgLens;


        public Column(String colName, String colType) {
            this.colName = colName;
            this.colType = colType;
            maxLens = new ArrayList<>();
            numNulls = new ArrayList<>();
            avgLens = new ArrayList<>();
            numDvs = new ArrayList<>();
        }

        abstract ColumnStatisticsObj generate();

        abstract void compare(ColumnStatisticsObj obj, int offset);

        abstract void compareAggr(ColumnStatisticsObj obj);

        void compareCommon(ColumnStatisticsObj obj) {
            Assert.assertEquals(colName, obj.getColName());
            Assert.assertEquals(colType, obj.getColType());
        }

        long genMaxLen() {
            return genPositiveLong(maxLens);
        }

        long getMaxLen() {
            return maxLong(maxLens);
        }

        long genNumNulls() {
            return genPositiveLong(numNulls);
        }

        long genNumDvs() {
            return genPositiveLong(numDvs);
        }

        long getNumNulls() {
            return sumLong(numNulls);
        }

        long getNumDvs() {
            return maxLong(numDvs);
        }

        double genAvgLens() {
            return genDouble(avgLens);
        }

        double getAvgLen() {
            return maxDouble(avgLens);
        }

        protected long genNegativeLong(List<Long> addTo) {
            long val = rand.nextInt(100);
            if (val > 0) val *= -1;
            addTo.add(val);
            return val;
        }

        protected long genPositiveLong(List<Long> addTo) {
            long val = rand.nextInt(100);
            val = Math.abs(val) + 1; // make sure it isn't 0
            addTo.add(val);
            return val;
        }

        protected long maxLong(List<Long> maxOf) {
            long max = Long.MIN_VALUE;
            for (long maybe : maxOf) max = Math.max(max, maybe);
            return max;
        }

        protected long sumLong(List<Long> sumOf) {
            long sum = 0;
            for (long element : sumOf) sum += element;
            return sum;
        }

        protected double genDouble(List<Double> addTo) {
            double val = rand.nextDouble() * rand.nextInt(100);
            addTo.add(val);
            return val;
        }

        protected double maxDouble(List<Double> maxOf) {
            double max = Double.MIN_VALUE;
            for (double maybe : maxOf) max = Math.max(max, maybe);
            return max;
        }

    }

    private class BinaryColumn extends Column {
        public BinaryColumn() {
            super("bincol", ColumnType.BINARY_TYPE_NAME);
        }

        @Override
        ColumnStatisticsObj generate() {
            BinaryColumnStatsData binData = new BinaryColumnStatsData(genMaxLen(), genAvgLens(), genNumNulls());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setBinaryStats(binData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("binary max length", maxLens.get(offset),
                    (Long) obj.getStatsData().getBinaryStats().getMaxColLen());
            Assert.assertEquals("binary min length", avgLens.get(offset), obj.getStatsData().getBinaryStats().getAvgColLen(), 0.01);
            Assert.assertEquals("binary num nulls", numNulls.get(offset), (Long) obj.getStatsData().getBinaryStats().getNumNulls());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr binary max length", getMaxLen(), obj.getStatsData().getBinaryStats().getMaxColLen());
            Assert.assertEquals("aggr binary min length", getAvgLen(), obj.getStatsData().getBinaryStats().getAvgColLen(), 0.01);
            Assert.assertEquals("aggr binary num nulls", getNumNulls(), obj.getStatsData().getBinaryStats().getNumNulls());
        }
    }

    private class BooleanColumn extends Column {
        private List<Long> numTrues, numFalses;

        public BooleanColumn() {
            super("boolcol", ColumnType.BOOLEAN_TYPE_NAME);
            numTrues = new ArrayList<>();
            numFalses = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            BooleanColumnStatsData
                    boolData = new BooleanColumnStatsData(genNumTrues(), genNumFalses(), genNumNulls());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setBooleanStats(boolData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("boolean num trues", numTrues.get(offset), (Long) obj.getStatsData().getBooleanStats().getNumTrues());
            Assert.assertEquals("boolean num falses", numFalses.get(offset), (Long) obj.getStatsData().getBooleanStats().getNumFalses());
            Assert.assertEquals("boolean num nulls", numNulls.get(offset), (Long) obj.getStatsData().getBooleanStats().getNumNulls());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr boolean num trues", getNumTrues(), obj.getStatsData().getBooleanStats().getNumTrues());
            Assert.assertEquals("aggr boolean num falses", getNumFalses(), obj.getStatsData().getBooleanStats().getNumFalses());
            Assert.assertEquals("aggr boolean num nulls", getNumNulls(), obj.getStatsData().getBooleanStats().getNumNulls());
        }

        private long genNumTrues() {
            return genPositiveLong(numTrues);
        }

        private long genNumFalses() {
            return genPositiveLong(numFalses);
        }

        private long getNumTrues() {
            return sumLong(numTrues);
        }

        private long getNumFalses() {
            return sumLong(numFalses);
        }
    }

    private class DateColumn extends Column {
        private List<Date> lowVals, highVals;

        public DateColumn() {
            super("datecol", ColumnType.DATE_TYPE_NAME);
            lowVals = new ArrayList<>();
            highVals = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            DateColumnStatsData dateData = new DateColumnStatsData(genNumNulls(), genNumDvs());
            dateData.setLowValue(genLowValue());
            dateData.setHighValue(genHighValue());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setDateStats(dateData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("date num nulls", numNulls.get(offset), (Long) obj.getStatsData().getDateStats().getNumNulls());
            Assert.assertEquals("date num dvs", numDvs.get(offset), (Long) obj.getStatsData().getDateStats().getNumDVs());
            Assert.assertEquals("date low val", lowVals.get(offset), obj.getStatsData().getDateStats().getLowValue());
            Assert.assertEquals("date high val", highVals.get(offset), obj.getStatsData().getDateStats().getHighValue());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr date num nulls", getNumNulls(), obj.getStatsData().getDateStats().getNumNulls());
            Assert.assertEquals("aggr date num dvs", getNumDvs(), obj.getStatsData().getDateStats().getNumDVs());
            Assert.assertEquals("aggr date low val", getLowVal(), obj.getStatsData().getDateStats().getLowValue());
            Assert.assertEquals("aggr date high val", getHighVal(), obj.getStatsData().getDateStats().getHighValue());
        }

        private Date genLowValue() {
            Date d = new Date(rand.nextInt(100) * -1);
            lowVals.add(d);
            return d;
        }

        private Date genHighValue() {
            Date d = new Date(rand.nextInt(200));
            highVals.add(d);
            return d;
        }

        private Date getLowVal() {
            long min = Long.MAX_VALUE;
            for (Date d : lowVals) min = Math.min(min, d.getDaysSinceEpoch());
            return new Date(min);
        }

        private Date getHighVal() {
            long max = Long.MIN_VALUE;
            for (Date d : highVals) max = Math.max(max, d.getDaysSinceEpoch());
            return new Date(max);
        }
    }

    private class DoubleColumn extends Column {
        List<Double> lowVals, highVals;

        public DoubleColumn() {
            super("doublecol", ColumnType.DOUBLE_TYPE_NAME);
            lowVals = new ArrayList<>();
            highVals = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            DoubleColumnStatsData doubleData = new DoubleColumnStatsData(genNumNulls(), genNumDvs());
            doubleData.setLowValue(genLowVal());
            doubleData.setHighValue(genHighVal());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setDoubleStats(doubleData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("double num nulls", numNulls.get(offset),
                    (Long) obj.getStatsData().getDoubleStats().getNumNulls());
            Assert.assertEquals("double num dvs", numDvs.get(offset),
                    (Long) obj.getStatsData().getDoubleStats().getNumDVs());
            Assert.assertEquals("double low val", lowVals.get(offset),
                    obj.getStatsData().getDoubleStats().getLowValue(), 0.01);
            Assert.assertEquals("double high val", highVals.get(offset),
                    obj.getStatsData().getDoubleStats().getHighValue(), 0.01);
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr double num nulls", getNumNulls(),
                    obj.getStatsData().getDoubleStats().getNumNulls());
            Assert.assertEquals("aggr double num dvs", getNumDvs(),
                    obj.getStatsData().getDoubleStats().getNumDVs());
            Assert.assertEquals("aggr double low val", getLowVal(),
                    obj.getStatsData().getDoubleStats().getLowValue(), 0.01);
            Assert.assertEquals("aggr double high val", getHighVal(),
                    obj.getStatsData().getDoubleStats().getHighValue(), 0.01);

        }

        private double genLowVal() {
            return genDouble(lowVals);
        }

        private double genHighVal() {
            return genDouble(highVals);
        }

        private double getLowVal() {
            double min = Double.MAX_VALUE;
            for (Double d : lowVals) min = Math.min(min, d);
            return min;
        }

        private double getHighVal() {
            return maxDouble(highVals);
        }
    }

    private class LongColumn extends Column {
        List<Long> lowVals, highVals;

        public LongColumn() {
            super("bigintcol", ColumnType.BIGINT_TYPE_NAME);
            lowVals = new ArrayList<>();
            highVals = new ArrayList<>();
        }

        @Override
        ColumnStatisticsObj generate() {
            LongColumnStatsData longData = new LongColumnStatsData(genNumNulls(), genNumDvs());
            longData.setLowValue(genLowVal());
            longData.setHighValue(genHighVal());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setLongStats(longData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("long num nulls", numNulls.get(offset),
                    (Long) obj.getStatsData().getLongStats().getNumNulls());
            Assert.assertEquals("long num dvs", numDvs.get(offset),
                    (Long) obj.getStatsData().getLongStats().getNumDVs());
            Assert.assertEquals("long low val", (long) lowVals.get(offset),
                    obj.getStatsData().getLongStats().getLowValue());
            Assert.assertEquals("long high val", (long) highVals.get(offset),
                    obj.getStatsData().getLongStats().getHighValue());
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr long num nulls", getNumNulls(),
                    obj.getStatsData().getLongStats().getNumNulls());
            Assert.assertEquals("aggr long num dvs", getNumDvs(),
                    obj.getStatsData().getLongStats().getNumDVs());
            Assert.assertEquals("aggr long low val", getLowVal(),
                    obj.getStatsData().getLongStats().getLowValue());
            Assert.assertEquals("aggr long high val", getHighVal(),
                    obj.getStatsData().getLongStats().getHighValue());
        }

        private long genLowVal() {
            return genNegativeLong(lowVals);
        }

        private long genHighVal() {
            return genPositiveLong(highVals);
        }

        private long getLowVal() {
            long min = Long.MAX_VALUE;
            for (Long val : lowVals) min = Math.min(min, val);
            return min;
        }

        private long getHighVal() {
            return maxLong(highVals);
        }
    }

    private class StringColumn extends Column {
        public StringColumn() {
            super("strcol", ColumnType.STRING_TYPE_NAME);
        }

        @Override
        ColumnStatisticsObj generate() {
            StringColumnStatsData strData = new StringColumnStatsData(genMaxLen(), genAvgLens(),
                    genNumNulls(), genNumDvs());
            ColumnStatisticsData data = new ColumnStatisticsData();
            data.setStringStats(strData);
            return new ColumnStatisticsObj(colName, colType, data);
        }

        @Override
        void compare(ColumnStatisticsObj obj, int offset) {
            compareCommon(obj);
            Assert.assertEquals("str num nulls", numNulls.get(offset),
                    (Long) obj.getStatsData().getStringStats().getNumNulls());
            Assert.assertEquals("str num dvs", numDvs.get(offset),
                    (Long) obj.getStatsData().getStringStats().getNumDVs());
            Assert.assertEquals("str low val", (long) maxLens.get(offset),
                    obj.getStatsData().getStringStats().getMaxColLen());
            Assert.assertEquals("str high val", avgLens.get(offset),
                    obj.getStatsData().getStringStats().getAvgColLen(), 0.01);
        }

        @Override
        void compareAggr(ColumnStatisticsObj obj) {
            compareCommon(obj);
            Assert.assertEquals("aggr str num nulls", getNumNulls(),
                    obj.getStatsData().getStringStats().getNumNulls());
            Assert.assertEquals("aggr str num dvs", getNumDvs(),
                    obj.getStatsData().getStringStats().getNumDVs());
            Assert.assertEquals("aggr str low val", getMaxLen(),
                    obj.getStatsData().getStringStats().getMaxColLen());
            Assert.assertEquals("aggr str high val", getAvgLen(),
                    obj.getStatsData().getStringStats().getAvgColLen(), 0.01);

        }
    }
}