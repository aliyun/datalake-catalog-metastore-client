package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.functional.ThrowingConsumer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.junit.Assert;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.TableType.INDEX_TABLE;

public class TestUtil {
    public static final String MANAGED_TABLE_TYPE = "MANAGED_TABLE";
    public static final String OWNER = "EMR";
    public static final String WAREHOUSE_BASE_PATH = "file://" + System.getProperty("user.dir") + "/metastore-ut/";
    public static final String WAREHOUSE_PATH = WAREHOUSE_BASE_PATH + "hive/warehouse/";

    public static final String DEFAULT_OWNER_NAME = "Mr.test";
    public static final PrincipalType DEFAULT_OWNER_TYPE = PrincipalType.USER;
    public static final String TEST_DB = "metastore_ut_test_db";
    public static final String TEST_TABLE = "metastore_ut_test_tb";

    public static IMetaStoreClient getHMSClient() throws MetaException, FileNotFoundException {
        HiveConf conf = new HiveConf();
        return new ProxyMetaStoreClient(conf);
    }

    public static IMetaStoreClient getDlfClient() throws MetaException, FileNotFoundException {
        HiveConf conf = new HiveConf();
        return new ProxyMetaStoreClient(conf);
    }

    public static String randomNumStr(String name) {
        name = name == null ? "" : name;
        return name + "_" + UUID.randomUUID().toString().replace("-", "_");
    }

    /**
     * Return the default database path.
     */
    public static Path getDefaultDatabasePath(String dbName) {
        String path = WAREHOUSE_PATH + dbName + ".db";
        return new Path(path);
    }

    /**
     * Return the default table path.
     */
    public static Path getDefaultTablePath(String dbName, String tableName) {
        return new Path(getDefaultDatabasePath(dbName), tableName);
    }

    public static Database getDatabase(String dbName) {
        return DatabaseBuilder.builder()
                .withDbName(dbName)
                .withLocationUri(getDefaultDatabasePath(dbName).toString())
                .build();
    }

    public static Database getDatabase(String dbName, String locationUri) {
        return DatabaseBuilder.builder()
                .withDbName(dbName)
                .withLocationUri(locationUri)
                .build();
    }

    public static Table getTable(
            String dbName,
            String tableName,
            Map<String, String> cols
    ) {
        return TableBuilder.builder()
                .withDbName(dbName)
                .withTableName(tableName)
                .withStorageDescriptor(getStorageDescriptor(dbName, tableName, cols))
                .build();
    }

    public static Table getTable(
            String dbName,
            String tableName,
            TableType tableType,
            Map<String, String> cols
    ) {
        return TableBuilder.builder()
                .withDbName(dbName)
                .withTableName(tableName)
                .withTableType(tableType)
                .withStorageDescriptor(getStorageDescriptor(dbName, tableName, cols))
                .build();
    }

    public static Table getTable(
            String dbName,
            String tableName,
            Map<String, String> cols,
            Map<String, String> params
    ) {
        return TableBuilder.builder()
                .withDbName(dbName)
                .withTableName(tableName)
                .withParameters(params)
                .withStorageDescriptor(getStorageDescriptor(dbName, tableName, cols))
                .build();
    }

    // --------------------------------------------------------------------------
    // Belows are util functions to get objects, like databases, tables, partitions and functions, etl.
    // --------------------------------------------------------------------------

    public static Table getPartitionTable(
            String dbName,
            String tableName,
            Map<String, String> cols,
            Map<String, String> partKeys
    ) {
        List<FieldSchema> pKeys = getFieldSchemas(partKeys, "");
        return TableBuilder.builder()
                .withDbName(dbName)
                .withTableName(tableName)
                .withPartitionKeys(pKeys)
                .withStorageDescriptor(getStorageDescriptor(dbName, tableName, cols))
                .build();
    }

    public static Table getView(
            String dbName,
            String viewName,
            Map<String, String> cols
    ) {
        List<FieldSchema> columns = getFieldSchemas(cols, "");
        StorageDescriptor sd = StorageDescriptorBuilder.builder()
                .withCols(columns)
                .build();
        return TableBuilder.builder()
                .withDbName(dbName)
                .withTableName(viewName)
                .withTableType(TableType.VIRTUAL_VIEW)
                .withStorageDescriptor(sd)
                .build();
    }

    public static List<FieldSchema> getFieldSchemas(Map<String, String> cols, String s) {
        return cols.entrySet().stream()
                .map(e -> new FieldSchema(e.getKey(), e.getValue(), s))
                .collect(Collectors.toList());
    }

    public static StorageDescriptor getStorageDescriptor(
            String dbName,
            String tableName,
            Map<String, String> cols
    ) {
        List<FieldSchema> fields = getFieldSchemas(cols, "test");
        return StorageDescriptorBuilder.builder()
                .withCols(fields)
                .withLocation(getDefaultTablePath(dbName, tableName).toString())
                .build();
    }

    public static StorageDescriptor getPartitionStorageDescriptor(
            String dbName,
            String tableName,
            Map<String, String> cols,
            Map<String, String> partCols,
            List<String> partValues
    ) throws MetaException {
        List<FieldSchema> fields = getFieldSchemas(cols, "");
        List<FieldSchema> partKeys = getFieldSchemas(partCols, "");
        return StorageDescriptorBuilder.builder()
                .withCols(fields)
                .withLocation(getDefaultTablePath(dbName, tableName).toString() + "/" +
                        Warehouse.makePartName(partKeys, partValues))
                .build();
    }

    public static Partition getPartition(
            String dbName,
            String tableName,
            Map<String, String> cols,
            Map<String, String> partCols,
            List<String> partValues
    ) throws MetaException {
        return PartitionBuilder.builder()
                .withDbName(dbName)
                .withTableName(tableName)
                .withValues(partValues)
                .withStorageDescriptor(getPartitionStorageDescriptor(dbName, tableName, cols, partCols, partValues))
                .build();
    }

    public static Function getFunction(String dbName, String funcName, String className) {
        return new FunctionBuilder().builder()
                .withDbName(dbName)
                .withFuncName(funcName)
                .withClassName(className)
                .build();
    }

    public static Database buildDatabase(String name) {
        String path = WAREHOUSE_PATH + name + ".db";
        return TestUtil.buildDatabase(name, path);
    }

    public static Database buildDatabase(String name, String location) {
        String description = "Default Hive database";
        HashMap<String, String> map = Maps.newHashMap();
        map.put("testKeyP", "testValueP");
        return new Database(name, description, location, null);
    }

    public static Table buildTable(String databaseName, String tableName) {
        return buildTable(databaseName, tableName, Lists.newArrayList("day"), MANAGED_TABLE_TYPE);
    }

    public static Table buildTable(String databaseName, String tableName, String type) {
        return buildTable(databaseName, tableName, Lists.newArrayList("day"), type);
    }

    public static Table buildTable(String databaseName, String tableName, List<String> partitionKeys,
                                   String tableType) {
        Table table = new Table();
        table.setTableType(tableType);
        table.setDbName(databaseName);
        table.setTableName(tableName);
        table.setLastAccessTime(0);
        table.setRetention(365);
        table.setSd(buildStorageDescriptor(databaseName, tableName));
        List<FieldSchema> fieldSchemas = buildFieldSchema(partitionKeys);
        table.setPartitionKeys(fieldSchemas);
        table.setCreateTime(0);
        table.setOwner(OWNER);
        Map<String, String> parameters = new HashMap<String, String>();
        if ("EXTERNAL_TABLE".equalsIgnoreCase(tableType)) {
            parameters.put("EXTERNAL", "TRUE");
        }
        table.setParameters(parameters);
        table.setRewriteEnabled(true);
        table.setTemporary(false);
        return table;
    }

    public static Index buildIndex(String indexName, Table originTable) {
        Index index = new Index();
        index.setIndexName(indexName);
        index.setDbName(originTable.getDbName());
        index.setOrigTableName(originTable.getTableName());
        String indexTableName = indexName + "_table";
        index.setIndexTableName(indexTableName);
        index.setSd(buildStorageDescriptor(originTable.getDbName(), indexTableName));
        return index;
    }

    public static Table buildIndexTable(String indexTableName, Table originTable) {
        return buildTable(originTable.getDbName(), indexTableName, Lists.newArrayList("day"), INDEX_TABLE.name());
    }

    public static List<FieldSchema> buildFieldSchema(List<String> colNames) {
        List<FieldSchema> fieldSchemas = Lists.newArrayList();
        for (String colName : colNames) {
            fieldSchemas.add(new FieldSchema(colName, "int", "comment_day"));
        }

        return fieldSchemas;
    }

    public static void addSd(ArrayList<FieldSchema> cols, Table tbl) {
        StorageDescriptor storageDescriptor = buildStorageDescriptor(tbl.getDbName(), tbl.getTableName(), cols);
        tbl.setSd(storageDescriptor);
    }

    public static StorageDescriptor buildStorageDescriptor(String databaseName, String tableName, List<FieldSchema> columns) {
        StorageDescriptor sd = new StorageDescriptor();
        String location = WAREHOUSE_PATH + "/" + databaseName + ".db" + "/" + tableName;
        sd.setCols(columns);
        sd.setLocation(location);
        sd.setNumBuckets(5);
        sd.setCompressed(false);
        sd.setInputFormat("input");
        sd.setOutputFormat("output");
        sd.setBucketCols(ImmutableList.of("col1"));
        sd.setSortCols(ImmutableList.of(new Order("name", 1)));

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setName("name");
        serDeInfo.setParameters(new HashMap<>());
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");
        sd.setSerdeInfo(serDeInfo);

        sd.setParameters(ImmutableMap.of("hello", "world"));
        SkewedInfo skewedInfo = new SkewedInfo();
        skewedInfo.setSkewedColValueLocationMaps(ImmutableMap.of(ImmutableList.of("hello", "def"), "world"));
        skewedInfo.setSkewedColNames(Lists.newArrayList());
        skewedInfo.setSkewedColValues(Lists.newArrayList());

        sd.setSkewedInfo(skewedInfo);
        sd.setStoredAsSubDirectories(false);
        return sd;
    }

    public static StorageDescriptor buildStorageDescriptor(String databaseName, String tableName) {
        FieldSchema column = new FieldSchema("name", "string", "user_name");
        FieldSchema column1 = new FieldSchema("name1", "string", "user_name");
        List<FieldSchema> columns = new ArrayList<FieldSchema>();
        columns.add(column);
        columns.add(column1);
        return buildStorageDescriptor(databaseName, tableName, columns);
    }

    public static Function buildFunction(String dbName, String functionName) {
        Function function = new Function();
        function.setDbName(dbName);
        function.setFunctionName(functionName);
        function.setClassName("clazz");
        function.setOwnerName("Owner");
        function.setOwnerType(PrincipalType.USER);
        function.setFunctionType(FunctionType.JAVA);
        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, "uri");
        function.setResourceUris(ImmutableList.of(resourceUri));
        return function;
    }

    public static Partition buildPartition(String dbName, String tableName, Integer lastAccessTime,
                                           List<String> partValues) throws MetaException {
        Table tbl = TestUtil.buildTable(dbName, tableName);
        return buildPartition(dbName, tableName, tbl, lastAccessTime, partValues);
    }

    public static Partition buildPartition(String dbName, String tableName, Table table, Integer lastAccessTime,
                                           List<String> partValues) throws MetaException {
        Partition partition = new Partition();
        partition.setDbName(dbName);
        partition.setTableName(tableName);
        partition.setLastAccessTime(lastAccessTime);
        partition.setValues(partValues);
        partition.setSd(buildStorageDescriptor(dbName, tableName));
        String location = partition.getSd().getLocation();
        partition.getSd().setLocation(location + "/" + Warehouse.makePartName(table.getPartitionKeys(), partValues));

        Map<String, String> parameters = new HashMap<String, String>();
        partition.setParameters(parameters);
        return partition;
    }

    public static void addPartition(IMetaStoreClient client, Table table, List<String> vals, String location) throws TException {

        Partition part = new Partition();
        part.setDbName(table.getDbName());
        part.setTableName(table.getTableName());
        part.setValues(vals);
        part.setParameters(new HashMap<>());
        part.setSd(table.getSd().deepCopy());
        part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
        part.getSd().setLocation(table.getSd().getLocation() + location);

        client.add_partition(part);
    }

    public static void run(IMetaStoreClient client, ThrowingConsumer<IMetaStoreClient, TException> consumer,
                           Throwable throwable) {
        try {
            consumer.accept(client);
            if (throwable != null) {
                Assert.assertTrue(false);
            } else {
                Assert.assertTrue(true);
            }
        } catch (TException e) {
            Assert.assertTrue(throwable != null && throwable instanceof TException);
            Assert.assertTrue(e.getMessage().contains(throwable.getMessage()));
        }
    }

    /**
     * A Database builder.
     */
    public static class DatabaseBuilder {
        private Database database;

        private DatabaseBuilder() {
            database = new Database();
            // Set optional variables to default values.
            database.setDescription("Default Database Comment.");
            database.setParameters(new HashMap<>());
            database.setOwnerName(DEFAULT_OWNER_NAME);
            database.setOwnerType(DEFAULT_OWNER_TYPE);
            database.setPrivileges(new PrincipalPrivilegeSet());
        }

        public static DatabaseBuilder builder() {
            return new DatabaseBuilder();
        }

        public DatabaseBuilder withDbName(String dbName) {
            database.setName(dbName);
            return this;
        }

        public DatabaseBuilder withComment(String description) {
            database.setDescription(description);
            return this;
        }

        public DatabaseBuilder withLocationUri(String locationUri) {
            database.setLocationUri(locationUri);
            return this;
        }

        public DatabaseBuilder withOwner(String owner) {
            database.setOwnerName(owner);
            return this;
        }

        public DatabaseBuilder withOwnerType(PrincipalType ownerType) {
            database.setOwnerType(ownerType);
            return this;
        }

        public DatabaseBuilder withPrivilege(PrincipalPrivilegeSet privilege) {
            database.setPrivileges(privilege);
            return this;
        }

        public DatabaseBuilder withParameters(Map<String, String> parameters) {
            database.setParameters(parameters);
            return this;
        }

        public Database build() {
            return database;
        }
    }

    /**
     * A Table Builder.
     */
    public static class TableBuilder {
        private Table table;

        private TableBuilder() {
            table = new Table();
            // Set optional variables to default values.
            table.setOwner(DEFAULT_OWNER_NAME);
            // should set partitonKeys here, because null will produce unexpected result
            table.setPartitionKeys(new ArrayList<>());
            table.setParameters(new HashMap<>());
            table.setPrivileges(new PrincipalPrivilegeSet());
            table.setTableType(TableType.MANAGED_TABLE.name());
            table.setTemporary(false);
        }

        public static TableBuilder builder() {
            return new TableBuilder();
        }

        public TableBuilder withDbName(String dbName) {
            table.setDbName(dbName);
            return this;
        }

        public TableBuilder withTableName(String tblName) {
            table.setTableName(tblName);
            return this;
        }

        public TableBuilder withTableType(TableType tableType) {
            table.setTableType(tableType.name());
            // for external table, the property should be stored in table parameters.
            if (tableType == TableType.EXTERNAL_TABLE) {
                table.getParameters().put("EXTERNAL", "TRUE");
            }
            return this;
        }

        public TableBuilder withPartitionKeys(List<FieldSchema> partCols) {
            table.setPartitionKeys(partCols);
            return this;
        }

        public TableBuilder withStorageDescriptor(StorageDescriptor sd) {
            table.setSd(sd);
            return this;
        }

        public TableBuilder withCreateTime(int createTime) {
            table.setCreateTime(createTime);
            return this;
        }

        public TableBuilder withLastAccessTime(int lastAccessTime) {
            table.setLastAccessTime(lastAccessTime);
            return this;
        }

        public TableBuilder withOwner(String owner) {
            table.setOwner(owner);
            return this;
        }

        public TableBuilder withPrivilege(PrincipalPrivilegeSet privilege) {
            table.setPrivileges(privilege);
            return this;
        }

        public TableBuilder withParameters(Map<String, String> params) {
            table.setParameters(params);
            return this;
        }

        public TableBuilder withTemporary() {
            table.setTemporary(true);
            return this;
        }

        public Table build() {
            return table;
        }
    }

    /**
     * A StorageDescriptor Builder.
     */
    public static class StorageDescriptorBuilder {
        private StorageDescriptor sd;

        private StorageDescriptorBuilder() {
            sd = new StorageDescriptor();
            sd.setSerdeInfo(new SerDeInfo());
        }

        public static StorageDescriptorBuilder builder() {
            return new StorageDescriptorBuilder();
        }

        public StorageDescriptorBuilder withCols(List<FieldSchema> cols) {
            sd.setCols(cols);
            return this;
        }

        public StorageDescriptorBuilder withBucketCols(List<String> bucketCols) {
            sd.setBucketCols(bucketCols);
            return this;
        }

        public StorageDescriptorBuilder withNumBuckets(int numBuckets) {
            sd.setNumBuckets(numBuckets);
            return this;
        }

        public StorageDescriptorBuilder withSerDeInfo(SerDeInfo serDerInfo) {
            sd.setSerdeInfo(serDerInfo);
            return this;
        }

        public StorageDescriptorBuilder withInputFormat(String inputFormat) {
            sd.setInputFormat(inputFormat);
            return this;
        }

        public StorageDescriptorBuilder withOutputFormat(String outputFormat) {
            sd.setInputFormat(outputFormat);
            return this;
        }

        public StorageDescriptorBuilder withLocation(String location) {
            sd.setLocation(location);
            return this;
        }

        public StorageDescriptorBuilder withParameters(Map<String, String> params) {
            sd.setParameters(params);
            return this;
        }

        public StorageDescriptor build() {
            return sd;
        }
    }

    /**
     * A Partition Builder.
     */
    public static class PartitionBuilder {
        private Partition partition;

        private PartitionBuilder() {
            this.partition = new Partition();
            // Set optional variables to default values.
            partition.setParameters(new HashMap<>());
            partition.setPrivileges(new PrincipalPrivilegeSet());
        }

        public static PartitionBuilder builder() {
            return new PartitionBuilder();
        }

        public PartitionBuilder withDbName(String dbName) {
            partition.setDbName(dbName);
            return this;
        }

        public PartitionBuilder withTableName(String tableName) {
            partition.setTableName(tableName);
            return this;
        }

        public PartitionBuilder withCreateTime(int createTime) {
            partition.setCreateTime(createTime);
            return this;
        }

        public PartitionBuilder withLastAccessTime(int lastAccessTime) {
            partition.setLastAccessTime(lastAccessTime);
            return this;
        }

        public PartitionBuilder withParameters(Map<String, String> params) {
            partition.setParameters(params);
            return this;
        }

        public PartitionBuilder withPrivilege(PrincipalPrivilegeSet privilege) {
            partition.setPrivileges(privilege);
            return this;
        }

        public PartitionBuilder withStorageDescriptor(StorageDescriptor sd) {
            partition.setSd(sd);
            return this;
        }

        public PartitionBuilder withValues(List<String> values) {
            partition.setValues(values);
            return this;
        }

        public Partition build() {
            return partition;
        }
    }

    /**
     * A Function Builder.
     */
    public static class FunctionBuilder {
        private Function function;

        private FunctionBuilder() {
            function = new Function();
            // Set optional variables to default values.
            function.setFunctionType(FunctionType.JAVA);
            function.setOwnerName(DEFAULT_OWNER_NAME);
            function.setOwnerType(DEFAULT_OWNER_TYPE);
            function.setResourceUris(Lists.newArrayList());
        }

        public FunctionBuilder withDbName(String dbName) {
            function.setDbName(dbName);
            return this;
        }

        public FunctionBuilder withFuncName(String funcName) {
            function.setFunctionName(funcName);
            return this;
        }

        public FunctionBuilder withClassName(String className) {
            function.setClassName(className);
            return this;
        }

        public FunctionBuilder withOwner(String owner) {
            function.setOwnerName(owner);
            return this;
        }

        public FunctionBuilder withOwnerType(PrincipalType ownerType) {
            function.setOwnerType(ownerType);
            return this;
        }

        public FunctionBuilder builder() {
            return new FunctionBuilder();
        }

        public Function build() {
            return function;
        }
    }
}
