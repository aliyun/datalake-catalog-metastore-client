package com.aliyun.datalake.metastore.common;


import com.aliyun.datalake20200710.models.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class CacheDataLakeMetaStore implements IDataLakeMetaStore {
    private final Logger logger = LoggerFactory.getLogger(CacheDataLakeMetaStore.class);
    private final boolean databaseCacheEnabled;
    private final boolean tableCacheEnabled;
    @VisibleForTesting
    protected Cache<DbIdentifier, Database> databaseCache;
    @VisibleForTesting
    protected Cache<TableIdentifier, Table> tableCache;
    private CacheDataLakeMetaStoreConfig config;
    private IDataLakeMetaStore defaultDataLakeMetaStore;

    public CacheDataLakeMetaStore(CacheDataLakeMetaStoreConfig config, IDataLakeMetaStore defaultDataLakeMetaStore) {
        this.config = config;
        this.defaultDataLakeMetaStore = defaultDataLakeMetaStore;
        databaseCacheEnabled = config.isDataLakeDbCacheEnable();
        if (databaseCacheEnabled) {
            int dbCacheSize = config.getDataLakeDbCacheSize();
            int dbCacheTtlMins = config.getDataLakeDbCacheTTLMins();

            //validate config values for size and ttl
            validateConfigValueIsGreaterThanZero(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_SIZE, dbCacheSize);
            validateConfigValueIsGreaterThanZero(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_TTL_MINS, dbCacheTtlMins);

            //initialize database cache
            databaseCache = CacheBuilder.newBuilder().maximumSize(dbCacheSize)
                    .expireAfterWrite(dbCacheTtlMins, TimeUnit.MINUTES).build();
        } else {
            databaseCache = null;
        }

        tableCacheEnabled = config.isDataLakeTbCacheEnable();
        if (tableCacheEnabled) {
            int tableCacheSize = config.getDataLakeTbCacheSize();
            int tableCacheTtlMins = config.getDataLakeTbCacheTTLMins();

            //validate config values for size and ttl
            validateConfigValueIsGreaterThanZero(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_SIZE, tableCacheSize);
            validateConfigValueIsGreaterThanZero(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_TTL_MINS, tableCacheTtlMins);

            //initialize table cache
            tableCache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
                    .expireAfterWrite(tableCacheTtlMins, TimeUnit.MINUTES).build();
        } else {
            tableCache = null;
        }
    }

    private void validateConfigValueIsGreaterThanZero(String configName, int value) {
        checkArgument(value > 0, String.format("Invalid value for Hive Config %s. " +
                "Provide a value greater than zero", configName));

    }

    public Cache<DbIdentifier, Database> getDatabaseCache() {
        return databaseCache;
    }

    public Cache<TableIdentifier, Table> getTableCache() {
        return tableCache;
    }

    @Override
    public void createDatabase(String catalogId, Database database) throws Exception {
        defaultDataLakeMetaStore.createDatabase(catalogId, database);
    }

    @Override
    public void createDatabase(String catalogId, String dbName, String description, String location, Map<String, String> parameters, String ownerName, String ownerType, PrincipalPrivilegeSet principalPrivilegeSet) throws Exception {
        defaultDataLakeMetaStore.createDatabase(catalogId, dbName, description, location, parameters, ownerName, ownerType, principalPrivilegeSet);
    }

    @Override
    public Database getDatabase(String catalogId, String dbName) throws Exception {
        Database result;
        if (databaseCacheEnabled) {
            DbIdentifier key = new DbIdentifier(catalogId, dbName);
            Database valueFromCache = databaseCache.getIfPresent(key);
            if (valueFromCache != null) {
                logger.debug("Cache hit for operation [getDatabase] on key [" + key + "]");
                result = valueFromCache;
            } else {
                logger.debug("Cache miss for operation [getDatabase] on key [" + key + "]");
                result = defaultDataLakeMetaStore.getDatabase(catalogId, dbName);
                databaseCache.put(key, result);
            }
        } else {
            result = defaultDataLakeMetaStore.getDatabase(catalogId, dbName);
        }
        return result;
    }

    @Override
    public List<String> getDatabases(String catalogId, String pattern, int pageSize) throws Exception {
        return defaultDataLakeMetaStore.getDatabases(catalogId, pattern, pageSize);
    }

    @Override
    public void alterDatabase(String catalogId, String dbName, Database database) throws Exception {
        defaultDataLakeMetaStore.alterDatabase(catalogId, dbName, database);
        if (databaseCacheEnabled) {
            purgeDatabaseFromCache(catalogId, dbName);
        }
    }

    private void purgeDatabaseFromCache(String catalogId, String dbName) {
        databaseCache.invalidate(new DbIdentifier(catalogId, dbName));
    }

    @Override
    public void dropDatabase(String catalogId, String dbName, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws Exception {
        defaultDataLakeMetaStore.dropDatabase(catalogId, dbName, deleteData, ignoreUnknownDb, cascade);
        if (databaseCacheEnabled) {
            purgeDatabaseFromCache(catalogId, dbName);
            if (tableCacheEnabled) {
                purgeTableFromCache(catalogId, dbName);
            }
        }
    }

    @Override
    public void createTable(String catalogId, TableInput tbl) throws Exception {
        defaultDataLakeMetaStore.createTable(catalogId, tbl);
    }

    @Override
    public Table getTable(String catalogId, String dbName, String tableName) throws Exception {
        Table result;
        if (tableCacheEnabled) {
            TableIdentifier key = new TableIdentifier(catalogId, dbName, tableName);
            Table valueFromCache = tableCache.getIfPresent(key);
            if (valueFromCache != null) {
                //logger.info("Cache hit for operation [getTable] on key [" + key + "]");
                //System.out.println("Cache hit for operation [getTable] on key [" + key + "]");
                result = valueFromCache;
            } else {
                //logger.info("Cache miss for operation [getTable] on key [" + key + "]");
                //System.out.println("Cache miss for operation [getTable] on key [" + key + "]");
                result = defaultDataLakeMetaStore.getTable(catalogId, dbName, tableName);
                tableCache.put(key, result);
            }
        } else {
            result = defaultDataLakeMetaStore.getTable(catalogId, dbName, tableName);
        }
        return result;
    }

    @Override
    public List<String> getTables(String catalogId, String dbname, String tablePattern, int pageSize, String tableType) throws Exception {
        return defaultDataLakeMetaStore.getTables(catalogId, dbname, tablePattern, pageSize, tableType);
    }

    @Override
    public List<Table> getTableObjects(String catalogId, String dbname, String tablePattern, int pageSize, String tableType) throws Exception {
        return defaultDataLakeMetaStore.getTableObjects(catalogId, dbname, tablePattern, pageSize, tableType);
    }

    @Override
    public List<Table> getTableObjects(String catalogId, String dbname, List<String> tableNames) throws Exception {
        return defaultDataLakeMetaStore.getTableObjects(catalogId, dbname, tableNames);
    }

    @Override
    public void alterTable(String catalogId, String dbName, String oldTableName, TableInput newTable) throws Exception {
        alterTable(catalogId, dbName, oldTableName, newTable, false, false);
    }

    @Override
    public void alterTable(String catalogId, String dbName, String oldTableName, TableInput newTable, boolean cascade, boolean isAsync) throws Exception {
        defaultDataLakeMetaStore.alterTable(catalogId, dbName, oldTableName, newTable, cascade, isAsync);
        if (tableCacheEnabled) {
            purgeTableFromCache(catalogId, dbName, oldTableName);
        }
    }

    public void purgeTableFromCache(String catalogId, String dbName, String oldTableName) {
        tableCache.invalidate(new TableIdentifier(catalogId, dbName, oldTableName));
    }

    public void purgeTableFromCache(String catalogId, String dbName) {
        ConcurrentMap<TableIdentifier, Table> cacheMaps = tableCache.asMap();
        Iterator<Map.Entry<TableIdentifier, Table>> iterator = cacheMaps.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableIdentifier, Table> entry = iterator.next();
            TableIdentifier tableIdentifier = entry.getKey();
            if (tableIdentifier.getDbName().equalsIgnoreCase(dbName)) {
                tableCache.invalidate(new TableIdentifier(catalogId, dbName, entry.getValue().getTableName()));
            }
        }
    }

    @Override
    public void dropTable(String catalogId, String dbName, String tbName, boolean deleteData) throws Exception {
        defaultDataLakeMetaStore.dropTable(catalogId, dbName, tbName, deleteData);
        if (tableCacheEnabled) {
            purgeTableFromCache(catalogId, dbName, tbName);
        }
    }

    @Override
    public void doRenameTableInMs(String catalogId, String dbName, String oldTableName, TableInput newTable, Boolean isAsync) throws Exception {
        defaultDataLakeMetaStore.doRenameTableInMs(catalogId, dbName, oldTableName, newTable, isAsync);
        if (tableCacheEnabled) {
            purgeTableFromCache(catalogId, dbName, oldTableName);
        }
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> columnNames) throws Exception {
        return defaultDataLakeMetaStore.getTableColumnStatistics(catalogId, dbName, tableName, columnNames);
    }

    @Override
    public boolean updateTableColumnStatistics(UpdateTablePartitionColumnStatisticsRequest columnStatistics) throws Exception {
        return defaultDataLakeMetaStore.updateTableColumnStatistics(columnStatistics);
    }

    @Override
    public boolean deleteTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> colNames) throws Exception {
        return defaultDataLakeMetaStore.deleteTableColumnStatistics(catalogId, dbName, tableName, colNames);
    }

    @Override
    public Partition addPartition(String catalogId, String dbName, String tableName, PartitionInput partitions, boolean ifNotExist, boolean needResult) throws Exception {
        return defaultDataLakeMetaStore.addPartition(catalogId, dbName, tableName, partitions, ifNotExist, needResult);
    }

    @Override
    public List<Partition> addPartitions(String catalogId, String dbName, String tableName, List<PartitionInput> partitions, boolean ifNotExist, boolean needResult) throws Exception {
        return defaultDataLakeMetaStore.addPartitions(catalogId, dbName, tableName, partitions, ifNotExist, needResult);
    }

    @Override
    public void alterPartitions(String catalogId, String dbName, String tblName, List<PartitionInput> partitions) throws Exception {
        defaultDataLakeMetaStore.alterPartitions(catalogId, dbName, tblName, partitions);
    }

    @Override
    public <T> T listPartitionsByExpr(String catalogId, String dbName, String tblName, byte[] expr, String defaultPartName, int max, String filter, int pageSizeIn, IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter) throws Exception {
        return defaultDataLakeMetaStore.listPartitionsByExpr(catalogId, dbName, tblName, expr, defaultPartName, max, filter, pageSizeIn, resultConverter);
    }

    @Override
    public void doDropPartitions(String catalogId, String dbName, String tableName, List<List<String>> partValuesList, boolean ifExist) throws Exception {
        defaultDataLakeMetaStore.doDropPartitions(catalogId, dbName, tableName, partValuesList, ifExist);
    }

    @Override
    public Partition getPartition(String catalogId, String databaseName, String tableName, List<String> parValues) throws Exception {
        return defaultDataLakeMetaStore.getPartition(catalogId, databaseName, tableName, parValues);
    }

    @Override
    public List<Partition> getPartitionsByValues(String catalogId, String databaseName, String tableName, List<List<String>> partValuesList) throws Exception {
        return defaultDataLakeMetaStore.getPartitionsByValues(catalogId, databaseName, tableName, partValuesList);
    }

    @Override
    public void renamePartitionInCatalog(String catalogId, String dbName, String tbName, List<String> partitionValues, PartitionInput newPartition) throws Exception {
        defaultDataLakeMetaStore.renamePartitionInCatalog(catalogId, dbName, tbName, partitionValues, newPartition);
    }

    @Override
    public List<String> listPartitionNames(String catalogId, String dbName, String tblName, List<String> partialPartValues, int max, int pageSizeIn) throws Exception {
        return defaultDataLakeMetaStore.listPartitionNames(catalogId, dbName, tblName, partialPartValues, max, pageSizeIn);
    }

    @Override
    public int getNumPartitionsByFilter(String catalogId, String dbName, String tblName, String filter, int pageSizeIn) throws Exception {
        return defaultDataLakeMetaStore.getNumPartitionsByFilter(catalogId, dbName, tblName, filter, pageSizeIn);
    }

    @Override
    public <T> T listPartitionsInternal(String catalogId, String databaseName, String tableName, List<String> values, String filter, int max, int pageSizeIn, IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter) throws Exception {
        return defaultDataLakeMetaStore.listPartitionsInternal(catalogId, databaseName, tableName, values, filter, max, pageSizeIn, resultConverter);
    }

    @Override
    public <T> T listPartitions(String catalogId, String databaseName, String tableName, int max, int pageSizeIn, PartitionVisitor<T, Partition> resultConverter) throws Exception {
        return defaultDataLakeMetaStore.listPartitions(catalogId, databaseName, tableName, max, pageSizeIn, resultConverter);
    }

    @Override
    public List<Partition> listPartitionsByFilter(String catalogId, String databaseName, String tableName, String filter, int pageSizeIn) throws Exception {
        return defaultDataLakeMetaStore.listPartitionsByFilter(catalogId, databaseName, tableName, filter, pageSizeIn);
    }

    @Override
    public void doDropPartition(String catalogId, String dbName, String tableName, List<String> partValuesList, boolean ifExist) throws Exception {
        defaultDataLakeMetaStore.doDropPartition(catalogId, dbName, tableName, partValuesList, ifExist);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws Exception {
        return defaultDataLakeMetaStore.getPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> batchGetPartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws Exception {
        return defaultDataLakeMetaStore.batchGetPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public boolean updatePartitionColumnStatistics(UpdateTablePartitionColumnStatisticsRequest columnStatistics) throws Exception {
        return defaultDataLakeMetaStore.updatePartitionColumnStatistics(columnStatistics);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partNames, List<String> colNames) throws Exception {
        return defaultDataLakeMetaStore.deletePartitionColumnStatistics(catalogId, dbName, tableName, partNames, colNames);
    }

    @Override
    public void createFunction(String catalogId, FunctionInput function, String dbName) throws Exception {
        defaultDataLakeMetaStore.createFunction(catalogId, function, dbName);
    }

    @Override
    public Function getFunction(String catalogId, String dbName, String functionName) throws Exception {
        return defaultDataLakeMetaStore.getFunction(catalogId, dbName, functionName);
    }

    @Override
    public List<String> getFunctions(String catalogId, String dbName, String pattern, int pageSize) throws Exception {
        return defaultDataLakeMetaStore.getFunctions(catalogId, dbName, pattern, pageSize);
    }

    @Override
    public List<Function> getFunctionObjects(String catalogId, String dbName, String pattern, int pageSize) throws Exception {
        return defaultDataLakeMetaStore.getFunctionObjects(catalogId, dbName, pattern, pageSize);
    }

    @Override
    public void alterFunction(String catalogId, String dbName, String functionName, FunctionInput function) throws Exception {
        defaultDataLakeMetaStore.alterFunction(catalogId, dbName, functionName, function);
    }

    @Override
    public void dropFunction(String catalogId, String dbName, String functionName) throws Exception {
        defaultDataLakeMetaStore.dropFunction(catalogId, dbName, functionName);
    }


    @Override
    public List<Partition> getNonSubDirectoryPartitionLocations(String catalogId, String dbName, String tableName, int pageSize) throws Exception {
        return defaultDataLakeMetaStore.getNonSubDirectoryPartitionLocations(catalogId, dbName, tableName, pageSize);
    }

    @Override
    public LockStatus lock(List<LockObj> lockObjList)
            throws Exception {
        return defaultDataLakeMetaStore.lock(lockObjList);
    }

    @Override
    public Boolean unLock(Long lockId)
            throws Exception {
        return defaultDataLakeMetaStore.unLock(lockId);
    }

    @Override
    public LockStatus getLock(Long lockId)
            throws Exception {
        return defaultDataLakeMetaStore.getLock(lockId);
    }

    @Override
    public Boolean refreshLock(Long lockId)
            throws Exception {
        return defaultDataLakeMetaStore.refreshLock(lockId);
    }

    public static class TableIdentifier {
        private final String catalogId;
        private final String dbName;
        private final String tableName;

        public TableIdentifier(String catalogId, String dbName, String tableName) {
            this.catalogId = catalogId.toLowerCase();
            this.dbName = dbName.toLowerCase();
            this.tableName = tableName.toLowerCase();
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getCatalogId() {
            return catalogId;
        }

        @Override
        public String toString() {
            return "TableIdentifier{" + "catalogId='" + catalogId + '\'' +
                    ", dbName='" + dbName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableIdentifier that = (TableIdentifier) o;
            return Objects.equals(catalogId, that.catalogId) &&
                    Objects.equals(dbName, that.dbName) &&
                    Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, dbName, tableName);
        }
    }

    public static class DbIdentifier {
        private final String catalogId;
        private final String dbName;

        public DbIdentifier(String catalogId, String dbName) {
            this.catalogId = catalogId;
            this.dbName = dbName;
        }

        public String getDbName() {
            return dbName;
        }

        @Override
        public String toString() {
            return "DbIdentifier{" + "catalogId='" + catalogId + '\'' +
                    ", dbName='" + dbName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DbIdentifier that = (DbIdentifier) o;
            return Objects.equals(catalogId, that.catalogId) &&
                    Objects.equals(dbName, that.dbName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogId, dbName);
        }
    }
}
