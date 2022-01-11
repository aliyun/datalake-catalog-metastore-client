package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.DefaultExecutorServiceFactory;
import com.aliyun.datalake.metastore.common.ExecutorServiceFactory;
import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.ProxyMode;
import com.aliyun.datalake.metastore.common.api.DataLakeAPIException;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.hive.common.CommonMetaStoreClientDelegate;
import com.aliyun.datalake.metastore.hive.common.MetastoreFactory;
import com.aliyun.datalake.metastore.hive.common.converters.CatalogToHiveConverter;
import com.aliyun.datalake.metastore.hive.common.converters.HiveToCatalogConverter;
import com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.aliyun.datalake.metastore.hive.common.utils.HiveMetaHookWrapper;
import com.aliyun.datalake.metastore.hive.common.utils.Utils;
import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.aliyun.datalake.metastore.hive.shims.ShimsLoader;
import com.aliyun.tea.TeaException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.common.util.BloomFilter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.aliyun.datalake.metastore.common.Constant.EXECUTOR_FACTORY_CONF;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

public class DlfMetaStoreClientDelegate {
    private static final String PUBLIC = "public";
    private static final String EMPTY_TOKEN = "";
    private static final List<Role> implicitRoles = Lists.newArrayList(new Role(PUBLIC, 0, PUBLIC));
    private static final Logger logger = LoggerFactory.getLogger(DlfMetaStoreClientDelegate.class);
    private final Configuration conf;
    private final Warehouse warehouse;
    private final ExecutorService executorService;
    private final PartitionExpressionProxy expressionProxy;
    private final Pattern partitionValidationPattern;
    private final int pageSize;
    private IDataLakeMetaStore dataLakeMetaStore;
    private final IHiveShims hiveShims = ShimsLoader.getHiveShims();
    private final boolean isAggregateStatsCacheEnabled;
    // Thread local configuration is needed as many threads could make changes
    // to the conf using the connection hook
    private final CommonMetaStoreClientDelegate commonMetaStoreClientDelegate;
    private final Boolean enableFsOperation;
    private final boolean isProxyModeDlfOnly;
    private final boolean enableRenameFileOperation;
    //private DataLakeClient dataLakeClient;
    private HiveMetaHookLoader hookLoader;
    private AggregateStatsCache aggrStatsCache;

    public DlfMetaStoreClientDelegate(Configuration conf,
                                      Warehouse warehouse,
                                      HiveMetaHookLoader hookLoader) throws MetaException {
        this.conf = conf;
        this.warehouse = warehouse;
        this.hookLoader = hookLoader;
        this.pageSize = ConfigUtils.getPageSize(this.conf);
        this.executorService = getExecutorService();
        this.dataLakeMetaStore = new MetastoreFactory().getMetaStore(this.conf, executorService);
        this.commonMetaStoreClientDelegate = new CommonMetaStoreClientDelegate(dataLakeMetaStore, hiveShims, this.conf);
        //reInitDataLakeClient(conf);
        String partitionValidationRegex =
                MetastoreConf.getVar(this.conf, MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
        if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
            partitionValidationPattern = Pattern.compile(partitionValidationRegex);
        } else {
            partitionValidationPattern = null;
        }
        isAggregateStatsCacheEnabled = MetastoreConf.getBoolVar(
                this.conf, MetastoreConf.ConfVars.AGGREGATE_STATS_CACHE_ENABLED);
        if (isAggregateStatsCacheEnabled) {
            aggrStatsCache = AggregateStatsCache.getInstance(this.conf);
        }
        String className = MetastoreConf.getVar(this.conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS);
        try {
            @SuppressWarnings("unchecked")
            Class<? extends PartitionExpressionProxy> clazz =
                    JavaUtils.getClass(className, PartitionExpressionProxy.class);
            expressionProxy = JavaUtils.newInstance(clazz, new Class<?>[0], new Object[0]);
        } catch (MetaException e) {
            logger.error("Error loading PartitionExpressionProxy", e);
            throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage(), e);
        }
        this.isProxyModeDlfOnly = ConfigUtils.getProxyMode(conf) == ProxyMode.DLF_ONLY;
        this.enableRenameFileOperation =  isProxyModeDlfOnly || ConfigUtils.getProxyMode(conf) == ProxyMode.DLF_METASTORE_FAILURE;
        this.enableFsOperation = this.isProxyModeDlfOnly || (ConfigUtils.getEnableFileOperation(conf) && DataLakeUtil.isEnableFileOperationGray(ConfigUtils.getEnableFileOperationGrayRate(conf)));
    }

    private static void throwExceptionIfIncompatibleColTypeChange(List<FieldSchema> oldCols, List<FieldSchema> newCols)
            throws InvalidOperationException {

        List<String> incompatibleCols = new ArrayList<String>();
        int maxCols = Math.min(oldCols.size(), newCols.size());
        for (int i = 0; i < maxCols; i++) {
            if (!areColTypesCompatible(oldCols.get(i).getType(), newCols.get(i).getType())) {
                incompatibleCols.add(newCols.get(i).getName());
            }
        }
        if (!incompatibleCols.isEmpty()) {
            throw new InvalidOperationException(
                    "The following columns have types incompatible with the existing " +
                            "columns in their respective positions :\n" +
                            org.apache.commons.lang.StringUtils.join(incompatibleCols, ',')
            );
        }
    }

    private static boolean areColTypesCompatible(String oldType, String newType) {
        return TypeInfoUtils.implicitConvertible(TypeInfoUtils.getTypeInfoFromTypeString(oldType),
                TypeInfoUtils.getTypeInfoFromTypeString(newType));
    }

    /**
     * Data needs deletion. Check if trash may be skipped.
     * Trash may be skipped iff:
     * 1. deleteData == true, obviously.
     * 2. tbl is external.
     * 3. Either
     * 3.1. User has specified PURGE from the commandline, and if not,
     * 3.2. User has set the table to auto-purge.
     */
    private static boolean isMustPurge(EnvironmentContext envContext, Table table) {
        return ((envContext != null) && Boolean.parseBoolean(envContext.getProperties().get("ifPurge")))
                || (table.isSetParameters() && "true".equalsIgnoreCase(table.getParameters().get("auto.purge")));
    }

    private static boolean isMustPurge(boolean purgeData, Table tbl) {
        // Data needs deletion. Check if trash may be skipped.
        // Trash may be skipped iff:
        //  1. deleteData == true, obviously.
        //  2. tbl is external.
        //  3. Either
        //    3.1. User has specified PURGE from the commandline, and if not,
        //    3.2. User has set the table to auto-purge.
        boolean isAutoPurgeSet = tbl.isSetParameters() &&
                "true".equalsIgnoreCase(tbl.getParameters().get("auto.purge"));
        return purgeData || isAutoPurgeSet;

    }

    private static String convertToFilter(byte[] expr, PartitionExpressionProxy expressionProxy) throws MetaException {
        return expressionProxy.convertExprToFilter(expr);
    }

    private static boolean is_partition_spec_grouping_enabled(Table table) {

        Map<String, String> parameters = table.getParameters();
        return parameters.containsKey("hive.hcatalog.partition.spec.grouping.enabled")
                && parameters.get("hive.hcatalog.partition.spec.grouping.enabled").equalsIgnoreCase("true");
    }

    // ==================== Catalog ========================================

    //    public Catalog getCatalog(String catalogId) throws TException {
    //        try {
    //            return metastore.catalog().getCatalog(catalogId);
    //        } catch (ClientException e) {
    //            throw CatalogToHiveConverter.toHiveException(e);
    //        }
    //    }
    //
    //    public void alterCatalog(String catalogId, Catalog catalog) throws TException {
    //        try {
    //            metastore.catalog().updateCatalog(catalogId, catalog);
    //        } catch (ClientException e) {
    //            throw CatalogToHiveConverter.toHiveException(e);
    //        }
    //    }
    //

    // ==================== Database ========================================

    public Boolean getEnableFsOperation() {
        return enableFsOperation;
    }

    public IDataLakeMetaStore getDataLakeMetaStore() {
        return dataLakeMetaStore;
    }

    @VisibleForTesting
    protected void setDataLakeMetaStore(IDataLakeMetaStore dataLakeMetaStore) {
        this.dataLakeMetaStore = dataLakeMetaStore;
    }

    public Configuration getConf() {
        return conf;
    }

    public boolean makeDirs(Warehouse wh, Path path) throws MetaException {
        checkNotNull(wh, "Warehouse cannot be null");
        checkNotNull(path, "Path cannot be null");

        boolean madeDir = false;
        if (!wh.isDir(path)) {
            if (!hiveShims.mkdirs(wh, path, true, enableFsOperation)) {
                throw new MetaException("Unable to create path: " + path);
            }
            madeDir = true;
        }
        return madeDir;
    }

    public void createDatabase(String catalogId, Database database) throws TException {
        checkNotNull(database, "database cannot be null");

        if (StringUtils.isEmpty(database.getLocationUri())) {
            database.setLocationUri(warehouse.getDefaultDatabasePath(database.getName()).toString());
        } else {
            database.setLocationUri(warehouse.getDnsPath(new Path(database.getLocationUri())).toString());
        }
        Path dbPath = new Path(database.getLocationUri());
        boolean madeDir = makeDirs(warehouse, dbPath);

        boolean isSuccess = false;
        try {
            this.dataLakeMetaStore.createDatabase(catalogId, database.getName(),
                    database.getDescription(), database.getLocationUri(), database.getParameters(),
                    database.getOwnerName(), HiveToCatalogConverter.toCatalogPrincipalTypeString(database.getOwnerType()),
                    HiveToCatalogConverter.toCatalogPrivilegeSet(database.getPrivileges()));
            isSuccess = true;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to create database: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        } finally {
            if (!isSuccess && madeDir) {
                hiveShims.deleteDir(warehouse, dbPath, true, database, enableFsOperation);
            }
        }
    }

    public Database getDatabase(String catalogId, String dbName) throws TException {
        try {
            com.aliyun.datalake20200710.models.Database database = dataLakeMetaStore.getDatabase(catalogId, dbName);
            Database hiveDatabase = CatalogToHiveConverter.toHiveDatabase(database);
            hiveShims.addCatalogForDb(hiveDatabase, catalogId);
            return hiveDatabase;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get database: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    // ==================== Table ========================================

    public List<String> getDatabases(String catalogId, String pattern) throws TException {
        checkNotNull(pattern, "pattern cannot be null");
        try {
            return this.dataLakeMetaStore.getDatabases(catalogId, pattern, this.pageSize);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get databases: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void alterDatabase(String catalogId, String dbName, Database database) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkNotNull(database, "database cannot be null");
        try {
            this.dataLakeMetaStore.alterDatabase(catalogId, dbName, HiveToCatalogConverter.toCatalogDatabase(database));
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to alter database: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void dropDatabase(String catalogId,
                             String dbName,
                             boolean deleteData,
                             boolean ignoreUnknownDb,
                             boolean cascade) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "name cannot be null or empty");

        String dbLocation;
        try {
            List<String> tables = getTables(catalogId, dbName, ".*", null);
            boolean isEmptyDatabase = tables.isEmpty();

            Database db = getDatabase(catalogId, dbName);
            dbLocation = db.getLocationUri();
            Path path = new Path(db.getLocationUri()).getParent();
            if (!warehouse.isWritable(path)) {
                throw new MetaException("Database not dropped since " +
                        path + " is not writable by " + SecurityUtils.getUser());
            }

            if (isEmptyDatabase || cascade) {
                try {

                    if (deleteData) {
                        List<String> materializedViews = getTables(catalogId, dbName, ".*", TableType.MATERIALIZED_VIEW);
                        for (String table : materializedViews) {
                            // First we delete the materialized views to avoid that tabled is deleted but materialized views still exists
                            dropTable(catalogId, dbName, table, deleteData, true, null);
                            tables.remove(table);
                        }

                        for (String tableName : tables) {
                            try {
                                dropTable(catalogId, dbName, tableName, true, true, null);
                            } catch (UnsupportedOperationException e) {
                                // Ignore Index tables, those will be dropped with parent tables
                            }
                        }
                    }
                    this.dataLakeMetaStore.dropDatabase(catalogId, dbName, deleteData, ignoreUnknownDb, cascade);
                } catch (DataLakeAPIException e) {
                    throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
                }
            } else {
                throw new InvalidOperationException("Database " + dbName + " is not empty.");
            }
            if (deleteData) {
                try {
                    hiveShims.deleteDir(warehouse, new Path(dbLocation), true, db, enableFsOperation);
                } catch (Exception e) {
                    logger.error("Unable to remove database directory " + dbLocation, e);
                }
            }
        } catch (NoSuchObjectException e) {
            if (ignoreUnknownDb) {
                return;
            } else {
                throw e;
            }
        } catch (InvalidOperationException e) {
            throw e;
        } catch (Exception e) {
            String msg = "Unable to drop database: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }

    }

    public void createTable(String catalogId, Table tbl, EnvironmentContext envContext) throws TException {
        HiveMetaHook hook = new HiveMetaHookWrapper(hookLoader, tbl);
        hook.preCreateTable(tbl);
        boolean dirCreated = validateNewTableAndCreateDirectory(catalogId, tbl);
        Database db = getDatabase(catalogId, tbl.getDbName());
        boolean isSuccess = false;
        try {
            if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_AUTO_GATHER) && !isView(tbl)) {
                hiveShims.updateTableStatsFast(db, tbl, warehouse, dirCreated, false, envContext);
            }
            this.dataLakeMetaStore.createTable(catalogId, HiveToCatalogConverter.toCatalogTableInput(tbl));
            hook.commitCreateTable(tbl);
            isSuccess = true;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to create table: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        } finally {
            if (!isSuccess && dirCreated) {
                Path tblPath = getTablePath(catalogId, tbl);
                hiveShims.deleteDir(this.warehouse, tblPath, true, db, enableFsOperation);
            }
            if (!isSuccess) {
                try {
                    hook.rollbackCreateTable(tbl);
                } catch (Exception e) {
                    logger.error("Create rollback failed with", e);
                }
            }
        }
    }

    public Table getTable(String catalogId, String dbName, String tableName) throws NoSuchObjectException, TException {
        try {
            com.aliyun.datalake20200710.models.Table result = this.dataLakeMetaStore.getTable(catalogId, dbName, tableName);
            return CatalogToHiveConverter.toHiveTable(result);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get table: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<ColumnStatisticsObj> getTableColumnStatisticsObjs(String catalogId, String dbName, String tableName, List<String> colNames)
            throws UnsupportedOperationException, TException {
        return commonMetaStoreClientDelegate.getTableColumnStatisticsObjs(catalogId, dbName, tableName, colNames);
    }

    public ColumnStatistics getTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> colNames)
            throws UnsupportedOperationException, TException {
        return commonMetaStoreClientDelegate.getTableColumnStatistics(catalogId, dbName, tableName, colNames);
    }

    public List<String> getTables(String catalogId, String dbname, String tablePattern, TableType tableType)
            throws TException {
        try {
            try {
                List<String> tables = new ArrayList<>();
                if (tableType != null) {
                    tables = this.dataLakeMetaStore.getTables(catalogId, dbname, tablePattern, this.pageSize, tableType.name());
                } else {
                    tables = this.dataLakeMetaStore.getTables(catalogId, dbname, tablePattern, this.pageSize, null);
                }
                return tables;
            } catch (DataLakeAPIException e) {
                throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
            }
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            String msg = "Unable to get tables: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<Table> getTableObjects(String catalogId, String dbname, String tablePattern, TableType tableType)
            throws TException {
        try {
            try {
                List<Table> tables = new ArrayList<>();
                if (tableType != null) {
                    tables = this.dataLakeMetaStore.getTableObjects(catalogId, dbname, tablePattern, this.pageSize, tableType.name()).stream().map(t -> CatalogToHiveConverter.toHiveTable(t)).collect(Collectors.toList());
                } else {
                    tables = this.dataLakeMetaStore.getTableObjects(catalogId, dbname, tablePattern, this.pageSize, null).stream().map(t -> CatalogToHiveConverter.toHiveTable(t)).collect(Collectors.toList());
                }
                return tables;
            } catch (DataLakeAPIException e) {
                throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
            }
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            String msg = "Unable to get tables: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<Table> getTableObjects(String catalogId, String dbname, List<String> tableNames)
            throws TException {
        try {
            try {
                List<Table> tables = this.dataLakeMetaStore.getTableObjects(catalogId, dbname, tableNames).stream().map(t -> CatalogToHiveConverter.toHiveTable(t)).collect(Collectors.toList());
                return tables;
            } catch (DataLakeAPIException e) {
                throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
            }
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            String msg = "Unable to get tables: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<TableMeta> getTableMeta(String catalogId, String dbPatterns, String tablePatterns, List<String> tableTypes) throws TException {
        List<TableMeta> tableMetaList = new ArrayList<>();
        if (dbPatterns == null) {
            dbPatterns = "*";
        }
        if (tablePatterns == null) {
            tablePatterns = "*";
        }
        String dbPatternsLowercase = dbPatterns.trim().toLowerCase();
        String tablePatternsLowercase = tablePatterns.trim().toLowerCase();
        List<String> databases = getDatabases(catalogId, dbPatternsLowercase);

        List<Table> tbs = new ArrayList<>();
        for (String db : databases) {
            if (tableTypes != null && tableTypes.size() > 0) {
                for (String tableType : tableTypes) {
                    try {
                        TableType realTableType = TableType.valueOf(tableType);
                        tbs.addAll(getTableObjects(catalogId, db, tablePatternsLowercase, realTableType));
                    } catch (IllegalArgumentException e) {
                        // not illegal tabletype
                    }
                }
            } else {
                tbs.addAll(getTableObjects(catalogId, db, tablePatternsLowercase, null));
            }
        }
        for (Table table : tbs) {
            TableMeta tableMeta = new TableMeta(table.getDbName(), table.getTableName(), table.getTableType());
            tableMeta.setComments(table.getParameters().get("comment"));
            tableMetaList.add(tableMeta);
        }
        return tableMetaList;
    }

    public void alterTable(String catalogId,
                           String dbName,
                           String oldTableName,
                           Table newTable,
                           EnvironmentContext environmentContext) throws TException {
        validateTableObject(newTable, getConf());
        updateTableType(newTable);

        Table oldTable;
        try {
            oldTable = getTable(catalogId, dbName, oldTableName);
        } catch (NoSuchObjectException e) {
            throw DataLakeUtil.throwException(new InvalidOperationException(String.format("table %s.%s doesn't exist", dbName, oldTableName)), e);
        }

        populateMissingSd(newTable);
        Database database = getDatabase(catalogId, dbName);
        if (isSupportedRenameAction(dbName, oldTableName, newTable)) {
            doRename(catalogId, dbName, oldTable, newTable, database);
            return;
        }
        validateTableChangeCompatibility(newTable, oldTable);

        if (hiveShims.requireCalStats(getConf(), null, null, newTable, environmentContext) && newTable.getPartitionKeys().isEmpty()) {
            //update table stats for non-partition Table
            hiveShims.updateTableStatsFast(database, newTable, this.warehouse, false, true, environmentContext);
        }
        try {
            final boolean cascade = environmentContext != null
                    && environmentContext.isSetProperties()
                    && StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(
                    StatsSetupConst.CASCADE));
            Boolean isAsync = cascade && (oldTable.getPartitionKeys() != null && oldTable.getPartitionKeys().size() > 0);
            this.dataLakeMetaStore.alterTable(catalogId, dbName, oldTableName, HiveToCatalogConverter.toCatalogTableInput(newTable), cascade, isAsync);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to alter table: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private void validateTableChangeCompatibility(Table newTable, Table oldTable) throws InvalidOperationException {
        checkNonViewColumnCompatibility(newTable, oldTable);
        validatePartitionKeyChange(newTable, oldTable);
    }

    /**
     * if a table was created in a user specified location using the DDL like
     * create table tbl ... location ...., it should be treated like an external table
     * in the table rename, its data location should not be changed. We can check
     * if the table directory was created directly under its database directory to tell
     * if it is such a table
     */
    private void doRename(String catalogId, String dbName, Table oldTable, Table newTable, Database database)
            throws TException {
        if (isView(oldTable) || isNewLocationSpecified(oldTable, newTable) || Utils.isExternalTable(oldTable)) {
            doRenameTableInMs(catalogId, dbName, oldTable, newTable);
            return;
        }

        Path srcPath = new Path(oldTable.getSd().getLocation());
        String oldTableRelativePath =
                (new Path(database.getLocationUri()).toUri()).relativize(srcPath.toUri()).toString();
        String oldTableName = oldTable.getTableName();
        boolean tableInSpecifiedLoc = !oldTableRelativePath.equalsIgnoreCase(oldTableName)
                && !oldTableRelativePath.equalsIgnoreCase(oldTableName + Path.SEPARATOR);

        if (tableInSpecifiedLoc) {
            doRenameTableInMs(catalogId, dbName, oldTable, newTable);
            return;
        }

        Boolean isSameDb = dbName.equalsIgnoreCase(newTable.getDbName());
        FileSystem srcFs = warehouse.getFs(srcPath);
        Database db = database;
        if (!isSameDb) {
            db = getDatabase(catalogId, newTable.getDbName());
        }

        // get new location
        if (!FileUtils.equalsFileSystem(srcFs, warehouse.getFs(warehouse.getDatabasePath(db)))) {
            throw new InvalidOperationException("table new location is" +
                    " on a different file system than the old location "
                    + srcPath + ". This operation is not supported");
        }

        Path databasePath = Utils.constructRenamedPath(warehouse.getDatabasePath(db), srcPath);
        Path destPath = new Path(databasePath, newTable.getTableName().toLowerCase());
        FileSystem destFs = warehouse.getFs(destPath);

        if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException("table new location " + destPath
                    + " is on a different file system than the old location "
                    + srcPath + ". This operation is not supported");
        }
        boolean dataWasMoved = false;
        try {
            if (enableRenameFileOperation && destFs.exists(destPath)) {
                throw new InvalidOperationException("New location for this table "
                        + newTable.getDbName() + "." + newTable.getTableName()
                        + " already exists : " + destPath);
            }
            // check that src exists and also checks permissions necessary, rename src to dest
            if (srcFs.exists(srcPath) && Utils.renameFs(srcFs, srcPath, destPath, enableRenameFileOperation)) {
                dataWasMoved = true;
            }
        } catch (IOException e) {
            throw DataLakeUtil.throwException(new InvalidOperationException(String.format("Alter Table operation for %s.%s failed to move data due to: '%s' See hive log file for details.", dbName, oldTableName, e.getMessage())), e);
        }

        try {
            doRenameTableInMs(catalogId, dbName, oldTable, newTable);
        } catch (Exception e) {
            if (dataWasMoved) {
                try {
                    if (destFs.exists(destPath)) {
                        if (!Utils.renameFs(destFs, destPath, srcPath, enableRenameFileOperation)) {
                            logger.error("Failed to restore data from {} to {} in alter table failure. Manual restore is needed.", destPath, srcPath);
                        }
                    }
                } catch (IOException e1) {
                    logger.error("Failed to restore data from {} to {} in alter table failure. Manual restore is needed.", destPath, srcPath, e1);
                }
            }
            String msg = String.format("Unable to rename table from %s to %s due to: ",
                    oldTableName, newTable.getTableName());
            throw new MetaException(msg + e.getMessage());
        }
    }

    private boolean isNewLocationSpecified(Table oldTable, Table newTable) {
        return StringUtils.isNotEmpty(newTable.getSd().getLocation()) &&
                !newTable.getSd().getLocation().equals(oldTable.getSd().getLocation());
    }

    private void doRenameTableInMs(String catalogId, String dbName, Table oldTable, Table newTable)
            throws TException {
        try {
            Boolean isAsync = false;
            if (!dbName.equalsIgnoreCase(newTable.getDbName()) || (oldTable.getPartitionKeys() != null && oldTable.getPartitionKeys().size() > 0)) {
                isAsync = true;
            }
            this.dataLakeMetaStore.doRenameTableInMs(catalogId, dbName, oldTable.getTableName(), HiveToCatalogConverter.toCatalogTableInput(newTable), isAsync);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to do rename table InMs:";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private void updateTableType(Table newTable) {
        // If table properties has EXTERNAL set, update table type accordingly
        // mimics Hive's ObjectStore#convertToMTable, added in HIVE-1329
        boolean isExternal = Utils.isExternalTableSetInParameters(newTable);
        if (MANAGED_TABLE.toString().equals(newTable.getTableType()) && isExternal) {
            newTable.setTableType(EXTERNAL_TABLE.toString());
        }
    }

    private void validatePartitionKeyChange(Table newTable, Table oldTable) throws InvalidOperationException {
        boolean partKeysPartiallyEqual =
                checkPartialPartKeysEqual(oldTable.getPartitionKeys(), newTable.getPartitionKeys());
        if (!isView(oldTable)) {
            if (oldTable.getPartitionKeys().size() != newTable.getPartitionKeys().size() || !partKeysPartiallyEqual) {
                throw new InvalidOperationException("partition keys can not be changed.");
            }
        }
    }

    private boolean isView(Table table) {
        return TableType.VIRTUAL_VIEW.toString().equals(table.getTableType());
    }

    private boolean checkPartialPartKeysEqual(List<FieldSchema> oldPartKeys, List<FieldSchema> newPartKeys) {
        //return true if both are null, or false if one is null and the other isn't
        if (newPartKeys == null || oldPartKeys == null) {
            return oldPartKeys == newPartKeys;
        }
        if (oldPartKeys.size() != newPartKeys.size()) {
            return false;
        }
        Iterator<FieldSchema> oldPartKeysIter = oldPartKeys.iterator();
        Iterator<FieldSchema> newPartKeysIter = newPartKeys.iterator();
        FieldSchema oldFs;
        FieldSchema newFs;
        while (oldPartKeysIter.hasNext()) {
            oldFs = oldPartKeysIter.next();
            newFs = newPartKeysIter.next();
            // Alter table can change the type of partition key now.
            // So check the column name only.
            if (!oldFs.getName().equals(newFs.getName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Views derive the column type from the base table definition.  So the view definition
     * can be altered to change the column types.  The column type compatibility checks should
     * be done only for non-views.
     */
    private void checkNonViewColumnCompatibility(Table newTable, Table oldTable) throws InvalidOperationException {
        if (MetastoreConf.getBoolVar(getConf(), MetastoreConf.ConfVars.DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES)
                && !isView(oldTable)) {
            throwExceptionIfIncompatibleColTypeChange(oldTable.getSd().getCols(), newTable.getSd().getCols());
        }
    }

    private boolean isSupportedRenameAction(String dbName, String oldTableName, Table newTable) {
        return !oldTableName.equalsIgnoreCase(newTable.getTableName()) || !dbName.equalsIgnoreCase(newTable.getDbName());
    }

    private void populateMissingSd(Table newTable) throws MetaException {
        if (newTable.getSd() != null) {
            String newLocation = newTable.getSd().getLocation();
            if (org.apache.commons.lang.StringUtils.isNotEmpty(newLocation)) {
                Path tblPath = warehouse.getDnsPath(new Path(newLocation));
                newTable.getSd().setLocation(tblPath.toString());
            }
        }
    }

    private boolean isCascade(EnvironmentContext environmentContext) {
        return environmentContext != null && environmentContext.isSetProperties() &&
                StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(StatsSetupConst.CASCADE));
    }

    public boolean deletePartitionColumnStatistics(String catalogId, String dbName, String tableName, String partName, String colName) throws TException {
        return commonMetaStoreClientDelegate.deletePartitionColumnStatistics(catalogId, dbName, tableName, partName, colName);
    }

    public boolean deleteTableColumnStatistics(String catalogId, String dbName, String tableName, String colName)
            throws UnsupportedOperationException, TException {
        return commonMetaStoreClientDelegate.deleteTableColumnStatistics(catalogId, dbName, tableName, colName);
    }

    public boolean validateNewTableAndCreateDirectory(String catalogId, Table tbl) throws TException {
        if (tableExists(catalogId, tbl.getDbName(), tbl.getTableName())) {
            throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists.");
        }
        validateTableObject(tbl, getConf());

        if (TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
            return false;
        }
        Path tablePath = getTablePath(catalogId, tbl);
        tbl.getSd().setLocation(tablePath.toString());
        return makeDirs(this.warehouse, tablePath);
    }

    public Path getTablePath(String catalogId, Table table) throws TException {
        Path tablePath = null;
        if (StringUtils.isEmpty(table.getSd().getLocation())) {
            Database db = getDatabase(catalogId, table.getDbName());
            if (db.getName().equalsIgnoreCase("default") && ConfigUtils.getDefaultDbCreateTableUseCurrentDbLocation(conf)) {
                tablePath = this.warehouse.getDnsPath(new Path(new Path(db.getLocationUri()), MetaStoreUtils.encodeTableName(table.getTableName().toLowerCase())));
            } else {
                tablePath = this.warehouse.getDefaultTablePath(db, table);
            }
        } else {
            tablePath = this.warehouse.getDnsPath(new Path(table.getSd().getLocation()));
        }
        return tablePath;
    }

    public boolean tableExists(String catalogId, String dbName, String tableName) throws TException {
        checkNotNull(tableName);
        checkNotNull(catalogId);
        checkNotNull(dbName);
        try {
            try {
                com.aliyun.datalake20200710.models.Table result = this.dataLakeMetaStore.getTable(catalogId, dbName, tableName);
                return result != null;
            } catch (DataLakeAPIException e) {
                throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
            }
        } catch (NoSuchObjectException e) {
            return false;
        } catch (Exception e) {
            String msg = "Unable to get table exists: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private void validateTableObject(Table table, Configuration conf) throws InvalidObjectException {
        checkNotNull(table, "table cannot be null");
        checkNotNull(table.getSd(), "Table#StorageDescriptor cannot be null");

        validateTableName(table, conf);
        validateTableColumns(table);
        validatePartitionKeys(table);
        validateSkewedInfo(table);
    }

    private void validateSkewedInfo(Table table) throws InvalidObjectException {
        String validate;
        SkewedInfo skew = table.getSd().getSkewedInfo();
        if (skew != null) {
            validate = hiveShims.validateSkewedColNames(skew.getSkewedColNames());
            if (validate != null) {
                throw new InvalidObjectException("Invalid skew column " + validate);
            }
            validate = hiveShims.validateSkewedColNamesSubsetCol(skew.getSkewedColNames(),
                    table.getSd().getCols());
            if (validate != null) {
                throw new InvalidObjectException("Invalid skew column " + validate);
            }
        }
    }

    private void validatePartitionKeys(Table table) throws InvalidObjectException {
        String validate;
        if (table.getPartitionKeys() != null) {
            validate = hiveShims.validateTblColumns(table.getPartitionKeys());
            if (validate != null) {
                throw new InvalidObjectException("Invalid partition column " + validate);
            }
        }
    }

    private void validateTableColumns(Table table) throws InvalidObjectException {
        List<FieldSchema> cols = table.getSd().getCols();
        if (null == cols || cols.size() < 1) {
            throw new InvalidObjectException("Invalid table columns");
        }
        String validate = hiveShims.validateTblColumns(cols);
        if (validate != null) {
            throw new InvalidObjectException("Invalid column " + validate);
        }
    }

    private void validateTableName(Table table, Configuration conf) throws InvalidObjectException {
        if (!hiveShims.validateName(table.getTableName(), conf)) {
            throw new InvalidObjectException(table.getTableName() + " is not a valid object name");
        }
    }

    public void dropTable(String catalogId, String dbName, Table table, boolean deleteData,
                          EnvironmentContext context)
            throws MetaException, NoSuchObjectException, TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "database cannot be null or empty");
        checkNotNull(table, "table cannot be null");

        Path tablePath = null;
        if (table.getSd().getLocation() != null) {
            tablePath = new Path(table.getSd().getLocation());
        }
        boolean isSourceOfReplication = ReplChangeManager.isSourceOfReplication(getDatabase(catalogId, dbName));
        boolean mustPurge = isMustPurge(context, table);
        boolean isExternal = Utils.isExternalTable(table);
        boolean shouldDeleteData = deleteData && !isExternal;

        // Drop the partitions and get a list of non subdirectory locations which need to be deleted
        List<Path> partPaths;
        try {
            partPaths = getNonSubDirectoryPartitionLocations(catalogId, dbName, table.getTableName(),
                    table.getPartitionKeys(), shouldDeleteData, tablePath);
        } catch (IOException ioException) {
            throw DataLakeUtil.throwException(new MetaException(ioException.getMessage()), ioException);
        }
        try {
            this.dataLakeMetaStore.dropTable(catalogId, dbName, table.getTableName(), deleteData);
            if (shouldDeleteData) {
                deletePartitionData(partPaths, mustPurge, isSourceOfReplication);
                deleteTableData(tablePath, mustPurge, isSourceOfReplication);
            }
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to drop table: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
            throws MetaException, NoSuchObjectException, TException {
        Table tbl = getTable(catName, dbName, tableName);
        Database db = getDatabase(catName, dbName);

        try {
            boolean isAutopurge = (tbl.isSetParameters() && "true".equalsIgnoreCase(tbl.getParameters().get("auto.purge")));
            List<Partition> partitions = getPatitionsForTruncate(catName, dbName, tableName, tbl, partNames);

            for (Path location : getLocationsForTruncate(tbl, partitions)) {
                FileSystem fs = location.getFileSystem(getConf());
                if (!HdfsUtils.isPathEncrypted(getConf(), fs.getUri(), location) &&
                        !org.apache.hadoop.hive.metastore.utils.FileUtils.pathHasSnapshotSubDir(location, fs)) {
                    HdfsUtils.HadoopFileStatus status = new HdfsUtils.HadoopFileStatus(getConf(), fs, location);
                    FileStatus targetStatus = fs.getFileStatus(location);
                    String targetGroup = targetStatus == null ? null : targetStatus.getGroup();
                    hiveShims.deleteDir(warehouse, location, true, isAutopurge, db, enableFsOperation);
                    fs.mkdirs(location);
                    HdfsUtils.setFullFileStatus(getConf(), status, targetGroup, fs, location, false);
                } else {
                    FileStatus[] statuses = fs.listStatus(location, org.apache.hadoop.hive.metastore.utils.FileUtils.HIDDEN_FILES_PATH_FILTER);
                    if (statuses == null || statuses.length == 0) {
                        continue;
                    }
                    for (final FileStatus status : statuses) {
                        hiveShims.deleteDir(warehouse, status.getPath(), true, isAutopurge, db, enableFsOperation);
                    }
                }
            }

            alterTableStatsForTruncate(catName, dbName, tableName, tbl, partitions);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to drop table: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private void alterTableStatsForTruncate(final String catName,
                                            final String dbName,
                                            final String tableName,
                                            final Table table,
                                            final List<Partition> partitions) throws Exception {
        if (partitions == null) {
            EnvironmentContext environmentContext = new EnvironmentContext();
            updateStatsForTruncate(table.getParameters(), environmentContext);
            alterTable(catName, dbName, tableName, table, environmentContext);
        } else {
            for (Partition partition : partitions) {
                alterPartitionForTruncate(catName, dbName, tableName, partition);
            }
        }
        return;
    }

    public List<Partition> getPatitionsForTruncate(final String catName,
                                                   final String dbName,
                                                   final String tableName,
                                                   final Table table,
                                                   final List<String> partNames) throws TException {
        if (partNames == null) {
            if (0 != table.getPartitionKeysSize()) {
                return listPartitions(catName, dbName, tableName, Lists.newArrayList(), Integer.MAX_VALUE);
            } else {
                return null;
            }
        } else {
            return getPartitionsByNames(catName, dbName, tableName, partNames);
        }
    }

    private List<Path> getLocationsForTruncate(final Table table,
                                               List<Partition> partitions) throws Exception {
        List<Path> locations = new ArrayList<>();
        if (partitions != null) {
            partitions.stream().forEach(p -> locations.add(new Path(p.getSd().getLocation())));
        } else {
            locations.add(new Path(table.getSd().getLocation()));
        }
        return locations;
    }

    private void alterPartitionForTruncate(String catName, String dbName, String tableName, Partition partition) throws Exception {
        EnvironmentContext environmentContext = new EnvironmentContext();
        updateStatsForTruncate(partition.getParameters(), environmentContext);
        alterPartitions(catName, dbName, tableName, Lists.newArrayList(partition), environmentContext);
        return ;
    }

    private void updateStatsForTruncate(Map<String,String> props, EnvironmentContext environmentContext) {
        if (null == props) {
            return;
        }
        for (String stat : StatsSetupConst.supportedStats) {
            String statVal = props.get(stat);
            if (statVal != null) {
                //In the case of truncate table, we set the stats to be 0.
                props.put(stat, "0");
            }
        }
        //first set basic stats to true
        StatsSetupConst.setBasicStatsState(props, StatsSetupConst.TRUE);
        environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
        //then invalidate column stats
        StatsSetupConst.clearColumnStatsState(props);
        return;
    }


    private List<Path> getNonSubDirectoryPartitionLocations(String catalogId,
                                                            String dbName,
                                                            String tableName,
                                                            List<FieldSchema> partitionKeys,
                                                            boolean checkLocation,
                                                            Path tablePath) throws IOException, TException {
        if (!checkLocation) {
            return Collections.emptyList();
        }
        List<Path> partPaths = new ArrayList<>();
        String nextToken = EMPTY_TOKEN;
        try {
            if (tablePath != null) {
                tablePath = warehouse.getDnsPath(tablePath);
            }
            if (!partitionKeys.isEmpty()) {
                List<com.aliyun.datalake20200710.models.Partition> partitions = this.dataLakeMetaStore.getNonSubDirectoryPartitionLocations(catalogId, dbName, tableName, this.pageSize);
                if (null != partitions) {
                    for (com.aliyun.datalake20200710.models.Partition partition : partitions) {
                        String absoluteLocation = partition.sd.location;
                        if (null == absoluteLocation) {
                            continue;
                        }
                        Path partPath = warehouse.getDnsPath(new Path(absoluteLocation));
                        if (tablePath == null ||
                                (partPath != null && !Utils.isSubdirectory(tablePath, partPath))) {
                            if (!warehouse.isWritable(partPath.getParent())) {
                                throw new MetaException("Table metadata not deleted since the partition " +
                                        Warehouse.makePartName(partitionKeys, partition.values) +
                                        " has parent location " + partPath.getParent() +
                                        " which is not writable " + "by "
                                        + SecurityUtils.getUser()
                                );
                            }
                            partPaths.add(partPath);
                        }
                    }
                }
            }
            return partPaths;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to get NonSubDirectoryPartitionLocations:";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private void validateTablePath(Path tablePath) throws MetaException {
        try {
            if (!warehouse.isWritable(tablePath.getParent())) {
                throw new MetaException("Table metadata not deleted since " +
                        tablePath.getParent() + " is not writable by "
                        + SecurityUtils.getUser()
                );
            }
        } catch (IOException e) {
            throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
        }
    }

    public void dropTable(
            String catalogId,
            String dbName,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTab,
            EnvironmentContext context
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
        Table table;
        try {
            table = getTable(catalogId, dbName, tableName);
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownTab) {
                throw e;
            }
            return;
        }
        validateTableToDrop(table);
        HiveMetaHook hook = new HiveMetaHookWrapper(hookLoader, table);
        hook.preDropTable(table);
        boolean success = false;
        try {
            dropTable(catalogId, dbName, table, deleteData, context);
            hook.commitDropTable(table, deleteData || (context != null &&
                    "TRUE".equals(context.getProperties().get("ifPurge"))));
            success = true;
        } catch (NoSuchObjectException e) {
            if (!ignoreUnknownTab) {
                throw e;
            }
        } finally {
            if (!success) {
                hook.rollbackDropTable(table);
            }
        }
    }

    /**
     * Give a list of partitions' locations, tries to delete each one
     * and for each that fails logs an error.
     *
     * @param partPaths
     * @param ifPurge   completely purge the partition (skipping trash) while
     *                  removing data from warehouse
     */
    private void deletePartitionData(List<Path> partPaths, boolean ifPurge, boolean needCmRecycle) {
        if (partPaths != null && !partPaths.isEmpty()) {
            for (Path partPath : partPaths) {
                try {
                    hiveShims.deleteDir(warehouse, partPath, true, ifPurge, needCmRecycle, enableFsOperation);
                } catch (Exception e) {
                    logger.error("Failed to delete partition directory: {} {}", partPath, e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Deletes the data in a table's location, if it fails logs an error
     *
     * @param tablePath
     * @param ifPurge   completely purge the table (skipping trash) while removing
     *                  data from warehouse
     */
    private void deleteTableData(Path tablePath, boolean ifPurge, boolean needCmRecycle) {
        if (tablePath != null) {
            try {
                hiveShims.deleteDir(warehouse, tablePath, true, ifPurge, needCmRecycle, enableFsOperation);
            } catch (Exception e) {
                logger.error("Failed to delete table directory: {} {}", tablePath, e.getMessage(), e);
            }
        }
    }

    private void validateTableToDrop(Table table) throws MetaException {
        if (hiveShims.isIndexTable(table)) {
            throw new UnsupportedOperationException("Cannot drop index tables");
        }
        // For a view table, its location could be null
        //if (null == table.getSd() || null == table.getSd().getLocation()) {
        if (null == table.getSd()) {
            throw new MetaException("Table metadata is corrupted");
        }
    }

    public boolean updatePartitionColumnStatistics(String catalogId, ColumnStatistics columnStatistics) throws TException {
        if (columnStatistics.getStatsDesc() == null) {
            throw new MetaException("columnStatistics desc is null");
        }
        Table tbl = getTable(catalogId, columnStatistics.getStatsDesc().getDbName(), columnStatistics.getStatsDesc().getTableName());
//        List<String> partitionNames = new ArrayList<>();
//        partitionNames.add(columnStatistics.getStatsDesc().getPartName());
//        List<Partition> partitions = getPartitionsByNames(catalogId, columnStatistics.getStatsDesc().getDbName(), columnStatistics.getStatsDesc().getTableName(), partitionNames);
        return commonMetaStoreClientDelegate.updatePartitionColumnStatistics(catalogId, tbl, columnStatistics);
    }

    public boolean updateTableColumnStatistics(String catalogId, ColumnStatistics columnStatistics)
            throws TException {
        return commonMetaStoreClientDelegate.updateTableColumnStatistics(catalogId, columnStatistics);
    }

    public List<Partition> addPartitions(
            String catalogId,
            List<Partition> partitions,
            boolean ifNotExist,
            boolean needResult
    ) throws TException, MetaException {
        if (CollectionUtils.isEmpty(partitions)) {
            return needResult ? Lists.newArrayList() : null;
        }

        Partition firstPartition = partitions.get(0);
        String dbName = firstPartition.getDbName();
        String tableName = firstPartition.getTableName();
        Database db = getDatabase(catalogId, dbName);
        validateDbTblAndPartitionNames(partitions, firstPartition.getDbName(), firstPartition.getTableName());
        validateNoDuplicatePartitions(partitions);

        Table table = getTable(catalogId, firstPartition.getDbName(), firstPartition.getTableName());

        boolean success = false;
        Set<String> dirsCreated = new HashSet<>(partitions.size());
        List<com.aliyun.datalake20200710.models.PartitionInput> catalogPartitions = new ArrayList<>(partitions.size());

        try {
            for (Partition partition : partitions) {
                Optional<String> dirCreated = createLocationForAddedPartition(table, partition);
                dirCreated.ifPresent(dirsCreated::add);
                initAddedPartition(table, partition, dirCreated.isPresent());
                catalogPartitions.add(HiveToCatalogConverter.toCatalogPartitionInput(partition));
            }
            List<com.aliyun.datalake20200710.models.Partition> result = this.dataLakeMetaStore.addPartitions(catalogId, dbName, tableName, catalogPartitions, ifNotExist, needResult);
            success = true;

            if (needResult && result != null) {
                return result.stream().map(r -> CatalogToHiveConverter.toHivePartition(r)).collect(Collectors.toList());
            }

            return null;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to add partitions: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        } finally {
            if (!success) {
                for (String dir : dirsCreated) {
                    hiveShims.deleteDir(warehouse, new Path(dir), true, db, enableFsOperation);
                }
            }
        }
    }

    // ==================== Partition ========================================

    private void validateDbTblAndPartitionNames(List<Partition> partitions, String dbName, String tblName)
            throws MetaException {
        if (StringUtils.isBlank(dbName)) {
            throw new MetaException("Database name is blank");
        }
        if (StringUtils.isBlank(tblName)) {
            throw new MetaException("Table name is blank");
        }

        for (Partition part : partitions) {
            if (!dbName.trim().equalsIgnoreCase(part.getDbName().trim()) || !tblName.trim().equalsIgnoreCase(part.getTableName().trim())) {
                throw new MetaException("Partition does not belong to target table "
                        + dbName + "." + tblName + ": " + part);
            }
            if (part.getValues() == null) {
                throw new MetaException("Partition values cannot be null");
            }
            hiveShims.validatePartitionNameCharacters(part.getValues(), partitionValidationPattern);
        }
    }

    private void validateNoDuplicatePartitions(List<Partition> partitions) throws MetaException {
        Set<PartValueEqWrapper> partitionsToAdd = new HashSet<>(partitions.size());
        for (Partition partition : partitions) {
            if (!partitionsToAdd.add(new PartValueEqWrapper(partition.getValues()))) {
                throw new MetaException("Duplicate partitions in the list: " + partition);
            }
        }
    }

    private Optional<String> createLocationForAddedPartition(Table table, Partition partition) throws MetaException {
        Path absolutePath = null;
        String relativeLocation = null;

        if (partition.getSd() == null || partition.getSd().getLocation() == null) {
            // set default location if not specified and this is
            // a physical table partition (not a view)
            if (table.getSd().getLocation() != null) {
                relativeLocation = Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
                absolutePath = new Path(table.getSd().getLocation(), relativeLocation);
            }
        } else if (table.getSd().getLocation() == null) {
            throw new MetaException("Cannot specify location for a view partition");
        } else {
            absolutePath = warehouse.getDnsPath(new Path(partition.getSd().getLocation()));
        }

        if (absolutePath != null) {
            partition.getSd().setLocation(absolutePath.toString());

            if (!warehouse.isDir(absolutePath)) {
                if (!hiveShims.mkdirs(warehouse, absolutePath, true, enableFsOperation)) {
                    throw new MetaException(absolutePath + " is not a directory or unable to create one");
                }

                return Optional.of(absolutePath.toString());
            }
        }
        return Optional.empty();
    }

    private void initAddedPartition(Table table, Partition partition, boolean madeDir) throws MetaException {
        if (MetastoreConf.getBoolVar(getConf(), MetastoreConf.ConfVars.STATS_AUTO_GATHER) && !hiveShims.isView(table)) {
            hiveShims.updatePartitionStatsFast(partition, table, warehouse, madeDir, false, null, true);
        }

        inheritTableParameters(table, partition);
    }

    private void inheritTableParameters(Table table, Partition partition) {
        Map<String, String> tblParams = table.getParameters();
        String inheritProps = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.PART_INHERIT_TBL_PROPS).trim();

        if (StringUtils.isBlank(inheritProps)) {
            return;
        }

        Set<String> inheritKeys = new HashSet<>(Arrays.asList(inheritProps.split(",")));
        if (inheritKeys.contains("*")) {
            inheritKeys = tblParams.keySet();
        }

        for (String key : inheritKeys) {
            String paramVal = tblParams.get(key);
            if (null != paramVal) {
                // add the property only if it exists in table properties
                partition.putToParameters(key, paramVal);
            }
        }
    }

    public int addPartitionsSpecProxy(String catalogId, PartitionSpecProxy pSpec) throws TException {
        List<PartitionSpec> partSpecs = pSpec.toPartitionSpec();
        if (partSpecs.isEmpty()) {
            return 0;
        }
        if (!partSpecs.get(0).isSetCatName()) {
            partSpecs.forEach(ps -> ps.setCatName(catalogId));
        }
        String dbName = partSpecs.get(0).getDbName();
        String tableName = partSpecs.get(0).getTableName();
        boolean ifNotExist = false;
        boolean needResult = true;
        Database db = getDatabase(catalogId, dbName);
        for (PartitionSpec spc : partSpecs) {
            if (spc.isSetPartitionList()) {
                validateDbTblAndPartitionNames(spc.getPartitionList().getPartitions(), dbName, tableName);
                validateNoDuplicatePartitions(spc.getPartitionList().getPartitions());
            } else if (spc.isSetSharedSDPartitionSpec()) {
                validateDbTblAndPartitionNames(getPspecWithSd(spc), dbName, tableName);
                validateNoDuplicatePartitions(getPspecWithSd(spc));
            } else {
                throw new MetaException("failed to addPartitionsSpecProxy: " + "PartitionSpec's type is unknown");
            }
        }

        Table table = getTable(catalogId, dbName, tableName);

        boolean success = false;
        ;
        Set<String> dirsCreated = new HashSet<>(pSpec.size());
        List<com.aliyun.datalake20200710.models.PartitionInput> catalogPartitions = new ArrayList<>(pSpec.size());

        try {
            PartitionSpecProxy.PartitionIterator iterator = pSpec.getPartitionIterator();

            while (iterator.hasNext()) {
                Partition part = iterator.next();
                Optional<String> dirCreated = createLocationForAddedPartition(table, part);
                dirCreated.ifPresent(dirsCreated::add);
                initAddedPartition(table, part, dirCreated.isPresent());
                catalogPartitions.add(HiveToCatalogConverter.toCatalogPartitionInput(part));
            }
            List<com.aliyun.datalake20200710.models.Partition> result = this.dataLakeMetaStore.addPartitions(catalogId, dbName, tableName, catalogPartitions, ifNotExist, needResult);
            success = true;

            if (needResult && result != null) {
                return result.size();
            }

            return 0;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to add partitions: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        } finally {
            if (!success) {
                for (String dir : dirsCreated) {
                    hiveShims.deleteDir(warehouse, new Path(dir), true, db, enableFsOperation);
                }
            }
        }
    }

    public List<Partition> getPspecWithSd(PartitionSpec spc) {
        List<Partition> partitions = new ArrayList<>();
        for (PartitionWithoutSD partWithoutSD : spc.getSharedSDPartitionSpec().getPartitions()) {
            StorageDescriptor partSD = new StorageDescriptor(spc.getSharedSDPartitionSpec().getSd());
            partSD.setLocation(partSD.getLocation() + partWithoutSD.getRelativePath());
            partitions.add(new Partition(partWithoutSD.getValues(), spc.getDbName(), spc.getTableName(), partWithoutSD.getCreateTime(), partWithoutSD.getLastAccessTime(), partSD, partWithoutSD.getParameters()));
        }
        return partitions;
    }

    public void alterPartitions(
            String catalogId,
            String dbName,
            String tblName,
            List<Partition> partitions,
            EnvironmentContext environmentContext
    ) throws TException {
        if (CollectionUtils.isEmpty(partitions)) {
            return;
        }

        validateDbTblAndPartitionNames(partitions, dbName, tblName);
        validateNoDuplicatePartitions(partitions);

        List<com.aliyun.datalake20200710.models.PartitionInput> catalogPartitions =
                new ArrayList<>(partitions.size());
        Table table = getTable(catalogId, dbName, tblName);
        for (Partition partition : partitions) {
            // Adds the missing scheme/authority for the new partition location
            if (partition.getSd() != null) {
                String newLocation = partition.getSd().getLocation();
                if (StringUtils.isNotEmpty(newLocation)) {
                    Path newPath = warehouse.getDnsPath(new Path(newLocation));
                    partition.getSd().setLocation(newPath.toString());
                }
            }
            updatePartitionStatsFast(catalogId, dbName, tblName, table, partition.getValues(), partition, environmentContext, false);
            catalogPartitions.add(HiveToCatalogConverter.toCatalogPartitionInput(partition));
        }
        try {
            this.dataLakeMetaStore.alterPartitions(catalogId, dbName, tblName, catalogPartitions);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to alter partitions: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public Partition appendPartition(
            String catalogId,
            String dbName,
            String tblName,
            List<String> values
    ) throws TException {
        Table table = getTable(catalogId, dbName, tblName);

        if (table.getSd().getLocation() == null) {
            throw new MetaException("Cannot append a partition to a view");
        }

        Partition partition = new Partition();
        partition.setDbName(dbName);
        partition.setTableName(tblName);
        partition.setValues(values);
        partition.setSd(table.getSd().deepCopy());
        String partName = Warehouse.makePartName(table.getPartitionKeys(), values);
        Path partLocation = new Path(table.getSd().getLocation(), partName);
        partition.getSd().setLocation(partLocation.toString());

        List<Partition> partitions = addPartitions(catalogId, Lists.newArrayList(partition), false, true);
        if (partitions.size() != 1) {
            throw new MetaException("Unable to add partition: " + values);
        }
        return partitions.get(0);
    }

    public Partition appendPartition(
            String catalogId,
            String dbName,
            String tblName,
            String partitionName
    ) throws TException {
        Table table = getTable(catalogId, dbName, tblName);
        List<String> partValues = getPartValsFromName(table, partitionName);
        return appendPartition(catalogId, dbName, tblName, partValues);
    }

    private List<String> getPartValsFromName(Table table, String partName)
            throws MetaException, InvalidObjectException {

        LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

        List<String> partVals = new ArrayList<>(table.getPartitionKeys().size());
        for (FieldSchema field : table.getPartitionKeys()) {
            String key = field.getName();
            String val = hm.get(key);
            if (val == null) {
                throw new InvalidObjectException("incomplete partition name - missing " + key);
            }
            partVals.add(val);
        }
        return partVals;
    }

    public boolean listPartitionsByExpr(String catalogId,
                                        String dbName,
                                        String tblName,
                                        byte[] expr,
                                        String defaultPartName,
                                        int max,
                                        List<Partition> result
    ) throws TException {
        final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
        boolean hasUnknownPartitions = false;
        if (exprTree != null) {
            // try first
            String filter = convertToFilter(expr, expressionProxy);
            try {
                List<Partition> partitions = this.dataLakeMetaStore.listPartitionsByExpr(catalogId, dbName, tblName, expr, defaultPartName, max, filter, this.pageSize, new PartitionToHiveVisitor());
                if (partitions != null) {
                    result.addAll(partitions);
                }
            } catch (DataLakeAPIException e) {
                throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
            } catch (Exception e) {
                String msg = "Unable to list partitions: ";
                logger.error(msg, e);
                throw DataLakeUtil.throwException(new MetaException(msg + e), e);
            }
        } else {
            List<String> partitionNames = listPartitionNames(catalogId, dbName, tblName, Lists.newArrayList(), max);
            Table table = this.getTable(catalogId, dbName, tblName);
            hasUnknownPartitions = getPartitionNamesPrunedByExpr(table, expr, defaultPartName, partitionNames);
            if (CollectionUtils.isNotEmpty(partitionNames)) {
                result.addAll(getPartitionsByNames(catalogId, dbName, tblName, partitionNames));
            }
        }

        return hasUnknownPartitions;
    }

    private boolean getPartitionNamesPrunedByExpr(Table table, byte[] expr, String defaultPartName, List<String> result) throws MetaException {
        if (defaultPartName == null || defaultPartName.isEmpty()) {
            defaultPartName = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.DEFAULTPARTITIONNAME);
        }
        return hiveShims.filterPartitionsByExpr(expressionProxy, table.getPartitionKeys(), expr, defaultPartName, result);
    }

    private List<Partition> dropPartitions(
            String catalogId,
            List<Partition> partitionsToDelete,
            boolean ifExist,
            boolean deleteData,
            boolean purgeData
    ) throws TException {
        if (CollectionUtils.isEmpty(partitionsToDelete)) {
            return Lists.newArrayList();
        }

        Partition firstPartition = partitionsToDelete.get(0);
        final String dbName = firstPartition.getDbName();
        final String tableName = firstPartition.getTableName();
        boolean isSourceOfReplication = ReplChangeManager.isSourceOfReplication(getDatabase(catalogId, dbName));
        validateDbTblAndPartitionNames(partitionsToDelete, dbName, tableName);
        validateNoDuplicatePartitions(partitionsToDelete);

        Table table = getTable(catalogId, dbName, tableName);

        // Should we use tableType to decide whether the table is external, instead of using parameters?
        boolean isExternalTbl = hiveShims.isExternalTable(table);
        boolean mustPurge = isMustPurge(purgeData, table);
        boolean shouldDelete = deleteData && !isExternalTbl;
        List<PathAndPartValSize> dirsToDelete = new ArrayList<>();
        List<Path> archToDelete = new ArrayList<>();

        try {
            List<List<String>> partValuesList = new ArrayList<>(partitionsToDelete.size());

            for (Partition part : partitionsToDelete) {
                partValuesList.add(part.getValues());
                Optional<Path> archiveToDelete = getArchiveToDelete(mustPurge, shouldDelete, part,
                        getObjectName(dbName, tableName, part));
                archiveToDelete.ifPresent(archToDelete::add);
                Optional<PathAndPartValSize> dirToDelete = getDirToDelete(mustPurge, shouldDelete, part,
                        getObjectName(dbName, tableName, part));
                dirToDelete.ifPresent(dirsToDelete::add);
            }

            List<Partition> deleted = doDropPartitions(catalogId, dbName, tableName, partValuesList, ifExist,
                    partitionsToDelete);

            if (shouldDelete) {
                logger.info(mustPurge
                        ? "dropPartition() will purge partition-directories directly, skipping trash."
                        : "dropPartition() will move partition-directories to trash-directory.");
                // Archived partitions have har:/to_har_file as their location.
                // The original directory was saved in params
                for (Path path : archToDelete) {

                    hiveShims.deleteDir(warehouse, path, true, mustPurge, isSourceOfReplication, enableFsOperation);
                }
                for (PathAndPartValSize p : dirsToDelete) {
                    hiveShims.deleteDir(warehouse, p.getPath(), true, mustPurge, isSourceOfReplication, enableFsOperation);
                    try {
                        deleteParentRecursive(p.getPath().getParent(), p.getPartValSize() - 1, mustPurge, isSourceOfReplication);
                    } catch (IOException ex) {
                        logger.warn("Error from deleteParentRecursive", ex);
                        throw DataLakeUtil.throwException(new MetaException("Failed to delete parent: " + ex.getMessage()), ex);
                    }
                }
            }
            return deleted;
        } catch (Exception e) {
            String message = "Unable to drop partitions: ";
            logger.error(message, e);
            if (e instanceof MetaException) {
                throw e;
            }
            throw new MetaException(message + e);
        }
    }

    private Optional<Path> getArchiveToDelete(
            boolean mustPurge,
            boolean shouldDelete,
            Partition part,
            String objectName
    ) throws MetaException {
        if (part.getParameters() != null && hiveShims.isArchived(part)) {
            Path archiveParentDir = hiveShims.getOriginalLocation(part);
            verifyPathRemovable(mustPurge, shouldDelete, objectName, archiveParentDir);
            return Optional.of(archiveParentDir);
        }
        return Optional.empty();
    }

    private void verifyPathRemovable(
            boolean mustPurge,
            boolean shouldDelete,
            String objectName,
            Path path
    ) throws MetaException {
        verifyIsWritablePath(path);
    }

    private String getObjectName(String dbName, String tableName, Partition part) {
        return dbName + "." + tableName + "." + part.getValues();
    }

    private void deleteParentRecursive(Path parent, int depth, boolean mustPurge, boolean isSourceOfReplication) throws IOException, MetaException {
        if (depth > 0 && parent != null && warehouse.isWritable(parent)) {
            if (warehouse.isDir(parent) && Utils.isEmptyDir(warehouse, parent)) {
                hiveShims.deleteDir(warehouse, parent, true, mustPurge, isSourceOfReplication, enableFsOperation);
            }
            deleteParentRecursive(parent.getParent(), depth - 1, mustPurge, isSourceOfReplication);
        }
    }

    private void verifyIsWritablePath(Path dir) throws MetaException {
        try {
            if (!warehouse.isWritable(dir.getParent())) {
                throw new MetaException("Table partition not deleted since " + dir.getParent()
                        + " is not writable by "
                        + SecurityUtils.getUser()
                );
            }
        } catch (IOException ex) {
            logger.warn("Error from isWritable", ex);
            throw DataLakeUtil.throwException(new MetaException("Table partition not deleted since " + dir.getParent()
                    + " access cannot be checked: " + ex.getMessage()), ex);
        }
    }

    private Optional<PathAndPartValSize> getDirToDelete(
            boolean mustPurge,
            boolean shouldDelete,
            Partition part,
            String objectName
    ) throws MetaException {
        if (part.getSd() != null && part.getSd().getLocation() != null) {
            Path partPath = new Path(part.getSd().getLocation());

            // What if the table is an external table? Will the path be removable?
            // The answer is yes. Implementation of Warehouse.isWritable will return true if it cannot
            // find the specified file.
            verifyPathRemovable(mustPurge, shouldDelete, objectName, partPath);
            return Optional.of(new PathAndPartValSize(partPath, part.getValues().size()));
        }
        return Optional.empty();
    }

    private List<Partition> doDropPartitions(
            String catalogId,
            String dbName,
            String tableName,
            List<List<String>> partValuesList,
            boolean ifExist,
            List<Partition> partitionsToDelete
    ) throws TException {
        // Delete All Partitions or Not at All, this api has some difference with HMS client
        try {
            this.dataLakeMetaStore.doDropPartitions(catalogId, dbName, tableName, partValuesList, ifExist);
            return partitionsToDelete;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to do drop partitions: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public boolean dropPartition(
            String catalogId,
            String dbName,
            String tblName,
            List<String> values,
            boolean ifExists,
            boolean deleteData,
            boolean purgeData
    ) throws TException {
        try {
            Partition partition = getPartition(catalogId, dbName, tblName, values);
            List<Partition> result = dropPartitions(catalogId, Lists.newArrayList(partition), ifExists, deleteData, purgeData);
            return result.size() > 0;
        } catch (NoSuchObjectException e) {
            if (!ifExists) {
                throw e;
            } else {
                return true;
            }
        }
    }

    public Partition getPartition(
            String catalogId,
            String dbName,
            String tblName,
            List<String> partitionValues
    ) throws TException {
        return getPartition(catalogId, dbName, tblName, partitionValues, null);
    }

    public Partition getPartition(
            String catalogId,
            String dbName,
            String tblName,
            String partitionName
    ) throws TException {
        Table table = getTable(catalogId, dbName, tblName);
        List<String> partitionValues;
        try {
            partitionValues = partitionNameToValues(partitionName, table.getPartitionKeys());
        } catch (InvalidObjectException exception) {
            throw DataLakeUtil.throwException(new NoSuchObjectException(), exception);
        }
        return getPartition(catalogId, dbName, tblName, partitionValues, table);
    }

    private Partition getPartition(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> parValues,
            Table table
    ) throws TException {
        try {
            com.aliyun.datalake20200710.models.Partition partition = this.dataLakeMetaStore.getPartition(catalogId, databaseName, tableName, parValues);
            return CatalogToHiveConverter.toHivePartition(partition);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get partition: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }

    }

    private List<String> partitionNameToValues(String partName, List<FieldSchema> partitionKeys)
            throws MetaException, InvalidObjectException {
        LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

        List<String> partVals = new ArrayList<>();
        for (FieldSchema field : partitionKeys) {
            String key = field.getName();
            String val = hm.get(key);
            if (val == null) {
                throw new InvalidObjectException("incomplete partition name - missing " + key);
            }
            partVals.add(val);
        }
        return partVals;
    }

    private List<String> partitionNameToValues(String name) throws TException {
        if (name.length() == 0) {
            return new ArrayList<>();
        }
        LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(name);
        return new ArrayList<>(map.values());
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatisticsObj(
            String catalogId
            , String dbName
            , String tableName
            , List<String> partitionNames
            , List<String> columnNames) throws TException {
        return commonMetaStoreClientDelegate.getPartitionColumnStatisticsObj(catalogId, dbName, tableName, partitionNames, columnNames);
    }

    public Map<String, ColumnStatistics> getPartitionColumnStatistics(String catalogId
            , String dbName
            , String tableName
            , List<String> partitionNames
            , List<String> columnNames) throws TException {
        return commonMetaStoreClientDelegate.getPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames);
    }

    public boolean setPartitionColumnStatistics(String catalogId, SetPartitionsStatsRequest request) throws TException {
        boolean ret = true;
        List<ColumnStatistics> csNews = request.getColStats();
        if (csNews == null || csNews.isEmpty()) {
            return ret;
        }
        ColumnStatistics firstColStats = csNews.get(0);
        ColumnStatisticsDesc statsDesc = firstColStats.getStatsDesc();
        String dbName = statsDesc.getDbName();
        String tableName = statsDesc.getTableName();
        Table t = getTable(catalogId, dbName, tableName);
        List<String> partitionNames = new ArrayList<>();
        List<Partition> partitions = new ArrayList<>();
        if (!statsDesc.isIsTblLevel()) {
            for (ColumnStatistics csNew : csNews) {
                String partName = csNew.getStatsDesc().getPartName();
                partitionNames.add(partName);
            }
            // a single call to get all column stats for all partitions
            partitions = getPartitionsByNames(catalogId, dbName, tableName, partitionNames);
        }
        return commonMetaStoreClientDelegate.setPartitionColumnStatistics(catalogId, request, t, partitions);
    }

    public List<Partition> getPartitionsByNames(
            String catalogId,
            String dbName,
            String tblName,
            List<String> partitionNames
    ) throws TException {
        List<List<String>> partValuesList = new ArrayList<>(partitionNames.size());

        for (String partitionName : partitionNames) {
            partValuesList.add(partitionNameToValues(partitionName));
        }

        return getPartitionsByValues(catalogId, dbName, tblName, partValuesList);
    }

    private List<Partition> getPartitionsByValues(
            String catalogId,
            String databaseName,
            String tableName,
            List<List<String>> partValuesList
    ) throws TException {
        try {
            List<com.aliyun.datalake20200710.models.Partition> result = this.dataLakeMetaStore.getPartitionsByValues(catalogId, databaseName, tableName, partValuesList);
            return result.stream()
                    .map(CatalogToHiveConverter::toHivePartition)
                    .collect(Collectors.toList());
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to get partition by values:";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }

    }

    public void renamePartition(
            String catalogId,
            String dbName,
            String tblName,
            List<String> partValues,
            Partition newPartition
    ) throws TException {
        Table table = getTable(catalogId, dbName, tblName);
        Partition oldPartition;

        try {
            oldPartition = getPartition(catalogId, dbName, tblName, partValues);
        } catch (NoSuchObjectException e) {
            throw DataLakeUtil.throwException(new InvalidObjectException(e.getMessage()), e);
        }

        if (newPartition.getSd() == null || oldPartition.getSd() == null) {
            throw new InvalidObjectException("Storage descriptor cannot be null");
        }

        // if an external partition is renamed, the location should not change
        if (!Strings.isNullOrEmpty(table.getTableType()) && table.getTableType().equalsIgnoreCase(EXTERNAL_TABLE.toString())) {
            newPartition.getSd().setLocation(oldPartition.getSd().getLocation());
            renamePartitionInCatalog(catalogId, table, partValues, newPartition);
        } else {
            Path destPath = getDestinationPathForRename(table, newPartition, oldPartition);
            Path srcPath = new Path(oldPartition.getSd().getLocation());
            FileSystem srcFs = warehouse.getFs(srcPath);
            FileSystem destFs = warehouse.getFs(destPath);

            verifyDestinationLocation(srcFs, destFs, srcPath, destPath, table, newPartition);
            newPartition.getSd().setLocation(destPath.toString());

            boolean dataWasMoved = false;
            try {
                if (srcFs.exists(srcPath)) {
                    //if destPath's parent path doesn't exist, we should mkdir it
                    Path destParentPath = destPath.getParent();
                    if (!hiveShims.mkdirs(warehouse, destParentPath, true, enableRenameFileOperation)) {
                        throw new IOException("Unable to create path " + destParentPath);
                    }
                    if (Utils.renameDir(warehouse, srcPath, destPath, true, enableRenameFileOperation)) {
                        dataWasMoved = true;
                    }
                }
            } catch (IOException e) {
                throw DataLakeUtil.throwException(new InvalidOperationException(String.format("Unable to access old location %s for partition %s.%s %s", srcPath, table.getDbName(), table.getTableName(), partValues)), e);
            }
            try {
                updatePartitionStatsFast(catalogId, dbName, tblName, table, partValues, newPartition, null, true);
                renamePartitionInCatalog(catalogId, table, partValues, newPartition);
            } catch (Exception e) {
                if (dataWasMoved && !Utils.tryRename(destFs, destPath, srcPath, enableRenameFileOperation)) {
                    logger.error("Failed to restore data from {} to {} in alter table failure. Manual restore is needed.", destPath, srcPath, e);
                }
                throw e;
            }
        }
    }

    public void updatePartitionStatsFast(String catalogId, String dbName, String tblName, Table table, List<String> partVals, Partition partition, EnvironmentContext environmentContext, boolean isRename) throws TException {
        Partition oldPart = getPartition(catalogId, dbName, tblName, partVals);
        if (hiveShims.requireCalStats(conf, oldPart, partition, table, environmentContext)) {
            // if stats are same, no need to update
            if (hiveShims.isFastStatsSame(oldPart, partition) && !isRename) {
                hiveShims.updateBasicState(environmentContext, partition.getParameters());
            } else {
                hiveShims.updatePartitionStatsFast(partition, table, warehouse, false, true, environmentContext, false);
            }
        }
    }

    private void verifyDestinationLocation(
            FileSystem srcFs,
            FileSystem destFs,
            Path srcPath,
            Path destPath,
            Table tbl,
            Partition newPartition
    ) throws InvalidOperationException {
        String oldPartLoc = srcPath.toString();
        String newPartLoc = destPath.toString();

        // check that src and dest are on the same file system
        if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
            throw new InvalidOperationException(String.format("table new location %s is on a different file system than the old location %s. This operation is not supported", destPath, srcPath));
        }
        try {
            srcFs.exists(srcPath); // check that src exists and also checks
            if (enableRenameFileOperation && newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
                throw new InvalidOperationException(String.format("New location for this partition %s.%s.%s already exists : %s", tbl.getDbName(), tbl.getTableName(), newPartition.getValues(), destPath));
            }
        } catch (IOException e) {
            throw DataLakeUtil.throwException(new InvalidOperationException(String.format("Unable to access new location %s for partition %s.%s %s", destPath, tbl.getDbName(), tbl.getTableName(), newPartition.getValues())), e);
        }
    }

    private void renamePartitionInCatalog(
            String catalogId,
            Table table,
            List<String> partitionValues,
            Partition newPartition
    ) throws TException {
        try {
            this.dataLakeMetaStore.renamePartitionInCatalog(catalogId, table.getDbName(), table.getTableName(), partitionValues, HiveToCatalogConverter.toCatalogPartitionInput(newPartition));
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to rename partition";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private Path getDestinationPathForRename(
            Table table,
            Partition newPartition,
            Partition oldPartition
    ) throws TException {

        if (table.getSd().getLocation() == null) {
            throw new MetaException("The table location is null");
        }

        List<FieldSchema> partKeys = table.getPartitionKeys();
        if (partKeys == null || (partKeys.size() != newPartition.getValues().size())) {
            throw new MetaException("Invalid number of partition keys found for " + table.getTableName());
        }

        Map<String, String> partSpec =
                Warehouse.makeSpecFromValues(table.getPartitionKeys(), newPartition.getValues());
        Path tablePath = warehouse.getDnsPath(new Path(table.getSd().getLocation()));
        Path destPath = warehouse.getPartitionPath(tablePath, partSpec);

        // Notice: This is tricky. The second parameter in Hive is actually location from the new partition.
        // It is supposed to be the old location path. Because scheme and authority from old location can be reused.
        // So we use old partition's location here instead.
        return Utils.constructRenamedPath(destPath, new Path(oldPartition.getSd().getLocation()));
    }

    private boolean tryRename(FileSystem fileSystem, Path src, Path dest) {
        try {
            return fileSystem.exists(src) && fileSystem.rename(src, dest);
        } catch (IOException ignored) {
            return false;
        }
    }

    public List<Partition> dropPartitions(
            String catalogId,
            String dbName,
            String tableName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean purgeData,
            boolean needResult
    ) throws TException {
        List<Partition> toBeDeleted = Lists.newArrayList();
        for (ObjectPair<Integer, byte[]> expr : partExprs) {
            listPartitionsByExpr(catalogId, dbName, tableName, expr.getSecond(), null, -1, toBeDeleted);
        }

        List<Partition> deleted = dropPartitions(catalogId, toBeDeleted, false, deleteData, purgeData);
        return needResult ? deleted : null;
    }

    public List<Partition> dropPartitions(
            String catalogId,
            String dbName,
            String tableName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean purgeData,
            boolean ifExist,
            boolean needResult
    ) throws TException {
        List<Partition> toBeDeleted = Lists.newArrayList();
        for (ObjectPair<Integer, byte[]> expr : partExprs) {
            listPartitionsByExpr(catalogId, dbName, tableName, expr.getSecond(), null, -1, toBeDeleted);
        }
        List<Partition> deleted = dropPartitions(catalogId, toBeDeleted, ifExist, deleteData, purgeData);
        return needResult ? deleted : null;
    }

    public List<String> listPartitionNames(
            String catalogId,
            String dbName,
            String tblName,
            List<String> partialPartValues,
            int max
    ) throws TException {
        try {
            Table table = getTable(catalogId, dbName, tblName);
            if (table.getPartitionKeys() == null || table.getPartitionKeys().size() == 0) {
                return new ArrayList<>();
            }
            return this.dataLakeMetaStore.listPartitionNames(catalogId, dbName, tblName, partialPartValues, max, this.pageSize);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to list partitions names: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public int getNumPartitionsByFilter(
            String catalogId,
            String dbName,
            String tblName,
            String filter
    ) throws TException {
        try {
            return this.dataLakeMetaStore.getNumPartitionsByFilter(catalogId, dbName, tblName, filter, this.pageSize);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to list NumPartitionsByFilter: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<PartitionSpec> listPartitionSpecs(
            String catalogId,
            String databaseName,
            String tableName,
            int max
    ) throws TException {
        String cateName = catalogId.toLowerCase();
        String dbName = databaseName.toLowerCase();
        String tblName = tableName.toLowerCase();

        List<PartitionSpec> partitionSpecs = null;
        try {
            Table table = getTable(cateName, dbName, tableName);
            // get_partitions will parse out the catalog and db names itself
            List<Partition> partitions = this.dataLakeMetaStore.listPartitions(cateName, dbName, tblName, max, this.pageSize, new PartitionToHiveVisitor());

            if (is_partition_spec_grouping_enabled(table)) {
                partitionSpecs = get_partitionspecs_grouped_by_storage_descriptor(table, partitions);
            } else {
                PartitionSpec pSpec = new PartitionSpec();
                pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
                pSpec.setCatName(cateName);
                pSpec.setDbName(dbName);
                pSpec.setTableName(tblName);
                pSpec.setRootPath(table.getSd().getLocation());
                partitionSpecs = Arrays.asList(pSpec);
            }
            return partitionSpecs;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to list listPartitionSpecs: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    private List<PartitionSpec> get_partitionspecs_grouped_by_storage_descriptor(Table table, List<Partition> partitions)
            throws NoSuchObjectException, MetaException {

        assert is_partition_spec_grouping_enabled(table);

        final String tablePath = table.getSd().getLocation();

        ImmutableListMultimap<Boolean, Partition> partitionsWithinTableDirectory
                = Multimaps.index(partitions, new com.google.common.base.Function<Partition, Boolean>() {

            @Override
            public Boolean apply(Partition input) {
                return input.getSd().getLocation().startsWith(tablePath);
            }
        });

        List<PartitionSpec> partSpecs = new ArrayList<>();

        // Classify partitions within the table directory into groups,
        // based on shared SD properties.

        Map<StorageDescriptorKey, List<PartitionWithoutSD>> sdToPartList
                = new HashMap<>();

        if (partitionsWithinTableDirectory.containsKey(true)) {

            ImmutableList<Partition> partsWithinTableDir = partitionsWithinTableDirectory.get(true);
            for (Partition partition : partsWithinTableDir) {

                PartitionWithoutSD partitionWithoutSD
                        = new PartitionWithoutSD(partition.getValues(),
                        partition.getCreateTime(),
                        partition.getLastAccessTime(),
                        partition.getSd().getLocation().substring(tablePath.length()), partition.getParameters());

                StorageDescriptorKey sdKey = new StorageDescriptorKey(partition.getSd());
                if (!sdToPartList.containsKey(sdKey)) {
                    sdToPartList.put(sdKey, new ArrayList<>());
                }

                sdToPartList.get(sdKey).add(partitionWithoutSD);

            } // for (partitionsWithinTableDirectory);

            for (Map.Entry<StorageDescriptorKey, List<PartitionWithoutSD>> entry : sdToPartList.entrySet()) {
                partSpecs.add(getSharedSDPartSpec(table, entry.getKey(), entry.getValue()));
            }

        } // Done grouping partitions within table-dir.

        // Lump all partitions outside the tablePath into one PartSpec.
        if (partitionsWithinTableDirectory.containsKey(false)) {
            List<Partition> partitionsOutsideTableDir = partitionsWithinTableDirectory.get(false);
            if (!partitionsOutsideTableDir.isEmpty()) {
                PartitionSpec partListSpec = new PartitionSpec();
                partListSpec.setDbName(table.getDbName());
                partListSpec.setTableName(table.getTableName());
                partListSpec.setPartitionList(new PartitionListComposingSpec(partitionsOutsideTableDir));
                partSpecs.add(partListSpec);
            }

        }
        return partSpecs;
    }

    private PartitionSpec getSharedSDPartSpec(Table table, StorageDescriptorKey sdKey, List<PartitionWithoutSD> partitions) {

        StorageDescriptor sd = new StorageDescriptor(sdKey.getSd());
        sd.setLocation(table.getSd().getLocation()); // Use table-dir as root-dir.
        PartitionSpecWithSharedSD sharedSDPartSpec =
                new PartitionSpecWithSharedSD(partitions, sd);

        PartitionSpec ret = new PartitionSpec();
        ret.setRootPath(sd.getLocation());
        ret.setSharedSDPartitionSpec(sharedSDPartSpec);
        ret.setDbName(table.getDbName());
        ret.setTableName(table.getTableName());

        return ret;
    }

    public List<PartitionSpec> listPartitionSpecsByFilter(
            String catalogId,
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws TException {

        List<PartitionSpec> partitionSpecs = null;
        try {
            Table table = getTable(catalogId, dbName, tblName);
            List<Partition> partitions = listPartitionsByFilter(catalogId, dbName, tblName, filter, max);

            if (is_partition_spec_grouping_enabled(table)) {
                partitionSpecs = get_partitionspecs_grouped_by_storage_descriptor(table, partitions);
            } else {
                PartitionSpec pSpec = new PartitionSpec();
                pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
                pSpec.setRootPath(table.getSd().getLocation());
                pSpec.setCatName(catalogId);
                pSpec.setDbName(dbName);
                pSpec.setTableName(tblName);
                partitionSpecs = Arrays.asList(pSpec);
            }
            return partitionSpecs;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to list listPartitionSpecsByFilter: " + catalogId + "." + dbName + "." + tblName + ":" + filter;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<Partition> listPartitions(
            String catalogId,
            String dbName,
            String tblName,
            List<String> values,
            int max
    ) throws TException {
        return listPartitionsInternal(catalogId, dbName, tblName, values, null, max);
    }

    private List<Partition> listPartitionsInternal(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> values,
            String filter,
            int max
    ) throws TException {
        List<Partition> partitions = new ArrayList<>();
        if (0 == max) {
            return partitions;
        }
        checkArgument(StringUtils.isBlank(filter) || CollectionUtils.isEmpty(values),
                "Filter and partial part values can not be supplied both");
        try {
            return this.dataLakeMetaStore.listPartitionsInternal(catalogId, databaseName, tableName, values, filter, max, this.pageSize, new PartitionToHiveVisitor());
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to list partitions internal: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<Partition> listPartitionsByFilter(
            String catalogId,
            String dbName,
            String tblName,
            String partFilter,
            int max
    ) throws TException {
        return listPartitionsInternal(catalogId, dbName, tblName, null, partFilter, max);
    }

    public List<String> partitionNameToVals(String name) throws TException {
        if (name.length() == 0) {
            return new ArrayList<>();
        }
        LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(name);
        return new ArrayList<>(map.values());
    }

    public void addDynamicPartitions(long txnId, String dbName, String tblName, List<String> partNames,
                                     DataOperationType operationType) {
        throw new UnsupportedOperationException("addDynamicPartitions is not supported");
    }

    public boolean createRole(Role role) {
        throw new UnsupportedOperationException("createRole is not supported");
    }

    public boolean dropRole(String roleName) {
        throw new UnsupportedOperationException("dropRole is not supported");
    }

    public List<Role> listRoles(String principalName, PrincipalType principalType) {
        if (PrincipalType.USER == principalType) {
            return implicitRoles;
        } else {
            throw new UnsupportedOperationException("listRoles is only supported for "
                    + PrincipalType.USER + " Principal type");
        }
    }

    public List<String> listRoleNames() {
        return Lists.newArrayList(PUBLIC);
    }

    // ==================== Role ========================================

    public GetPrincipalsInRoleResponse getPrincipalsInRole(
            GetPrincipalsInRoleRequest request) throws TException {
        throw new UnsupportedOperationException("getPrincipalsInRole is not supported");
    }

    public GetRoleGrantsForPrincipalResponse getRoleGrantsForPrincipal(
            GetRoleGrantsForPrincipalRequest request) throws TException {
        throw new UnsupportedOperationException("getRoleGrantsForPrincipal is not supported");
    }

    public boolean grantRole(
            String roleName,
            String userName,
            PrincipalType principalType,
            String grantor,
            PrincipalType grantorType,
            boolean grantOption
    ) throws TException {
        throw new UnsupportedOperationException("grantRole is not supported");
    }

    public boolean revokeRole(
            String roleName,
            String userName,
            PrincipalType principalType,
            boolean grantOption
    ) throws TException {
        throw new UnsupportedOperationException("revokeRole is not supported");
    }

    public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef obj, String user, List<String> groups) {
        // getPrivilegeSet is NOT yet supported.
        // return null not to break due to optional info
        // Hive return null when every condition fail
        return null;
    }

    public boolean grantPrivileges(PrivilegeBag privileges) {
        throw new UnsupportedOperationException("grantPrivileges is not supported");
    }

    public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) {
        throw new UnsupportedOperationException("revokePrivileges is not supported");
    }

    public List<HiveObjectPrivilege> listPrivileges(
            String principal,
            PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws TException {
        throw new UnsupportedOperationException("listPrivileges is not supported");
    }

    public void cancelDelegationToken(String tokenStrForm) {
    }

    public String getTokenStrForm() {
        logger.warn("getTokenStrForm is not supported by dlf client, return null result by default");
        return null;
    }

    public boolean addToken(String tokenIdentifier, String delegationToken) {
        throw new UnsupportedOperationException("addToken is not supported");
    }

    public boolean removeToken(String tokenIdentifier) {
        throw new UnsupportedOperationException("removeToken is not supported");
    }

    public String getToken(String tokenIdentifier) {
        throw new UnsupportedOperationException("getToken is not supported");
    }

    public List<String> getAllTokenIdentifiers() {
        throw new UnsupportedOperationException("getAllTokenIdentifiers is not supported");
    }

    public int addMasterKey(String key) {
        throw new UnsupportedOperationException("addMasterKey is not supported");
    }

    public void updateMasterKey(Integer seqNo, String key) {
        throw new UnsupportedOperationException("updateMasterKey is not supported");
    }

    public boolean removeMasterKey(Integer keySeq) {
        throw new UnsupportedOperationException("removeMasterKey is not supported");
    }

    public String[] getMasterKeys() {
        throw new UnsupportedOperationException("getMasterKeys is not supported");
    }

    public LockResponse checkLock(long lockId) throws TException {
        try {
            com.aliyun.datalake20200710.models.LockStatus lockStatus = this.dataLakeMetaStore.getLock(lockId);
            return CatalogToHiveConverter.toHiveLockResponse(lockStatus);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to checklock: " + lockId;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void commitTxn(long txnId) {
        throw new UnsupportedOperationException("commitTxn is not supported");
    }

    public void abortTxns(List<Long> txnIds) {
        throw new UnsupportedOperationException("abortTxns is not supported");
    }

    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType) {
        throw new UnsupportedOperationException("compact is not supported");
    }

    // ==================== Transaction ========================================

    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties) {
        throw new UnsupportedOperationException("compact is not supported");
    }

    public CompactionResponse compact2(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties) {
        throw new UnsupportedOperationException("compact2 is not supported");
    }

    public Partition exchangePartition(
            Map<String, String> partitionSpecs,
            String srcCatalogId,
            String srcDb,
            String srcTbl,
            String dstCatalogId,
            String dstDb,
            String dstTbl) {
        throw new UnsupportedOperationException("exchangePartition is not supported");
    }

    public List<Partition> exchangePartitions(
            Map<String, String> partitionSpecs,
            String srcCatalogId,
            String sourceDb,
            String sourceTbl,
            String desCatalogId,
            String destDb,
            String destTbl) {
        throw new UnsupportedOperationException("exchangePartitions is not supported");
    }

    private BloomFilter createPartsBloomFilter(int maxPartsPerCacheNode, double fpp,
                                               List<String> partNames) {
        BloomFilter bloomFilter = new BloomFilter(maxPartsPerCacheNode, fpp);
        for (String partName : partNames) {
            bloomFilter.add(partName.getBytes());
        }
        return bloomFilter;
    }

    public AggrStats getAggrColStatsFor(
            String catalogId,
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partNames) throws TException {
        if (colNames.isEmpty() || partNames.isEmpty()) {
            logger.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
            return new AggrStats(new ArrayList<ColumnStatisticsObj>(), 0); // Nothing to aggregate
        }
        String catalogLowercase = catalogId.toLowerCase();
        String dbNameLowercase = dbName.toLowerCase();
        String tblNameLowercase = tblName.toLowerCase();
        List<String> lowerCaseColNames = new ArrayList<>(colNames.size());
        for (String colName : colNames) {
            lowerCaseColNames.add(colName.toLowerCase());
        }
        List<String> lowerCasePartNames = new ArrayList<>(partNames.size());
        for (String partName : partNames) {
            lowerCasePartNames.add(Utils.lowerCaseConvertPartName(partName));
        }
        PartitionsStatsRequest req = new PartitionsStatsRequest(dbNameLowercase, tblNameLowercase, lowerCaseColNames, lowerCasePartNames);

        long partsFound = 0;
        List<ColumnStatisticsObj> colStatsList;
        if (isAggregateStatsCacheEnabled
                && (partNames.size() < aggrStatsCache.getMaxPartsPerCacheNode())) {
            AggregateStatsCache.AggrColStats colStatsAggrCached;
            List<ColumnStatisticsObj> colStatsAggrFromDB;
            int maxPartsPerCacheNode = aggrStatsCache.getMaxPartsPerCacheNode();
            double fpp = aggrStatsCache.getFalsePositiveProbability();
            colStatsList = new ArrayList<ColumnStatisticsObj>();
            // Bloom filter for the new node that we will eventually add to the cache
            BloomFilter bloomFilter = createPartsBloomFilter(maxPartsPerCacheNode, fpp, lowerCasePartNames);
            boolean computePartsFound = true;
            for (String colName : lowerCaseColNames) {
                // Check the cache first
                colStatsAggrCached = aggrStatsCache.get(catalogLowercase, dbNameLowercase, tblNameLowercase, colName, lowerCasePartNames);
                if (colStatsAggrCached != null) {
                    colStatsList.add(colStatsAggrCached.getColStats());
                    partsFound = colStatsAggrCached.getNumPartsCached();
                } else {
                    if (computePartsFound) {
                        partsFound = this.commonMetaStoreClientDelegate.partsFoundForPartitions(catalogLowercase, dbNameLowercase, tblNameLowercase, lowerCasePartNames, lowerCaseColNames);
                        computePartsFound = false;
                    }
                    List<String> colNamesForDB = new ArrayList<String>();
                    colNamesForDB.add(colName);
                    // Read aggregated stats for one column
                    colStatsAggrFromDB =
                            this.commonMetaStoreClientDelegate.columnStatisticsObjForPartitions(catalogLowercase, dbNameLowercase, tblNameLowercase, lowerCasePartNames, colNamesForDB,
                                    partsFound, hiveShims.getDensityFunctionForNDVEstimation(conf), hiveShims.getNdvTuner(conf));
                    if (!colStatsAggrFromDB.isEmpty()) {
                        ColumnStatisticsObj colStatsAggr = colStatsAggrFromDB.get(0);
                        colStatsList.add(colStatsAggr);
                        // Update the cache to add this new aggregate node
                        aggrStatsCache.add(catalogLowercase, dbNameLowercase, tblNameLowercase, colName, partsFound, colStatsAggr, bloomFilter);
                    }
                }
            }
            return new AggrStats(colStatsList, partsFound);
        } else {
            return commonMetaStoreClientDelegate.getAggrStatsFor(catalogId, req);
        }
    }

    public String getDelegationToken(String owner, String renewerKerberosPrincipalName) {
        return null;
    }

    public List<FieldSchema> getFields(String catalogId, String db, String tableName) throws TException {
        return getFieldsWithEnvironmentContext(catalogId, db, tableName, null);
    }

    public List<FieldSchema> getFieldsWithEnvironmentContext(
            String catalogId,
            String db,
            String tableName,
            final EnvironmentContext envContext
    ) throws TException {
        Table tbl = getAndCheckTable(catalogId, db, tableName);
        return getFieldsWithEnvironmentContext(tableName, tbl, envContext);
    }

    private List<FieldSchema> getFieldsWithEnvironmentContext(
            String tableName,
            Table tbl,
            final EnvironmentContext envContext
    ) throws TException {
        if (useInternalSerde(tbl)) {
            return tbl.getSd().getCols();
        }

        ClassLoader orgHiveLoader = null;
        Configuration curConf = getConf();
        try {
            if (envContext != null) {
                String addedJars = envContext.getProperties().get(HiveConf.ConfVars.HIVEADDEDJARS.varname);
                if (StringUtils.isNotBlank(addedJars)) {
                    orgHiveLoader = curConf.getClassLoader();
                    ClassLoader loader =
                            hiveShims.addToClassPath(orgHiveLoader, StringUtils.split(addedJars, ","));
                    curConf.setClassLoader(loader);
                }
            }
            Deserializer s = hiveShims.getDeserializer(curConf, tbl, false);
            return hiveShims.getFieldsFromDeserializer(tableName, s);
        } catch (Exception e) {
            throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
        } finally {
            if (orgHiveLoader != null) {
                curConf.setClassLoader(orgHiveLoader);
            }
        }
    }

    private boolean useInternalSerde(Table tbl) {
        String serializationLib = tbl.getSd().getSerdeInfo().getSerializationLib();
        if (null == serializationLib) {
            return true;
        }
        Collection<String> internalSerdes = MetastoreConf.getStringCollection(conf,
                MetastoreConf.ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA);
        return internalSerdes.contains(serializationLib);
    }

    public List<FieldSchema> getSchema(String catalogId, String db, String tableName) throws TException {
        return getSchemaWithEnvironmentContext(catalogId, db, tableName, null);
    }

    public List<FieldSchema> getSchemaWithEnvironmentContext(
            String catalogId,
            String db,
            String tableName,
            final EnvironmentContext envContext
    ) throws TException {
        Table tbl = getAndCheckTable(catalogId, db, tableName);
        List<FieldSchema> fieldSchemas = getFieldsWithEnvironmentContext(tableName, tbl, envContext);
        if (tbl.getPartitionKeys() != null) {
            fieldSchemas.addAll(tbl.getPartitionKeys());
        }
        return fieldSchemas;
    }

    private Table getAndCheckTable(
            String catalogId,
            String db,
            String tableName
    ) throws TException {
        String[] names = tableName.split("\\.");
        String baseTableName = names[0];
        Table tbl = getTable(catalogId, db, baseTableName);
        if (tbl == null) {
            throw new UnknownTableException(tableName + " doesn't exist");
        }
        return tbl;
    }

    public ValidTxnList getValidTxns() {
        throw new UnsupportedOperationException("getValidTxns is not supported");
    }

    public ValidTxnList getValidTxns(long currentTxn) {
        throw new UnsupportedOperationException("getValidTxns is not supported");
    }

    public void heartbeat(long txnId, long lockId) throws TException {
        try {
            boolean result = this.dataLakeMetaStore.refreshLock(lockId);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to heart beat lock: " + txnId + " " + lockId;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) {
        throw new UnsupportedOperationException("heartbeatTxnRange is not supported");
    }

    public boolean isPartitionMarkedForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType) {
        return false;
    }

    public List<String> listTableNamesByFilter(String catalogId, String dbName, String filter, int maxTables) {
        throw new UnsupportedOperationException();
    }

    public LockResponse lock(String catalogId, LockRequest lockRequest) throws TException {
        checkArgument(!lockRequest.getComponent().isEmpty(), "unable to lock with empty components");
        try {
            com.aliyun.datalake20200710.models.LockStatus lockStatus = this.dataLakeMetaStore.lock(lockRequest.getComponent().stream().map(component -> HiveToCatalogConverter.toCatalogLockObj(catalogId, component)).collect(Collectors.toList()));
            return CatalogToHiveConverter.toHiveLockResponse(lockStatus);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to lock: " + lockRequest.getHostname() + " " + lockRequest.getUser();
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType) {
        throw new UnsupportedOperationException("markPartitionForEvent is not supported");
    }

    public long openTxn(String user) {
        throw new UnsupportedOperationException("openTxn is not supported");
    }

    public OpenTxnsResponse openTxns(String user, int numTxns) {
        throw new UnsupportedOperationException("openTxns is not supported");
    }

    public long renewDelegationToken(String tokenStrForm) {
        return 0L;
    }

    public void rollbackTxn(long txnId) {
        throw new UnsupportedOperationException();
    }

    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) {
        throw new UnsupportedOperationException();
    }

    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters) {
        throw new UnsupportedOperationException();
    }

    public void clearFileMetadata(List<Long> fileIds) {
        throw new UnsupportedOperationException();
    }

    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) {
        throw new UnsupportedOperationException();
    }

    public boolean cacheFileMetadata(String dbName, String tblName, String partName, boolean allParts) {
        throw new UnsupportedOperationException();
    }

    public void createTableWithConstraints(Table table, List<SQLPrimaryKey> primaryKeys,
                                           List<SQLForeignKey> foreignKeys) {
        // TODO: fire hook like createTable
        throw new UnsupportedOperationException("createTableWithConstraints is not supported");
    }

    public void dropConstraint(String dbName, String tblName, String constraintName) {
        throw new UnsupportedOperationException("dropConstraint is not supported");
    }

    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) {
        throw new UnsupportedOperationException("addPrimaryKey is not supported");
    }

    public void addForeignKey(List<SQLForeignKey> foreignKeyCols) {
        throw new UnsupportedOperationException("addForeignKey is not supported");
    }

    public ShowCompactResponse showCompactions() {
        throw new UnsupportedOperationException("showCompactions is not supported");
    }

    public void insertTable(Table table, boolean overwrite) {
        // TODO: fire hook like createTable
        throw new UnsupportedOperationException("insertTable is not supported");
    }

    public NotificationEventResponse getNextNotification(
            long lastEventId,
            int maxEvents,
            IMetaStoreClient.NotificationFilter notificationFilter) {
        throw new UnsupportedOperationException("getNextNotification is not supported");
    }

    public CurrentNotificationEventId getCurrentNotificationEventId() {
        throw new UnsupportedOperationException("getCurrentNotificationEventId is not supported");
    }

    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) {
        throw new UnsupportedOperationException("fireListenerEvent is not supported");
    }

    public ShowLocksResponse showLocks() {
        throw new UnsupportedOperationException("showLocks is not supported");
    }

    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) {
        throw new UnsupportedOperationException("showLocks is not supported");
    }

    public GetOpenTxnsInfoResponse showTxns() {
        throw new UnsupportedOperationException("showTxns is not supported");
    }

    public void unlock(long lockId) throws TException {
        try {
            boolean result = this.dataLakeMetaStore.unLock(lockId);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to unlock: " + lockId;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void createFunction(String catalogId, Function function) throws TException {
        try {
            this.dataLakeMetaStore.createFunction(catalogId, HiveToCatalogConverter.toCatalogFunctionInput(catalogId, function), function.getDbName());
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to create function";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public Function getFunction(String catalogId, String dbName, String functionName) throws TException {
        try {
            com.aliyun.datalake20200710.models.Function result = this.dataLakeMetaStore.getFunction(catalogId, dbName, functionName);
            return CatalogToHiveConverter.toHiveFunction(result);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to get funcition: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<String> getFunctions(String catalogId, String dbName, String pattern) throws TException {
        try {
            return this.dataLakeMetaStore.getFunctions(catalogId, dbName, pattern, this.pageSize);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to get funcitions: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public List<Function> getFunctionObjects(String catalogId, String dbName, String pattern) throws TException {
        List<Function> functions = new ArrayList<>();
        try {
            List<com.aliyun.datalake20200710.models.Function> result = this.dataLakeMetaStore.getFunctionObjects(catalogId, dbName, pattern, this.pageSize);
            result.stream().forEach(r -> functions.add(CatalogToHiveConverter.toHiveFunction(r)));
            return functions;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to get function objects";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }


    // ==================== Function ========================================

    public void alterFunction(
            String catalogId,
            String dbName,
            String functionName,
            Function function
    ) throws TException {
        try {
            this.dataLakeMetaStore.alterFunction(catalogId, dbName, functionName, HiveToCatalogConverter.toCatalogFunctionInput(catalogId, function));
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to alter funcition: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public void dropFunction(String catalogId, String dbName, String functionName) throws TException {
        try {
            this.dataLakeMetaStore.dropFunction(catalogId, dbName, functionName);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to drop funcition: ";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public GetAllFunctionsResponse getAllFunctions(String catalogId) throws TException {
        String nextToken = EMPTY_TOKEN;
        try {
            List<String> databases = this.dataLakeMetaStore.getDatabases(catalogId, ".*", this.pageSize);
            List<Future<List<Function>>> listFunctionsFutures = Lists.newArrayList();
            for (String dbName : databases) {
                listFunctionsFutures.add(this.executorService.submit(() -> getFunctionObjects(catalogId, dbName, ".*")));
            }

            final List<Function> functions = new ArrayList<>();
            for (Future<List<Function>> listFunctionsFuture : listFunctionsFutures) {
                try {
                    functions.addAll(listFunctionsFuture.get());
                } catch (Exception e) {
                    if (e instanceof TeaException) {
                        throw (TeaException) e;
                    }
                    throw DataLakeUtil.throwException(new MetaException("get functions failed, message: " + e.getMessage()), e);
                }
            }

            GetAllFunctionsResponse getAllFunctionsResponse = new GetAllFunctionsResponse();
            getAllFunctionsResponse.setFunctions(functions);
            return getAllFunctionsResponse;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "unable to get all functions";
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    protected ExecutorService getExecutorService() {
        Class<? extends ExecutorServiceFactory> executorFactoryClass = getConf()
                .getClass(EXECUTOR_FACTORY_CONF,
                        DefaultExecutorServiceFactory.class).asSubclass(
                        ExecutorServiceFactory.class);
        ExecutorServiceFactory factory = ReflectionUtils.newInstance(
                executorFactoryClass, getConf());
        return factory.getExecutorService(ConfigUtils.getMetaStoreNumThreads(conf));
    }

    private static class PartitionToHiveVisitor implements IDataLakeMetaStore.PartitionVisitor<List<Partition>,
            com.aliyun.datalake20200710.models.Partition> {
        private List<Partition> result;

        PartitionToHiveVisitor() {
            result = new ArrayList<>();
        }

        @Override
        public void accept(List<com.aliyun.datalake20200710.models.Partition> partitions) {
            partitions.stream().forEach(p -> result.add(CatalogToHiveConverter.toHivePartition(p)));
        }

        @Override
        public List<Partition> getResult() {
            return result;
        }
    }

    private static class PathAndPartValSize {
        private Path path;
        private int partValSize;
        PathAndPartValSize(Path path, int partValSize) {
            this.path = path;
            this.partValSize = partValSize;
        }

        public Path getPath() {
            return path;
        }

        public int getPartValSize() {
            return partValSize;
        }
    }

    private static class PartValueEqWrapper {

        private final ImmutableList<String> partValues;

        PartValueEqWrapper(List<String> partVals) {
            this.partValues = partVals == null ? null : ImmutableList.copyOf(partVals);
        }

        @Override
        public int hashCode() {
            if (partValues == null) {
                return 0;
            } else {
                return partValues.hashCode();
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PartValueEqWrapper) {
                return Objects.equals(partValues, ((PartValueEqWrapper) obj).partValues);
            }
            return false;
        }
    }

    private static class StorageDescriptorKey {

        private final StorageDescriptor sd;

        StorageDescriptorKey(StorageDescriptor sd) {
            this.sd = sd;
        }

        StorageDescriptor getSd() {
            return sd;
        }

        private String hashCodeKey() {
            return sd.getInputFormat() + "\t"
                    + sd.getOutputFormat() + "\t"
                    + sd.getSerdeInfo().getSerializationLib() + "\t"
                    + sd.getCols();
        }

        @Override
        public int hashCode() {
            return hashCodeKey().hashCode();
        }

        @Override
        public boolean equals(Object rhs) {
            if (rhs == this) {
                return true;
            }

            if (!(rhs instanceof StorageDescriptorKey)) {
                return false;
            }

            return (hashCodeKey().equals(((StorageDescriptorKey) rhs).hashCodeKey()));
        }
    }
}