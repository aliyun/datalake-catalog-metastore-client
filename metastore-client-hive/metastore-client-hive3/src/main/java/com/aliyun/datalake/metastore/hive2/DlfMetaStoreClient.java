package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.aliyun.datalake.metastore.hive.shims.ShimsLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class DlfMetaStoreClient implements IMetaStoreClient {
    private static final Logger logger = LoggerFactory.getLogger(DlfMetaStoreClient.class); //Logger.getLogger(DlfMetaStoreClient.class);
    private static volatile Boolean isCreatedDefaultDatabase;
    protected final Configuration conf;
    private final Warehouse warehouse;
    private final DlfMetaStoreClientDelegate clientDelegate;
    private final String catalogId;
    private final IHiveShims hiveShims = ShimsLoader.getHiveShims();
    private final Pattern partitionValidationPattern;
    private final Boolean enableFsOperation;
    private Map<String, String> currentMetaVars;
    private final MetaStoreFilterHook filterHook;

    public DlfMetaStoreClient(Configuration conf) throws MetaException {
        this(conf, null);
    }

    public DlfMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
        if (conf == null) {
            conf = MetastoreConf.newMetastoreConf();
            this.conf = conf;
        } else {
            this.conf = new Configuration(conf);
        }
        this.catalogId = ConfigUtils.getCatalogId(this.conf);
        this.warehouse = new Warehouse(this.conf);
        String partitionValidationRegex =
                MetastoreConf.getVar(this.conf, MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
        if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
            partitionValidationPattern = Pattern.compile(partitionValidationRegex);
        } else {
            partitionValidationPattern = null;
        }
        snapshotActiveConf();
        filterHook = loadFilterHooks();
        this.clientDelegate = new DlfMetaStoreClientDelegate(this.conf, warehouse, hookLoader);
        boolean createDefaultDatabase = getConf().getBoolean(DataLakeConfig.CATALOG_CREATE_DEFAULT_DB, true);
        createDefaultCatalogAndDatabase(createDefaultDatabase);
        this.enableFsOperation = clientDelegate.getEnableFsOperation();
    }

    //hive3的RetryingMetaStoreClient中的getProxy支持的construct要求的conf的类型是configuration，而不是hiveconf，详见 RetryingMetaStoreClient中的getProxy函数
    public DlfMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, boolean allowEmbedded) throws MetaException {
        long startTime = System.currentTimeMillis();
        logger.info("DlfMetaStoreClient start");
        if (conf == null) {
            conf = MetastoreConf.newMetastoreConf();
            this.conf = conf;
        } else {
            this.conf = new Configuration(conf);
        }
        this.catalogId = ConfigUtils.getCatalogId(this.conf);
        this.warehouse = new Warehouse(this.conf);
        String partitionValidationRegex =
                MetastoreConf.getVar(this.conf, MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
        if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
            partitionValidationPattern = Pattern.compile(partitionValidationRegex);
        } else {
            partitionValidationPattern = null;
        }
        snapshotActiveConf();
        filterHook = loadFilterHooks();
        this.clientDelegate = new DlfMetaStoreClientDelegate(this.conf, warehouse, hookLoader);
        boolean createDefaultDatabase = getConf().getBoolean(DataLakeConfig.CATALOG_CREATE_DEFAULT_DB, true);
        createDefaultCatalogAndDatabase(createDefaultDatabase);
        this.enableFsOperation = clientDelegate.getEnableFsOperation();
        logger.info("DlfMetaStoreClient end, cost:{}ms", System.currentTimeMillis() - startTime);
    }

    private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
        Class<? extends MetaStoreFilterHook> authProviderClass = MetastoreConf.
                getClass(conf, MetastoreConf.ConfVars.FILTER_HOOK, DefaultMetaStoreFilterHookImpl.class,
                        MetaStoreFilterHook.class);
        String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
        try {
            Constructor<? extends MetaStoreFilterHook> constructor =
                    authProviderClass.getConstructor(Configuration.class);
            return constructor.newInstance(conf);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        }
    }

    public Boolean getEnableFsOperation() {
        return enableFsOperation;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public void createDefaultCatalogAndDatabase(
            boolean ifCreateDefaultDatabase) throws MetaException {
        // createDefaultCatalogIfNotExists();
        if (ifCreateDefaultDatabase) {
            synchronized (DlfMetaStoreClient.class) {
                if (isCreatedDefaultDatabase == null || isCreatedDefaultDatabase == false) {
                    logger.info("dlf metastoreclient create default database ");
                    createDefaultDatabaseIfNotExists();
                    isCreatedDefaultDatabase = true;
                }
            }
        }
    }

    public DlfMetaStoreClientDelegate getClientDelegate() {
        return clientDelegate;
    }
//    public void createDefaultCatalogIfNotExists() throws MetaException {
    //        String uri = warehouse.getWhRoot().toString();
    //        try {
    //            Catalog catalog = clientDelegate.getCatalog(catalogId);
    //            if (("TBD").equals(catalog.getLocationUri())) {
    //                catalog.setLocationUri(uri);
    //                clientDelegate.alterCatalog(catalogId, catalog);
    //            }
    //        } catch (NoSuchObjectException e) {
    //            try {
    //                clientDelegate.createCatalog(catalogId, Constance.DEFAULT_CATALOG_COMMENT, uri);
    //            } catch (TException te) {
    //                logger.error("createDefaultCatalogIfNotExists error:", te);
    //                throw new MetaException(te.getMessage());
    //            }
    //        } catch (Exception e) {
    //            logger.error("createDefaultCatalogIfNotExists error:", e);
    //            throw new MetaException(e.getMessage());
    //        }
    //    }

    private void createDefaultDatabaseIfNotExists() throws MetaException {
        try {
            clientDelegate.getDatabase(this.catalogId, Warehouse.DEFAULT_DATABASE_NAME);
        } catch (NoSuchObjectException noObjectEx) {
            Database defaultDB = getDefaultDatabaseObj();
            try {
                createDatabase(defaultDB);
            } catch (AlreadyExistsException ignoreException) {
                logger.warn("database - default already exists. Ignoring..");
            } catch (Exception e) {
                logger.error("Unable to create default database", e);
            }
        } catch (Exception ex) {
            logger.error("getDefaultDb exception");
            throw DataLakeUtil.throwException(new MetaException(ex.getMessage()), ex);
        }
    }

    private Database getDefaultDatabaseObj() throws MetaException {
        Database defaultDB = new Database();
        defaultDB.setName(Warehouse.DEFAULT_DATABASE_NAME);
        defaultDB.setDescription(Warehouse.DEFAULT_DATABASE_COMMENT);
        defaultDB.setParameters(Collections.EMPTY_MAP);
        defaultDB.setLocationUri(warehouse.getDefaultDatabasePath(Warehouse.DEFAULT_DATABASE_NAME).toString());
        defaultDB.setOwnerType(PrincipalType.USER);
        try {
            defaultDB.setOwnerName(UserGroupInformation.getCurrentUser().getUserName());
        } catch (IOException e) {
            defaultDB.setOwnerName("root");
        }
        PrincipalPrivilegeSet principalPrivilegeSet
                = new PrincipalPrivilegeSet();
        principalPrivilegeSet.setRolePrivileges(Maps.newHashMap());
        principalPrivilegeSet.setGroupPrivileges(Maps.newHashMap());
        principalPrivilegeSet.setUserPrivileges(Maps.newHashMap());
        /**
         * TODO: Grant access to role PUBLIC after role support is added
         */
        defaultDB.setPrivileges(principalPrivilegeSet);
        return defaultDB;
    }

    @Override
    public void createDatabase(Database database)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        clientDelegate.createDatabase(this.catalogId, database);
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
        return getDatabase(this.catalogId, name);
    }

    @Override
    public Database getDatabase(String catalogId, String databaseName) throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterDatabase(clientDelegate.getDatabase(catalogId, databaseName));
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return getDatabases(this.catalogId, pattern);
    }

    @Override
    public List<String> getDatabases(String catalogId, String databasePattern) throws MetaException, TException {
        return filterHook.filterDatabases(clientDelegate.getDatabases(catalogId, databasePattern));
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases(".*");
    }

    @Override
    public List<String> getAllDatabases(String catalogId) throws MetaException, TException {
        return getDatabases(catalogId, ".*");
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws NoSuchObjectException, MetaException, TException {
        clientDelegate.alterDatabase(this.catalogId, databaseName, database);
    }

    @Override
    public void alterDatabase(String catalogId, String databaseName, Database database) throws NoSuchObjectException, MetaException, TException {
        clientDelegate.alterDatabase(catalogId, databaseName, database);
    }

    @Override
    public void dropDatabase(String name)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, true, false, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, deleteData, ignoreUnknownDb, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        clientDelegate.dropDatabase(this.catalogId, name, deleteData, ignoreUnknownDb, cascade);
    }

    @Override
    public void dropDatabase(String catalogId, String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        clientDelegate.dropDatabase(catalogId, name, deleteData, ignoreUnknownDb, cascade);
    }

    @Override
    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {

        List<Partition> partitions =
                clientDelegate.addPartitions(this.catalogId, Lists.newArrayList(partition), false, true);
        return partitions != null ? partitions.get(0) : null;
    }

    @Override
    public int add_partitions(List<Partition> partitions)
            throws InvalidObjectException, AlreadyExistsException, MetaException,
            TException {
        return add_partitions(partitions, false, true).size();
    }

    @Override
    public List<Partition> add_partitions(
            List<Partition> partitions,
            boolean ifNotExists,
            boolean needResult
    ) throws TException {
        return filterHook.filterPartitions(clientDelegate.addPartitions(this.catalogId, partitions, ifNotExists, needResult));
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy pSpec)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return clientDelegate.addPartitionsSpecProxy(this.catalogId, pSpec);
    }

    @Override
    public void alterFunction(String dbName, String functionName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        clientDelegate.alterFunction(this.catalogId, dbName, functionName, newFunction);
    }

    @Override
    public void alterFunction(String catalogId, String dbName, String functionName, Function newFunction) throws InvalidObjectException, MetaException, TException {
        clientDelegate.alterFunction(catalogId, dbName, functionName, newFunction);
    }

//    @Override
//    public void alter_index(String dbName, String tblName, String indexName, Index index)
//            throws InvalidOperationException, MetaException, TException {
//        clientDelegate.alterIndex(this.catalogId, dbName, tblName, indexName, index);
//    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition partition)
            throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterPartitions(this.catalogId, dbName, tblName, Lists.newArrayList(partition), null);
    }

    @Override
    public void alter_partition(
            String dbName,
            String tblName,
            Partition partition,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterPartitions(this.catalogId, dbName, tblName, Lists.newArrayList(partition), environmentContext);
    }

    @Override
    public void alter_partition(String catalogId, String dbName, String tblName, Partition partition, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterPartitions(catalogId, dbName, tblName, Lists.newArrayList(partition), environmentContext);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<Partition> partitions
    ) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterPartitions(this.catalogId, dbName, tblName, partitions, null);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<Partition> partitions,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterPartitions(this.catalogId, dbName, tblName, partitions, environmentContext);
    }

    @Override
    public void alter_partitions(String catalogId, String dbName, String tblName, List<Partition> partitions, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterPartitions(catalogId, dbName, tblName, partitions, environmentContext);
    }

    @Override
    public void alter_table(String dbName, String tblName, Table table)
            throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterTable(this.catalogId, dbName, tblName, table, null);
    }

    @Override
    public void alter_table(String catalogId, String dbName, String tblName, Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterTable(catalogId, dbName, tblName, table, environmentContext);
    }

    @Override
    @Deprecated
    public void alter_table(
            String dbName,
            String tblName,
            Table table,
            boolean cascade
    ) throws InvalidOperationException, MetaException, TException {
        EnvironmentContext environmentContext = new EnvironmentContext();
        if (cascade) {
            environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
        }
        clientDelegate.alterTable(this.catalogId, dbName, tblName, table, environmentContext);
    }

    @Override
    public void alter_table_with_environmentContext(
            String dbName,
            String tblName,
            Table table,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterTable(this.catalogId, dbName, tblName, table, environmentContext);
    }

    @Override
    public Partition appendPartition(String dbName, String tblName, List<String> values)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return clientDelegate.appendPartition(this.catalogId, dbName, tblName, values);
    }

    @Override
    public Partition appendPartition(String catalogId, String dbName, String tblName, List<String> values) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return clientDelegate.appendPartition(catalogId, dbName, tblName, values);
    }

    @Override
    public Partition appendPartition(String dbName, String tblName, String partitionName)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return clientDelegate.appendPartition(this.catalogId, dbName, tblName, partitionName);
    }

    @Override
    public Partition appendPartition(String catalogId, String dbName, String tblName, String partitionName) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return clientDelegate.appendPartition(catalogId, dbName, tblName, partitionName);
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        return clientDelegate.createRole(role);
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        return clientDelegate.dropRole(roleName);
    }

    @Override
    public List<Role> list_roles(
            String principalName, PrincipalType principalType
    ) throws MetaException, TException {
        return clientDelegate.listRoles(principalName, principalType);
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return clientDelegate.listRoleNames();
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
            throws MetaException, TException {
        return clientDelegate.getPrincipalsInRole(request);
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
        return clientDelegate.getRoleGrantsForPrincipal(request);
    }

    @Override
    public boolean grant_role(
            String roleName,
            String userName,
            PrincipalType principalType,
            String grantor, PrincipalType grantorType,
            boolean grantOption
    ) throws MetaException, TException {
        return clientDelegate.grantRole(roleName, userName, principalType, grantor, grantorType, grantOption);
    }

    @Override
    public boolean revoke_role(
            String roleName,
            String userName,
            PrincipalType principalType,
            boolean grantOption
    ) throws MetaException, TException {
        return clientDelegate.revokeRole(roleName, userName, principalType, grantOption);
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        clientDelegate.cancelDelegationToken(tokenStrForm);
    }

    @Override
    public String getTokenStrForm() throws IOException {
        return clientDelegate.getTokenStrForm();
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return clientDelegate.addToken(tokenIdentifier, delegationToken);
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return clientDelegate.removeToken(tokenIdentifier);
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return clientDelegate.getToken(tokenIdentifier);
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return clientDelegate.getAllTokenIdentifiers();
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return clientDelegate.addMasterKey(key);
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key)
            throws NoSuchObjectException, MetaException, TException {
        clientDelegate.updateMasterKey(seqNo, key);
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return clientDelegate.removeMasterKey(keySeq);
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return clientDelegate.getMasterKeys();
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return clientDelegate.checkLock(lockId);
    }

    @Override
    public void close() {
        currentMetaVars = null;
    }

    @Override
    public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
        clientDelegate.commitTxn(txnId);
    }

    @Override
    public void replCommitTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TxnAbortedException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abortTxns(List<Long> txnIds) throws TException {
        clientDelegate.abortTxns(txnIds);
    }

    @Override
    public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
        throw new UnsupportedOperationException();
        //return 0;
    }

    @Override
    public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName,
                                                             String replPolicy, List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType
    ) throws TException {
        clientDelegate.compact(dbName, tblName, partitionName, compactionType);
    }

    @Override
    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        clientDelegate.compact(dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public CompactionResponse compact2(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        return clientDelegate.compact2(dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {
        clientDelegate.createFunction(this.catalogId, function);
    }

//    @Override
//    public void createIndex(Index index, Table indexTable)
//            throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {
//        clientDelegate.createIndex(this.catalogId, index, indexTable);
//    }

    @Override
    public void createTable(Table tbl)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        createTable(tbl, null);
    }

    public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        // Subclasses can override this step (for example, for temporary tables)
        create_table_with_environment_context(tbl, envContext);
    }

    protected void create_table_with_environment_context(
            org.apache.hadoop.hive.metastore.api.Table tbl, EnvironmentContext envContext)
            throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {
        // ignore envContext just as HiveMetaStoreClient, which has never use this envContext variable for creating table
        clientDelegate.createTable(this.catalogId, tbl, envContext);
    }

    @Override
    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return clientDelegate.deletePartitionColumnStatistics(this.catalogId, dbName, tableName, partName, colName);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catalogId, String dbName, String tableName, String partName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return clientDelegate.deletePartitionColumnStatistics(catalogId, dbName, tableName, partName, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, InvalidInputException {
        return clientDelegate.deleteTableColumnStatistics(this.catalogId, dbName, tableName, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String catalogId, String dbName, String tableName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return clientDelegate.deleteTableColumnStatistics(catalogId, dbName, tableName, colName);
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
            InvalidObjectException, InvalidInputException, TException {
        clientDelegate.dropFunction(this.catalogId, dbName, functionName);
    }

    @Override
    public void dropFunction(String catalogId, String dbName, String functionName) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        clientDelegate.dropFunction(catalogId, dbName, functionName);
    }

//    @Override
//    public boolean dropIndex(String dbName, String tblName, String name, boolean deleteData)
//            throws NoSuchObjectException, MetaException, TException {
//        return clientDelegate.dropIndex(this.catalogId, dbName, tblName, name, deleteData);
//    }

    private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
        if (depth > 0 && parent != null && warehouse.isWritable(parent) && warehouse.isEmpty(parent)) {
            //hiv3 need set whether dir is replication
            warehouse.deleteDir(parent, true, mustPurge, false);
            deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
        }
    }

    // This logic is taken from HiveMetaStore#isMustPurge
    private boolean isMustPurge(Table table, boolean ifPurge) {
        return (ifPurge || "true".equalsIgnoreCase(table.getParameters().get("auto.purge")));
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartition(this.catalogId, dbName, tblName, values,
                false, deleteData, false);
    }

    @Override
    public boolean dropPartition(String catalogId, String dbName, String tblName, List<String> values, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartition(catalogId, dbName, tblName, values,
                false, deleteData, false);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options)
            throws TException {
        return clientDelegate.dropPartition(this.catalogId, dbName, tblName, values,
                options.ifExists, options.deleteData, options.purgeData);
    }

    @Override
    public boolean dropPartition(String catalogId, String dbName, String tblName, List<String> values, PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartition(catalogId, dbName, tblName, values,
                options.ifExists, options.deleteData, options.purgeData);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
                                          List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                          boolean ifExists) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartitions(this.catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                deleteData, false, ifExists, true);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName,
                                          List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                          boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartitions(this.catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                deleteData, false, ifExists, needResult);
    }

    @Override
    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartitions(this.catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                partitionDropOptions.deleteData, partitionDropOptions.purgeData, partitionDropOptions.returnResults);
    }

    @Override
    public List<Partition> dropPartitions(String catalogId, String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions partitionDropOptions) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartitions(catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                partitionDropOptions.deleteData, partitionDropOptions.purgeData, partitionDropOptions.returnResults);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        List<String> values = partitionNameToVals(partitionName);
        return clientDelegate.dropPartition(this.catalogId, dbName, tblName, values,
                false, deleteData, false);
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tblName, String partitionName,
                                 boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        List<String> values = partitionNameToVals(partitionName);
        return clientDelegate.dropPartition(catalogId, dbName, tblName, values,
                false, deleteData, false);
    }

    @Override
    public void dropTable(String dbname, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        clientDelegate.dropTable(this.catalogId, dbname, tableName, true, true, null);

    }

    @Override
    public void dropTable(String catalogId, String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
        //build new environmentContext with ifPurge;
        EnvironmentContext envContext = null;
        if (ifPurge) {
            Map<String, String> warehouseOptions;
            warehouseOptions = new HashMap<>();
            warehouseOptions.put("ifPurge", "TRUE");
            envContext = new EnvironmentContext(warehouseOptions);
        }
        clientDelegate.dropTable(catalogId, dbname, tableName, deleteData, ignoreUnknownTab, envContext);
    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
        clientDelegate.truncateTable(this.catalogId, dbName, tableName, partNames);
    }

    @Override
    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames) throws MetaException, TException {
        clientDelegate.truncateTable(catName, dbName, tableName, partNames);
    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void dropTable(
            String dbname,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTab
    ) throws MetaException, TException, NoSuchObjectException {
        clientDelegate.dropTable(this.catalogId, dbname, tableName, deleteData, ignoreUnknownTab, null);
    }

    @Override
    public void dropTable(
            String dbname,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTable,
            boolean ifPurge
    ) throws MetaException, TException, NoSuchObjectException {

        EnvironmentContext envContext = null;
        if (ifPurge) {
            Map<String, String> warehouseOptions = null;
            warehouseOptions = new HashMap<>();
            warehouseOptions.put("ifPurge", "TRUE");
            envContext = new EnvironmentContext(warehouseOptions);
        }
        clientDelegate.dropTable(this.catalogId, dbname, tableName, deleteData, ignoreUnknownTable, envContext);
    }

    protected void drop_table_with_environment_context(String dbname, String tableName,
                                                       boolean deleteData, boolean ignoreUnknownTable, EnvironmentContext envContext) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        clientDelegate.dropTable(this.catalogId, dbname, tableName, deleteData, ignoreUnknownTable, envContext);
    }

    @Override
    public Partition exchange_partition(
            Map<String, String> partitionSpecs,
            String srcDb,
            String srcTbl,
            String dstDb,
            String dstTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return clientDelegate.exchangePartition(partitionSpecs, this.catalogId, srcDb, srcTbl, this.catalogId, dstDb, dstTbl);
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String srcCatalogId, String srcDb, String srcTbl, String descCatalogId, String dstDb, String dstTbl) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return clientDelegate.exchangePartition(partitionSpecs, srcCatalogId, srcDb, srcTbl, descCatalogId, dstDb, dstTbl);
    }

    @Override
    public List<Partition> exchange_partitions(
            Map<String, String> partitionSpecs,
            String sourceDb,
            String sourceTbl,
            String destDb,
            String destTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return clientDelegate.exchangePartitions(partitionSpecs, this.catalogId, sourceDb, sourceTbl, this.catalogId, destDb, destTbl);
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String srcCatalogId, String sourceDb, String sourceTbl, String dstCatalogId, String destDb, String destTbl) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return clientDelegate.exchangePartitions(partitionSpecs, srcCatalogId, sourceDb, sourceTbl, dstCatalogId, destDb, destTbl);
    }

    @Override
    public AggrStats getAggrColStatsFor(
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partName
    ) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getAggrColStatsFor(this.catalogId, dbName, tblName, colNames, partName);
    }

    @Override
    public AggrStats getAggrColStatsFor(String catalogId,
                                        String dbName,
                                        String tblName,
                                        List<String> colNames,
                                        List<String> partName) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getAggrColStatsFor(catalogId, dbName, tblName, colNames, partName);
    }

    @Override
    public List<String> getAllTables(String dbname)
            throws MetaException, TException, UnknownDBException {
        return getTables(dbname, ".*");
    }

    @Override
    public List<String> getAllTables(String catalogId, String dbName) throws MetaException, TException, UnknownDBException {
        return getTables(catalogId, dbName, ".*");
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
            throws TException, ConfigValSecurityException {
        if (name == null) {
            return defaultValue;
        }

        if (!Pattern.matches("(hive|hdfs|mapred).*", name)) {
            throw new ConfigValSecurityException(
                    "For security reasons, the config key " + name + " cannot be accessed");
        }

        return getConf().get(name, defaultValue);
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
            throws MetaException, TException {
        return clientDelegate.getDelegationToken(owner, renewerKerberosPrincipalName);
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws TException {
        return clientDelegate.getFields(this.catalogId, db, tableName);
    }

    @Override
    public List<FieldSchema> getFields(String catalogId, String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
        return clientDelegate.getFields(catalogId, db, tableName);
    }

    @Override
    public Function getFunction(String dbName, String functionName) throws MetaException, TException {
        try {
            return clientDelegate.getFunction(this.catalogId, dbName, functionName);
        } catch (NoSuchObjectException e) {
            // spark will check this error msg.
            if (e.getMessage().contains("Function not found")) {
                e.setMessage(String.format("Function %s.%s does not exist", dbName, functionName));
            }
            throw e;
        }
    }

    @Override
    public Function getFunction(String catalogId, String dbName, String funcName) throws MetaException, TException {
        try {
            return clientDelegate.getFunction(catalogId, dbName, funcName);
        } catch (NoSuchObjectException e) {
            // spark will check this error msg.
            if (e.getMessage().contains("Function not found")) {
                e.setMessage(String.format("Function %s.%s.%s does not exist", catalogId, dbName, funcName));
            }
            throw e;
        }
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return clientDelegate.getFunctions(this.catalogId, dbName, pattern);
    }

    @Override
    public List<String> getFunctions(String catalogId, String dbName, String pattern) throws MetaException, TException {
        return clientDelegate.getFunctions(catalogId, dbName, pattern);
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return clientDelegate.getAllFunctions(this.catalogId);
    }

//    @Override
//    public Index getIndex(String dbName, String tblName, String indexName)
//            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
//        return clientDelegate.getIndex(this.catalogId, dbName, tblName, indexName);
//    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        MetastoreConf.ConfVars metaConfVar = MetastoreConf.getMetaConf(key);
        if (metaConfVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        return getConf().get(key, metaConfVar.getDefaultVal().toString());
    }

    @Override
    public void createCatalog(Catalog catalog) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterCatalog(String catalogName, Catalog newCatalog) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Catalog getCatalog(String catalogId) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void dropCatalog(String catalogId) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> values)
            throws NoSuchObjectException, MetaException, TException {
        return getPartition(this.catalogId, dbName, tblName, values);
    }

    @Override
    public Partition getPartition(String catalogId, String dbName, String tblName, List<String> values) throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterPartition(clientDelegate.getPartition(catalogId, dbName, tblName, values));
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String partitionName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return getPartition(this.catalogId, dbName, tblName, partitionName);
    }

    @Override
    public Partition getPartition(String catalogId, String dbName, String tblName, String partitionName) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return filterHook.filterPartition(clientDelegate.getPartition(catalogId, dbName, tblName, partitionName));
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getPartitionColumnStatisticsObj(this.catalogId, dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String catalogId
            , String dbName
            , String tableName
            , List<String> partitionNames
            , List<String> columnNames) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getPartitionColumnStatisticsObj(catalogId, dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Partition getPartitionWithAuthInfo(
            String databaseName,
            String tableName,
            List<String> values,
            String userName,
            List<String> groupNames
    ) throws MetaException, UnknownTableException, NoSuchObjectException, TException {

        // TODO move this into the service
        Partition partition = getPartition(databaseName, tableName, values);
        Table table = getTable(databaseName, tableName);
        if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setDbName(databaseName);
            obj.setObjectName(tableName);
            obj.setPartValues(values);
            PrincipalPrivilegeSet privilegeSet =
                    this.get_privilege_set(obj, userName, groupNames);
            partition.setPrivileges(privilegeSet);
        }

        return filterHook.filterPartition(partition);
    }

    @Override
    public Partition getPartitionWithAuthInfo(String catalogId,
                                              String databaseName,
                                              String tableName,
                                              List<String> values,
                                              String userName,
                                              List<String> groupNames) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        // TODO move this into the service
        Partition partition = getPartition(catalogId, databaseName, tableName, values);
        Table table = getTable(catalogId, databaseName, tableName);
        if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setCatName(catalogId);
            obj.setDbName(databaseName);
            obj.setObjectName(tableName);
            obj.setPartValues(values);
            PrincipalPrivilegeSet privilegeSet =
                    this.get_privilege_set(obj, userName, groupNames);
            partition.setPrivileges(privilegeSet);
        }

        return filterHook.filterPartition(partition);
    }

    @Override
    public List<Partition> getPartitionsByNames(
            String databaseName,
            String tableName,
            List<String> partitionNames
    ) throws NoSuchObjectException, MetaException, TException {
        return getPartitionsByNames(this.catalogId, databaseName, tableName, partitionNames);
    }

    @Override
    public List<Partition> getPartitionsByNames(
            String catalogId
            , String databaseName,
            String tableName,
            List<String> partitionNames) throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterPartitions(clientDelegate.getPartitionsByNames(catalogId, databaseName, tableName, partitionNames));
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws TException {
        EnvironmentContext envCxt = null;
        String addedJars = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.ADDED_JARS);
        if (StringUtils.isNotBlank(addedJars)) {
            Map<String, String> props = new HashMap<>();
            props.put(HiveConf.ConfVars.HIVEADDEDJARS.varname, addedJars);
            envCxt = new EnvironmentContext(props);
        }
        return clientDelegate.getSchemaWithEnvironmentContext(this.catalogId, db, tableName, envCxt);
    }

    @Override
    public List<FieldSchema> getSchema(String catalogId, String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
        EnvironmentContext envCxt = null;
        String addedJars = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.ADDED_JARS);
        if (StringUtils.isNotBlank(addedJars)) {
            Map<String, String> props = new HashMap<>();
            props.put(HiveConf.ConfVars.HIVEADDEDJARS.varname, addedJars);
            envCxt = new EnvironmentContext(props);
        }
        return clientDelegate.getSchemaWithEnvironmentContext(catalogId, db, tableName, envCxt);
    }
    // it is removed by hive3
//    @Override
//    @Deprecated
//    public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
//        //this has been deprecated
//        return getTable(DEFAULT_DATABASE_NAME, tableName);
//    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        return getTable(this.catalogId, dbName, tableName);
    }

    @Override
    public Table getTable(String catalogId, String dbName, String tableName) throws MetaException, TException {
        try {
            return filterHook.filterTable(clientDelegate.getTable(this.catalogId, dbName, tableName));
        } catch (Exception e) {
            // by pass invalid resource for invalid database/table name input
            if (e.getMessage() != null && e.getMessage().contains("Invalid resource: acs:dlf")) {
                throw new NoSuchObjectException(e.getMessage());
            }

            throw e;
        }
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String dbName,
            String tableName,
            List<String> colNames
    ) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getTableColumnStatisticsObjs(this.catalogId, dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String catalogId
            , String dbName,
                                                              String tableName,
                                                              List<String> colNames) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getTableColumnStatisticsObjs(catalogId, dbName, tableName, colNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        List<Table> hiveTables = Lists.newArrayList();
        if (dbName == null || dbName.isEmpty()) {
            throw new UnknownDBException("DB name is null or empty");
        }
        if (tableNames == null) {
            throw new InvalidOperationException(dbName + " cannot find null tables");
        }
        try {
            getDatabase(dbName);
        } catch (NoSuchObjectException e) {
            throw new UnknownDBException("Could not find database " + dbName);
        }

        Set<String> tables = new HashSet<>(tableNames); //distinct
        try {
            return clientDelegate.getTableObjects(this.catalogId, dbName, new ArrayList<>(tables));
        } catch (NoSuchObjectException e) {
            logger.warn("Try to get a non exist table: " + e.getMessage());
        }
        return filterHook.filterTables(hiveTables);
    }

    @Override
    public List<Table> getTableObjectsByName(String catalogId, String dbName, List<String> tableNames) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        List<Table> hiveTables = Lists.newArrayList();
        if (dbName == null || dbName.isEmpty()) {
            throw new UnknownDBException("DB name is null or empty");
        }
        if (tableNames == null) {
            throw new InvalidOperationException(dbName + " cannot find null tables");
        }
        try {
            getDatabase(dbName);
        } catch (NoSuchObjectException e) {
            throw new UnknownDBException("Could not find database " + dbName);
        }

        Set<String> tables = new HashSet<>(tableNames); //distinct
        try {
            return clientDelegate.getTableObjects(this.catalogId, dbName, new ArrayList<>(tables));
        } catch (NoSuchObjectException e) {
            logger.warn("Try to get a non exist table: " + e.getMessage());
        }
        return filterHook.filterTables(hiveTables);
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList) throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm) throws MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCreationMetadata(String catalogId, String dbName, String tableName,
                                       CreationMetadata cm) throws MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        return getTables(this.catalogId, dbname, tablePattern, null);
    }

    @Override
    public List<String> getTables(String catalogId, String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
        return getTables(catalogId, dbname, tablePattern, null);
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return getTables(this.catalogId, dbname, tablePattern, tableType);
    }

    @Override
    public List<String> getTables(String catalogId, String dbname, String tablePattern, TableType tableType) throws MetaException, TException, UnknownDBException {
        return filterHook.filterTableNames(catalogId, dbname, clientDelegate.getTables(catalogId, dbname, tablePattern, tableType));
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String dbName) throws MetaException, TException, UnknownDBException {
        return getTables(dbName, "*", TableType.MATERIALIZED_VIEW);
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catalogId, String dbName) throws MetaException, TException, UnknownDBException {
        return getTables(catalogId, dbName, "*", TableType.MATERIALIZED_VIEW);
    }

    @Override
    public List<TableMeta> getTableMeta(
            String dbPatterns,
            String tablePatterns,
            List<String> tableTypes
    ) throws MetaException, TException, UnknownDBException, UnsupportedOperationException {
        return getTableMeta(this.catalogId, dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public List<TableMeta> getTableMeta(String catalogId, String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException, TException, UnknownDBException {
        return filterHook.filterTableMetas(clientDelegate.getTableMeta(catalogId, dbPatterns, tablePatterns, tableTypes));
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return clientDelegate.getValidTxns();
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return clientDelegate.getValidTxns(currentTxn);
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
                                                   String userName, List<String> groupNames)
            throws MetaException, TException {
        return clientDelegate.getPrivilegeSet(hiveObject, userName, groupNames);
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges)
            throws MetaException, TException {
        return clientDelegate.grantPrivileges(privileges);
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption)
            throws MetaException, TException {
        return clientDelegate.revokePrivileges(privileges, grantOption);
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
                                      PrivilegeBag grantPrivileges) throws MetaException, TException {
        throw new UnsupportedOperationException();
        //return false;
    }

    @Override
    public void heartbeat(long txnId, long lockId)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        clientDelegate.heartbeat(txnId, lockId);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return clientDelegate.heartbeatTxnRange(min, max);
    }

//    @Override
//    public boolean isCompatibleWith(HiveConf conf) {
//        if (currentMetaVars == null) {
//            return false; // recreate
//        }
//        boolean compatible = true;
//        for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
//            // Since metaVars are all of different types, use string for comparison
//            String oldVar = currentMetaVars.get(oneVar.varname);
//            String newVar = conf.get(oneVar.varname, "");
//            if (oldVar == null ||
//                    (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
//                logger.info("Mestastore configuration " + oneVar.varname +
//                        " changed from " + oldVar + " to " + newVar);
//                compatible = false;
//            }
//        }
//        return compatible;
//    }

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        Map<String, String> currentMetaVarsCopy = currentMetaVars;
        if (currentMetaVarsCopy == null) {
            return false; // recreate
        }
        boolean compatible = true;
        for (MetastoreConf.ConfVars oneVar : MetastoreConf.metaVars) {
            // Since metaVars are all of different types, use string for comparison
            String oldVar = currentMetaVarsCopy.get(oneVar.getVarname());
            String newVar = MetastoreConf.getAsString(configuration, oneVar);
            if (oldVar == null ||
                    (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
                logger.info("Mestastore configuration " + oneVar.toString() +
                        " changed from " + oldVar + " to " + newVar);
                compatible = false;
            }
        }
        return compatible;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {
        //taken from HiveMetaStoreClient
        MetastoreConf.setVar(getConf(), MetastoreConf.ConfVars.ADDED_JARS, addedJars);
    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    private void snapshotActiveConf() {
        currentMetaVars = new HashMap<>(MetastoreConf.metaVars.length);
        for (MetastoreConf.ConfVars oneVar : MetastoreConf.metaVars) {
            currentMetaVars.put(oneVar.getVarname(), MetastoreConf.getAsString(this.conf, oneVar));
        }
    }

    @Override
    public boolean isPartitionMarkedForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs, PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return clientDelegate.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
    }

    @Override
    public boolean isPartitionMarkedForEvent(String catalogId, String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        throw new UnsupportedOperationException();
        //return false;
    }

//    @Override
//    public List<String> listIndexNames(
//            String dbName,
//            String tblName,
//            short max
//    ) throws MetaException, TException {
//        // In current hive implementation, it ignores fields "max"
//        // https://github.com/apache/hive/blob/rel/release-2.3
//        // .0/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L3902-L3932
//        List<Index> indexes = listIndexes(dbName, tblName, max);
//        List<String> indexNames = Lists.newArrayList();
//        for (Index index : indexes) {
//            indexNames.add(index.getIndexName());
//        }
//
//        return indexNames;
//    }
//
//    @Override
//    public List<Index> listIndexes(String db_name, String tbl_name, short max)
//            throws NoSuchObjectException, MetaException,
//            TException {
//        // In current hive implementation, it ignores fields "max"
//        // https://github.com/apache/hive/blob/rel/release-2.3
//        // .0/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L3867-L3899
//        return clientDelegate.listIndexes(this.catalogId, db_name, tbl_name);
//    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        try {
            return listPartitionNames(dbName, tblName, Lists.newArrayList(), max);
        } catch (NoSuchObjectException e) {
            // For compatibility with Hive 1.0.0
            return Collections.emptyList();
        }
    }

    @Override
    public List<String> listPartitionNames(String catalogId, String dbName, String tblName, int max) throws NoSuchObjectException, MetaException, TException {
        try {
            return listPartitionNames(catalogId, dbName, tblName, Lists.newArrayList(), max);
        } catch (NoSuchObjectException e) {
            // For compatibility with Hive 1.0.0
            return Collections.emptyList();
        }
    }

    @Override
    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws MetaException, TException, NoSuchObjectException {
        return listPartitionNames(this.catalogId, databaseName, tableName, values, max);
    }

    @Override
    public List<String> listPartitionNames(String catalogId, String databaseName,
                                           String tableName,
                                           List<String> values,
                                           int max) throws MetaException, TException, NoSuchObjectException {
        return filterHook.filterPartitionNames(catalogId, databaseName, tableName,
                clientDelegate.listPartitionNames(catalogId, databaseName, tableName, values, max));
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest) throws MetaException, TException, NoSuchObjectException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public int getNumPartitionsByFilter(
            String dbName,
            String tableName,
            String filter
    ) throws MetaException, NoSuchObjectException, TException {
        return clientDelegate.getNumPartitionsByFilter(this.catalogId, dbName, tableName, filter);
    }

    @Override
    public int getNumPartitionsByFilter(
            String catalogId
            , String dbName
            , String tableName
            , String filter) throws MetaException, NoSuchObjectException, TException {
        return clientDelegate.getNumPartitionsByFilter(catalogId, dbName, tableName, filter);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(
            String dbName,
            String tblName,
            int max
    ) throws TException {
        return listPartitionSpecs(this.catalogId, dbName, tblName, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(
            String catalogId
            , String dbName
            , String tblName
            , int max) throws TException {
        return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
                clientDelegate.listPartitionSpecs(catalogId, dbName, tblName, max)));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws MetaException, NoSuchObjectException, TException {
        return listPartitionSpecsByFilter(this.catalogId, dbName, tblName, filter, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(
            String catalogId
            , String dbName
            , String tblName
            , String filter
            , int max) throws MetaException, NoSuchObjectException, TException {
        return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
                clientDelegate.listPartitionSpecsByFilter(catalogId, dbName, tblName, filter, max)));
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return listPartitions(dbName, tblName, Lists.newArrayList(), max);
    }

    @Override
    public List<Partition> listPartitions(String catalogId, String dbName, String tblName, int max) throws NoSuchObjectException, MetaException, TException {
        return listPartitions(catalogId, dbName, tblName, Lists.newArrayList(), max);
    }

    @Override
    public List<Partition> listPartitions(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws NoSuchObjectException, MetaException, TException {
        return listPartitions(this.catalogId, databaseName, tableName, values, max);
    }

    @Override
    public List<Partition> listPartitions(String catalogId, String databaseName, String tableName, List<String> values, int max) throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterPartitions(clientDelegate.listPartitions(catalogId, databaseName, tableName, values, max));
    }

    @Override
    public boolean listPartitionsByExpr(
            String databaseName,
            String tableName,
            byte[] expr,
            String defaultPartitionName,
            short max,
            List<Partition> result
    ) throws TException {
        checkNotNull(result, "The result argument cannot be null.");
        return listPartitionsByExpr(this.catalogId, databaseName, tableName, expr,
                defaultPartitionName, max, result);
    }

    @Override
    public boolean listPartitionsByExpr(String catalogId, String databaseName,
                                        String tableName,
                                        byte[] expr,
                                        String defaultPartitionName,
                                        int max,
                                        List<Partition> result) throws TException {
        checkNotNull(result, "The result argument cannot be null.");
        List<Partition> tmpResult = Lists.newArrayListWithExpectedSize(result.size());
        boolean hasUnknownPartitions = clientDelegate.listPartitionsByExpr(catalogId, databaseName, tableName, expr,
                defaultPartitionName, max, tmpResult);
        result.addAll(filterHook.filterPartitions(tmpResult));
        return hasUnknownPartitions;
    }

    @Override
    public List<Partition> listPartitionsByFilter(
            String databaseName,
            String tableName,
            String filter,
            short max
    ) throws MetaException, NoSuchObjectException, TException {
        return listPartitionsByFilter(this.catalogId, databaseName, tableName, filter, max);
    }

    @Override
    public List<Partition> listPartitionsByFilter(
            String catalogId
            , String databaseName
            , String tableName
            , String filter
            , int max) throws MetaException, NoSuchObjectException, TException {
        return filterHook.filterPartitions(
                clientDelegate.listPartitionsByFilter(catalogId, databaseName, tableName, filter, max));
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String database,
            String table,
            short maxParts,
            String user,
            List<String> groups
    ) throws MetaException, TException, NoSuchObjectException {
        List<Partition> partitions = listPartitions(database, table, maxParts);

        for (Partition p : partitions) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setDbName(database);
            obj.setObjectName(table);
            obj.setPartValues(p.getValues());
            PrincipalPrivilegeSet set = this.get_privilege_set(obj, user, groups);
            p.setPrivileges(set);
        }

        return filterHook.filterPartitions(partitions);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String catalogId
            , String database
            , String table
            , int maxParts
            , String user
            , List<String> groups) throws MetaException, TException, NoSuchObjectException {
        List<Partition> partitions = listPartitions(catalogId, database, table, maxParts);

        for (Partition p : partitions) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setCatName(catalogId);
            obj.setDbName(database);
            obj.setObjectName(table);
            obj.setPartValues(p.getValues());
            PrincipalPrivilegeSet set = this.get_privilege_set(obj, user, groups);
            p.setPrivileges(set);
        }

        return filterHook.filterPartitions(partitions);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String database,
            String table,
            List<String> partVals,
            short maxParts,
            String user,
            List<String> groups
    ) throws MetaException, TException, NoSuchObjectException {
        List<Partition> partitions = listPartitions(database, table, partVals, maxParts);

        for (Partition p : partitions) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setDbName(database);
            obj.setObjectName(table);
            obj.setPartValues(p.getValues());
            PrincipalPrivilegeSet set;
            try {
                set = get_privilege_set(obj, user, groups);
            } catch (MetaException e) {
                String msg = String.format("No privileges found for user: %s, groups: %s", user,
                        String.join(",", groups));
                logger.error(msg, e);
                set = new PrincipalPrivilegeSet();
            }
            p.setPrivileges(set);
        }

        return filterHook.filterPartitions(partitions);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(String catalogId, String database, String table, List<String> partVals, int maxParts, String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
        List<Partition> partitions = listPartitions(catalogId, database, table, partVals, maxParts);

        for (Partition p : partitions) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setCatName(catalogId);
            obj.setDbName(database);
            obj.setObjectName(table);
            obj.setPartValues(p.getValues());
            PrincipalPrivilegeSet set;
            try {
                set = get_privilege_set(obj, user, groups);
            } catch (MetaException e) {
                String msg = String.format("No privileges found for user: %s, groups: %s", user,
                        String.join(",", groups));
                logger.error(msg, e);
                set = new PrincipalPrivilegeSet();
            }
            p.setPrivileges(set);
        }

        return filterHook.filterPartitions(partitions);
    }

    @Override
    public List<String> listTableNamesByFilter(
            String dbName,
            String filter,
            short maxTables
    ) throws MetaException, TException, InvalidOperationException, UnknownDBException, UnsupportedOperationException {
        return listTableNamesByFilter(this.catalogId, dbName, filter, maxTables);
    }

    @Override
    public List<String> listTableNamesByFilter(String catalogId, String dbName, String filter, int maxTables) throws TException, InvalidOperationException, UnknownDBException {
        return filterHook.filterTableNames(catalogId, dbName, clientDelegate.listTableNamesByFilter(catalogId, dbName, filter, maxTables));
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(
            String principal,
            PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws MetaException, TException {
        return clientDelegate.listPrivileges(principal, principalType, objectRef);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return clientDelegate.lock(this.catalogId, lockRequest);
    }

    @Override
    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        clientDelegate.markPartitionForEvent(dbName, tblName, partKVs, eventType);
    }

    @Override
    public void markPartitionForEvent(
            String catalogId
            , String dbName
            , String tblName
            , Map<String, String> partKVs
            , PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long openTxn(String user) throws TException {
        return clientDelegate.openTxn(user);
    }

    @Override
    public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        return clientDelegate.openTxns(user, numTxns);
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        // Lifted from HiveMetaStore
        if (name.length() == 0) {
            return new HashMap<String, String>();
        }
        return Warehouse.makeSpecFromName(name);
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return clientDelegate.partitionNameToVals(name);
    }

    @Override
    public void reconnect() throws MetaException {
        snapshotActiveConf();
    }

    @Override
    public void renamePartition(
            String dbName,
            String tblName,
            List<String> partitionValues,
            Partition newPartition
    ) throws InvalidOperationException, MetaException, TException {
        clientDelegate.renamePartition(this.catalogId, dbName, tblName, partitionValues, newPartition);
    }

    @Override
    public void renamePartition(String catalogId, String dbName, String tblName, List<String> partitionValues, Partition newPartition) throws InvalidOperationException, MetaException, TException {
        clientDelegate.renamePartition(catalogId, dbName, tblName, partitionValues, newPartition);
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return clientDelegate.renewDelegationToken(tokenStrForm);
    }

    @Override
    public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
        clientDelegate.rollbackTxn(txnId);
    }

    @Override
    public void replRollbackTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        MetastoreConf.ConfVars confVar = MetastoreConf.getMetaConf(key);
        if (confVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        try {
            confVar.validate(value);
        } catch (IllegalArgumentException e) {
            throw DataLakeUtil.throwException(new MetaException("Invalid configuration value " + value + " for key " + key +
                    " by " + e.getMessage()), e);
        }
        getConf().set(key, value);
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, InvalidInputException {
        return clientDelegate.setPartitionColumnStatistics(this.catalogId, request);
    }

    @Override
    public void flushCache() {
        //no op
    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        return clientDelegate.getFileMetadata(fileIds);
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters
    ) throws TException {
        return clientDelegate.getFileMetadataBySarg(fileIds, sarg, doGetFooters);
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
        clientDelegate.clearFileMetadata(fileIds);
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        clientDelegate.putFileMetadata(fileIds, metadata);
    }

    @Override
    public boolean isSameConfObj(Configuration configuration) {
        return getConf() == configuration;
    }


    @Override
    public boolean cacheFileMetadata(
            String dbName,
            String tblName,
            String partName,
            boolean allParts
    ) throws TException {
        return clientDelegate.cacheFileMetadata(dbName, tblName, partName, allParts);
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        return new ArrayList<SQLPrimaryKey>();
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        return new ArrayList<SQLForeignKey>();
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest uniqueConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return new ArrayList<SQLUniqueConstraint>();
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest notNullConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return new ArrayList<SQLNotNullConstraint>();
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest defaultConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return new ArrayList<SQLDefaultConstraint>();
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest checkConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
        return new ArrayList<SQLCheckConstraint>();
    }

    @Override
    public void createTableWithConstraints(Table tbl,
                                           List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
                                           List<SQLUniqueConstraint> uniqueConstraints,
                                           List<SQLNotNullConstraint> notNullConstraints,
                                           List<SQLDefaultConstraint> defaultConstraints,
                                           List<SQLCheckConstraint> checkConstraints) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException();
    }

//    @Override
//    public void createTableWithConstraints(
//            Table table,
//            List<SQLPrimaryKey> primaryKeys,
//            List<SQLForeignKey> foreignKeys
//    ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
//        clientDelegate.createTableWithConstraints(table, primaryKeys, foreignKeys);
//    }

    @Override
    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws MetaException, NoSuchObjectException, TException {
        clientDelegate.dropConstraint(dbName, tblName, constraintName);
    }

    @Override
    public void dropConstraint(String catalogId, String dbName, String tableName, String constraintName) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        clientDelegate.addPrimaryKey(primaryKeyCols);
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        clientDelegate.addForeignKey(foreignKeyCols);
    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String copyFromName) throws InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WMFullResourcePlan getResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void dropResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan wmNullableResourcePlan, boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger) throws InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropWMTrigger(String resourcePlanName, String triggerName) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void createWMPool(WMPool wmPool) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterWMPool(WMNullablePool wmNullablePool, String poolPath) throws NoSuchObjectException, InvalidObjectException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropWMPool(String resourcePlanName, String poolPath) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean isUpdate) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropWMMapping(WMMapping wmMapping) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
                                                 String poolPath, boolean shouldDrop) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createISchema(ISchema iSchema) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISchema getISchema(String catalogId, String dbName, String name) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void dropISchema(String catalogId, String dbName, String name) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaVersion getSchemaVersion(String catalogId, String dbName, String schemaName, int version) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String catalogId, String dbName, String schemaName) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String catalogId, String dbName, String schemaName) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void dropSchemaVersion(String catalogId, String dbName, String schemaName, int version) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst findSchemasByColsRqst) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public void mapSchemaVersionToSerde(String catalogId, String dbName, String schemaName, int version, String serdeName) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchemaVersionState(String catalogId, String dbName, String schemaName, int version, SchemaVersionState state) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SerDeInfo getSerDe(String serDeName) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        throw new UnsupportedOperationException();
        //return false;
    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return clientDelegate.showCompactions();
    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
                                     List<String> partNames) throws TException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
                                     List<String> partNames, DataOperationType operationType) throws TException {
        throw new UnsupportedOperationException();
    }

//    @Override
//    @Deprecated
//    public void addDynamicPartitions(
//            long txnId,
//            String dbName,
//            String tblName,
//            List<String> partNames
//    ) throws TException {
//        clientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames, null);
//    }
//
//    @Override
//    public void addDynamicPartitions(
//            long txnId,
//            String dbName,
//            String tblName,
//            List<String> partNames,
//            DataOperationType operationType
//    ) throws TException {
//        clientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames, operationType);
//    }

    @Override
    public void insertTable(Table table, boolean overwrite) throws MetaException {
        clientDelegate.insertTable(table, overwrite);
    }

    @Override
    public NotificationEventResponse getNextNotification(
            long lastEventId,
            int maxEvents,
            NotificationFilter notificationFilter
    ) throws TException {
        return clientDelegate.getNextNotification(lastEventId, maxEvents, notificationFilter);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return clientDelegate.getCurrentNotificationEventId();
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
        throw new UnsupportedOperationException();
        //return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return clientDelegate.fireListenerEvent(fireEventRequest);
    }

    @Override
    @Deprecated
    public ShowLocksResponse showLocks() throws TException {
        return clientDelegate.showLocks();
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return clientDelegate.showLocks(showLocksRequest);
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return clientDelegate.showTxns();
    }

//    @Override
//    @Deprecated
//    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
//        //this method has been deprecated;
//        return tableExists(DEFAULT_DATABASE_NAME, tableName);
//    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        return tableExists(this.catalogId, databaseName, tableName);
    }

    @Override
    public boolean tableExists(String catalogId, String databaseName, String tableName) throws MetaException, TException, UnknownDBException {
        checkNotNull(tableName);
        checkNotNull(catalogId);
        checkNotNull(databaseName);
        try {
            Table result = getTable(databaseName, tableName);
            return result != null;
        } catch (NoSuchObjectException e) {
            return false;
        }
    }

    @Override
    public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
        clientDelegate.unlock(lockId);
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return clientDelegate.updatePartitionColumnStatistics(this.catalogId, columnStatistics);
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return clientDelegate.updateTableColumnStatistics(this.catalogId, columnStatistics);
    }

    @Override
    public void validatePartitionNameCharacters(List<String> part_vals) throws TException, MetaException {
        try {
            MetaStoreUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);
        } catch (Exception e) {
            if (e instanceof MetaException) {
                throw (MetaException) e;
            } else {
                throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
            }
        }
    }
}
