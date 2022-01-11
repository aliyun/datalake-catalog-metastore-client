package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.hive.common.converters.CatalogToHiveConverter;
import com.aliyun.datalake.metastore.hive.common.converters.HiveToCatalogConverter;
import com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.aliyun.datalake.metastore.hive.shims.ShimsLoader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

/**
 * Data Lake Formation MetaStore Client for Hive.
 */
public class DlfMetaStoreClient implements IMetaStoreClient {

    private static final Logger logger = LoggerFactory.getLogger(DlfMetaStoreClient.class);
    private static final String INDEX_PREFIX = "meta_index_prefix";
    private static volatile Boolean isCreatedDefaultDatabase;
    protected final HiveConf conf;
    private final Warehouse warehouse;
    private final DlfMetaStoreClientDelegate clientDelegate;
    private final String catalogId;
    private final IHiveShims hiveShims = ShimsLoader.getHiveShims();
    private final Boolean enableFsOperation;
    private Map<String, String> currentMetaVars;
    private final MetaStoreFilterHook filterHook;

    public DlfMetaStoreClient(HiveConf conf) throws MetaException {
        this(conf, null);
    }

    public DlfMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
        long startTime = System.currentTimeMillis();
        logger.info("DlfMetaStoreClient start");
        this.conf = new HiveConf(conf);
        this.catalogId = ConfigUtils.getCatalogId(this.conf);
        this.warehouse = new Warehouse(this.conf);
        snapshotActiveConf();
        this.filterHook = loadFilterHooks();
        this.clientDelegate = new DlfMetaStoreClientDelegate(this.conf, warehouse, hookLoader);

        boolean createDefaultDatabase = conf.getBoolean(DataLakeConfig.CATALOG_CREATE_DEFAULT_DB, true);
        createDefaultCatalogAndDatabase(createDefaultDatabase);
        this.enableFsOperation = clientDelegate.getEnableFsOperation();
        logger.info("DlfMetaStoreClient end, cost:{}ms", System.currentTimeMillis() - startTime);

    }


    private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
        Class<? extends MetaStoreFilterHook> authProviderClass = conf.
                getClass(HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname,
                        DefaultMetaStoreFilterHookImpl.class,
                        MetaStoreFilterHook.class);
        String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
        try {
            Constructor<? extends MetaStoreFilterHook> constructor =
                    authProviderClass.getConstructor(HiveConf.class);
            return constructor.newInstance(conf);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (SecurityException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (InstantiationException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(msg + e.getMessage(), e);
        }
    }

    public Boolean getEnableFsOperation() {
        return enableFsOperation;
    }

    public void createDefaultCatalogAndDatabase(
            boolean ifCreateDefaultDatabase) throws MetaException {
        if (ifCreateDefaultDatabase) {
            if (isCreatedDefaultDatabase == null || isCreatedDefaultDatabase == false) {
                synchronized (DlfMetaStoreClient.class) {
                    if (isCreatedDefaultDatabase == null || isCreatedDefaultDatabase == false) {
                        logger.info("dlf metastoreclient create default database ");
                        createDefaultDatabaseIfNotExists();
                        isCreatedDefaultDatabase = true;
                    }
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
            clientDelegate.getDatabase(this.catalogId, DEFAULT_DATABASE_NAME);
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
        defaultDB.setName(DEFAULT_DATABASE_NAME);
        defaultDB.setDescription(DEFAULT_DATABASE_COMMENT);
        defaultDB.setParameters(Collections.EMPTY_MAP);
        defaultDB.setLocationUri(warehouse.getDefaultDatabasePath(DEFAULT_DATABASE_NAME).toString());
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
        return filterHook.filterDatabase(clientDelegate.getDatabase(this.catalogId, name));
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return filterHook.filterDatabases(clientDelegate.getDatabases(this.catalogId, pattern));
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases(".*");
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws NoSuchObjectException, MetaException, TException {
        clientDelegate.alterDatabase(this.catalogId, databaseName, database);
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
        return filterHook.filterPartitions(
                clientDelegate.addPartitions(this.catalogId, partitions, ifNotExists, needResult));
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
    public void alter_index(String dbName, String tblName, String indexName, Index index)
            throws InvalidOperationException, MetaException, TException {
        alterIndex(this.catalogId, dbName, tblName, indexName, index);
    }

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
    public void alter_table(String dbName, String tblName, Table table)
            throws InvalidOperationException, MetaException, TException {
        clientDelegate.alterTable(this.catalogId, dbName, tblName, table, null);
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
    public Partition appendPartition(String dbName, String tblName, String partitionName)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return clientDelegate.appendPartition(this.catalogId, dbName, tblName, partitionName);
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
    public void abortTxns(List<Long> txnIds) throws TException {
        clientDelegate.abortTxns(txnIds);
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

    @Override
    public void createIndex(Index index, Table indexTable)
            throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {
        createIndex(this.catalogId, index, indexTable);
    }

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
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, InvalidInputException {
        return clientDelegate.deleteTableColumnStatistics(this.catalogId, dbName, tableName, colName);
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
            InvalidObjectException, InvalidInputException, TException {
        clientDelegate.dropFunction(this.catalogId, dbName, functionName);
    }

    @Override
    public boolean dropIndex(String dbName, String tblName, String name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return dropIndex(this.catalogId, dbName, tblName, name, deleteData);
    }

    private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
        if (depth > 0 && parent != null && warehouse.isWritable(parent) && warehouse.isEmpty(parent)) {
            warehouse.deleteDir(parent, true, mustPurge);
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
    public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options)
            throws TException {
        return clientDelegate.dropPartition(this.catalogId, dbName, tblName, values,
                options.ifExists, options.deleteData, options.purgeData);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        List<String> values = partitionNameToVals(partitionName);
        return clientDelegate.dropPartition(this.catalogId, dbName, tblName, values,
                false, deleteData, false);
    }

    @Override
    public List<Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean ifExists
    ) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartitions(this.catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                deleteData, false, true);
    }

    @Override
    public List<Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean ifExists,
            boolean needResults
    ) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.dropPartitions(this.catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                deleteData, false, needResults);
    }

    @Override
    public List<Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            PartitionDropOptions options
    ) throws TException {
        return clientDelegate.dropPartitions(this.catalogId, dbName, tblName, hiveShims.objectPairConvert(partExprs),
                options.deleteData, options.purgeData, options.returnResults);
    }
    // ==================== Index ========================================

    public void createIndex(
            String catalogId,
            Index index,
            Table indexTable
    ) throws TException {
        boolean dirCreated = clientDelegate.validateNewTableAndCreateDirectory(catalogId, indexTable);
        boolean indexTableCreated = false;
        String dbName = index.getDbName();
        String indexTableName = index.getIndexTableName();
        String originTableName = index.getOrigTableName();
        Path indexTablePath = clientDelegate.getTablePath(catalogId, indexTable);

        String indexTableObjectName = INDEX_PREFIX + index.getIndexName();

        try {
            Table originTable = clientDelegate.getTable(catalogId, dbName, originTableName);
            Map<String, String> parameters = originTable.getParameters();
            if (parameters.containsKey(indexTableObjectName)) {
                throw new AlreadyExistsException("Index: " + index.getIndexName() + " already exist");
            }
            clientDelegate.createTable(catalogId, indexTable, null);
            indexTableCreated = true;
            String indexInString = toIndexString(index);
            originTable.getParameters().put(indexTableObjectName, indexInString);
            clientDelegate.alterTable(catalogId, dbName, originTableName, originTable, null);
        } catch (Exception e) {
            if (dirCreated) {
                hiveShims.deleteDir(warehouse, indexTablePath, true, enableFsOperation);
            }
            if (indexTableCreated) {
                EnvironmentContext environmentContext = new EnvironmentContext();
                environmentContext.putToProperties("isIndexDelete","true");
                clientDelegate.dropTable(catalogId, dbName, indexTableName, true, true, environmentContext);
            }
            String msg = "Unable to create index: ";
            if (e instanceof TException) {
                throw e;
            } else {
                throw DataLakeUtil.throwException(new MetaException(msg + e), e);
            }
        }
    }

    private String toIndexString(Index index) throws MetaException {
        com.aliyun.datalake20200710.models.Table catalogIndexTableObject =
                HiveToCatalogConverter.convertIndexToCatalogTableObject(index);
        return catalogTableToString(catalogIndexTableObject);
    }

    public void alterIndex(
            String catalogId,
            String dbName,
            String tblName,
            String indexName,
            Index index
    ) throws TException {
        com.aliyun.datalake20200710.models.Table catalogIndexTableObject =
                HiveToCatalogConverter.convertIndexToCatalogTableObject(index);
        Table originTable = clientDelegate.getTable(catalogId, dbName, tblName);
        String indexTableObjectName = INDEX_PREFIX + indexName;
        if (!originTable.getParameters().containsKey(indexTableObjectName)) {
            throw new NoSuchObjectException("can not find index: " + indexName);
        }
        String indexInString = catalogTableToString(catalogIndexTableObject);
        originTable.getParameters().put(indexTableObjectName, indexInString);
        clientDelegate.alterTable(catalogId, dbName, tblName, originTable, null);
    }

    public Index getIndex(
            String catalogId,
            String dbName,
            String tblName,
            String indexName
    ) throws TException {
        Table originTable = clientDelegate.getTable(catalogId, dbName, tblName);
        Map<String, String> map = originTable.getParameters();
        String indexTableName = INDEX_PREFIX + indexName;
        if (!map.containsKey(indexTableName)) {
            throw new NoSuchObjectException("can not find index: " + indexName);
        }
        com.aliyun.datalake20200710.models.Table indexTableObject = null;
        try {
            indexTableObject = clientDelegate.stringToCatalogTable(map.get(indexTableName));
        } catch (IOException e) {
            throw DataLakeUtil.throwException(new MetaException("get index failed in conversion json to index object" + e.getMessage()), e);
        }
        return filterHook.filterIndex(CatalogToHiveConverter.ToHiveIndex(indexTableObject));
    }

    public boolean dropIndex(
            String catalogId,
            String dbName,
            String tblName,
            String indexName,
            boolean deleteData
    ) throws TException {
        Index indexToDrop = getIndex(catalogId, dbName, tblName, indexName);
        String indexTableName = indexToDrop.getIndexTableName();

        // Drop the index metadata
        Table originTable = clientDelegate.getTable(catalogId, dbName, tblName);
        Map<String, String> parameters = originTable.getParameters();
        String indexTableObjectName = INDEX_PREFIX + indexName;
        if (!parameters.containsKey(indexTableObjectName)) {
            throw new NoSuchObjectException("can not find Index: " + indexName);
        }
        parameters.remove(indexTableObjectName);

        clientDelegate.alterTable(catalogId, dbName, tblName, originTable, null);

        // Now drop the data associated with the table used to hold the index data
        if (indexTableName != null && indexTableName.length() > 0) {
            EnvironmentContext environmentContext = new EnvironmentContext();
            environmentContext.putToProperties("isIndexDelete","true");
            clientDelegate.dropTable(catalogId, dbName, indexTableName, deleteData, true, environmentContext);
        }
        return true;
    }

    private String catalogTableToString(com.aliyun.datalake20200710.models.Table catalogIndexTableObject)
            throws MetaException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(catalogIndexTableObject);
        } catch (JsonProcessingException e) {
            throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
        }
    }

    public List<Index> listIndexes(String catalogId, String dbName, String tblName) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");

        Table originTable = clientDelegate.getTable(catalogId, dbName, tblName);
        return clientDelegate.getIndicesInTable(originTable);
    }

    //=====================index end=======================
    @Override
    @Deprecated
    public void dropTable(String tableName, boolean deleteData)
            throws MetaException, UnknownTableException, TException, NoSuchObjectException {
        clientDelegate.dropTable(this.catalogId, DEFAULT_DATABASE_NAME, tableName, deleteData, false, null);
    }

    @Override
    public void dropTable(String dbname, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        clientDelegate.dropTable(this.catalogId, dbname, tableName, true, true, null);
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
    public AggrStats getAggrColStatsFor(
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partName
    ) throws NoSuchObjectException, MetaException, TException {
        return clientDelegate.getAggrColStatsFor(this.catalogId, dbName, tblName, colNames, partName);
    }

    @Override
    public List<String> getAllTables(String dbname)
            throws MetaException, TException, UnknownDBException {
        return getTables(dbname, ".*");
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

        return conf.get(name, defaultValue);
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
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return clientDelegate.getFunctions(this.catalogId, dbName, pattern);
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return clientDelegate.getAllFunctions(this.catalogId);
    }

    @Override
    public Index getIndex(String dbName, String tblName, String indexName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return getIndex(this.catalogId, dbName, tblName, indexName);
    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        ConfVars metaConfVar = HiveConf.getMetaConf(key);
        if (metaConfVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        return conf.get(key, metaConfVar.getDefaultValue());
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> values)
            throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterPartition(clientDelegate.getPartition(this.catalogId, dbName, tblName, values));
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String partitionName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return filterHook.filterPartition(clientDelegate.getPartition(this.catalogId, dbName, tblName, partitionName));
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
    public List<Partition> getPartitionsByNames(
            String databaseName,
            String tableName,
            List<String> partitionNames
    ) throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterPartitions(clientDelegate.getPartitionsByNames(this.catalogId, databaseName, tableName, partitionNames));
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws TException {
        EnvironmentContext envCxt = null;
        String addedJars = conf.getVar(HiveConf.ConfVars.HIVEADDEDJARS);
        if (StringUtils.isNotBlank(addedJars)) {
            Map<String, String> props = new HashMap<>();
            props.put(HiveConf.ConfVars.HIVEADDEDJARS.varname, addedJars);
            envCxt = new EnvironmentContext(props);
        }
        return clientDelegate.getSchemaWithEnvironmentContext(this.catalogId, db, tableName, envCxt);
    }

    @Override
    @Deprecated
    public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
        //this has been deprecated
        return getTable(DEFAULT_DATABASE_NAME, tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
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
    public List<String> getTables(String dbname, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        return getTables(dbname, tablePattern, null);
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return filterHook.filterTableNames(dbname, clientDelegate.getTables(this.catalogId, dbname, tablePattern, tableType));
    }

    @Override
    public List<TableMeta> getTableMeta(
            String dbPatterns,
            String tablePatterns,
            List<String> tableTypes
    ) throws MetaException, TException, UnknownDBException, UnsupportedOperationException {
        return filterNames(clientDelegate.getTableMeta(this.catalogId, dbPatterns, tablePatterns, tableTypes));
    }

    private List<TableMeta> filterNames(List<TableMeta> metas) throws MetaException {
        Map<String, TableMeta> sources = new LinkedHashMap<>();
        Map<String, List<String>> dbTables = new LinkedHashMap<>();
        for (TableMeta meta : metas) {
            sources.put(meta.getDbName() + "." + meta.getTableName(), meta);
            List<String> tables = dbTables.computeIfAbsent(meta.getDbName(), k -> new ArrayList<>());
            tables.add(meta.getTableName());
        }
        List<TableMeta> filtered = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : dbTables.entrySet()) {
            for (String table : filterHook.filterTableNames(entry.getKey(), entry.getValue())) {
                filtered.add(sources.get(entry.getKey() + "." + table));
            }
        }
        return filtered;
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
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef obj, String user, List<String> groups)
            throws MetaException, TException {
        return clientDelegate.getPrivilegeSet(obj, user, groups);
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
    public void heartbeat(long txnId, long lockId)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        clientDelegate.heartbeat(txnId, lockId);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return clientDelegate.heartbeatTxnRange(min, max);
    }

    @Override
    public boolean isCompatibleWith(HiveConf conf) {
        if (currentMetaVars == null) {
            return false; // recreate
        }
        boolean compatible = true;
        for (ConfVars oneVar : HiveConf.metaVars) {
            // Since metaVars are all of different types, use string for comparison
            String oldVar = currentMetaVars.get(oneVar.varname);
            String newVar = conf.get(oneVar.varname, "");
            if (oldVar == null ||
                    (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
                logger.info("Mestastore configuration " + oneVar.varname +
                        " changed from " + oldVar + " to " + newVar);
                compatible = false;
            }
        }
        return compatible;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {
        //taken from HiveMetaStoreClient
        HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    private void snapshotActiveConf() {
        currentMetaVars = new HashMap<>(HiveConf.metaVars.length);
        for (ConfVars oneVar : HiveConf.metaVars) {
            currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
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
    public List<String> listIndexNames(
            String dbName,
            String tblName,
            short max
    ) throws MetaException, TException {
        // In current hive implementation, it ignores fields "max"
        // https://github.com/apache/hive/blob/rel/release-2.3
        // .0/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L3902-L3932
        List<Index> indexes = listIndexes(dbName, tblName, max);
        List<String> indexNames = Lists.newArrayList();
        for (Index index : indexes) {
            indexNames.add(index.getIndexName());
        }

        return filterHook.filterIndexNames(dbName, tblName, indexNames);
    }

    @Override
    public List<Index> listIndexes(String db_name, String tbl_name, short max)
            throws NoSuchObjectException, MetaException,
            TException {
        // In current hive implementation, it ignores fields "max"
        // https://github.com/apache/hive/blob/rel/release-2.3
        // .0/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L3867-L3899
        return filterHook.filterIndexes(listIndexes(this.catalogId, db_name, tbl_name));
    }

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
    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws MetaException, TException, NoSuchObjectException {
        return filterHook.filterPartitionNames(databaseName, tableName,
                clientDelegate.listPartitionNames(this.catalogId, databaseName, tableName, values, max));
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest) throws MetaException, TException, NoSuchObjectException {
        throw new UnsupportedOperationException("listPartitionValues unsupported");
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
    public PartitionSpecProxy listPartitionSpecs(
            String dbName,
            String tblName,
            int max
    ) throws TException {
        return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
                clientDelegate.listPartitionSpecs(this.catalogId, dbName, tblName, max)));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws MetaException, NoSuchObjectException, TException {
        return PartitionSpecProxy.Factory.get(filterHook.filterPartitionSpecs(
                clientDelegate.listPartitionSpecsByFilter(this.catalogId, dbName, tblName, filter, max)));
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return listPartitions(dbName, tblName, Lists.newArrayList(), max);
    }

    @Override
    public List<Partition> listPartitions(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws NoSuchObjectException, MetaException, TException {
        return filterHook.filterPartitions(clientDelegate.listPartitions(this.catalogId, databaseName, tableName, values, max));
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
        List<Partition> tmpResult = Lists.newArrayListWithExpectedSize(result.size());
        boolean hasUnknownPartitions = clientDelegate.listPartitionsByExpr(this.catalogId, databaseName, tableName, expr,
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
        return filterHook.filterPartitions(clientDelegate.listPartitionsByFilter(this.catalogId, databaseName, tableName, filter, max));
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
    public List<String> listTableNamesByFilter(
            String dbName,
            String filter,
            short maxTables
    ) throws MetaException, TException, InvalidOperationException, UnknownDBException, UnsupportedOperationException {
        return filterHook.filterTableNames(dbName, clientDelegate.listTableNamesByFilter(this.catalogId, dbName, filter, maxTables));
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
    public long openTxn(String user) throws TException {
        return clientDelegate.openTxn(user);
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
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return clientDelegate.renewDelegationToken(tokenStrForm);
    }

    @Override
    public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
        clientDelegate.rollbackTxn(txnId);
    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        ConfVars confVar = HiveConf.getMetaConf(key);
        if (confVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        String validate = confVar.validate(value);
        if (validate != null) {
            throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + validate);
        }
        conf.set(key, value);
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
    public boolean isSameConfObj(HiveConf hiveConf) {
        //taken from HiveMetaStoreClient
        return this.conf == hiveConf;
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
        // PrimaryKeys are currently unsupported
        //return null to allow DESCRIBE (FORMATTED | EXTENDED)
        return new ArrayList<SQLPrimaryKey>();
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        // PrimaryKeys are currently unsupported
        //return null to allow DESCRIBE (FORMATTED | EXTENDED)
        return new ArrayList<SQLForeignKey>();
    }

    @Override
    public void createTableWithConstraints(
            Table table,
            List<SQLPrimaryKey> primaryKeys,
            List<SQLForeignKey> foreignKeys
    ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        clientDelegate.createTableWithConstraints(table, primaryKeys, foreignKeys);
    }

    @Override
    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws MetaException, NoSuchObjectException, TException {
        clientDelegate.dropConstraint(dbName, tblName, constraintName);
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
    public ShowCompactResponse showCompactions() throws TException {
        return clientDelegate.showCompactions();
    }

    @Override
    @Deprecated
    public void addDynamicPartitions(
            long txnId,
            String dbName,
            String tblName,
            List<String> partNames
    ) throws TException {
        clientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames, null);
    }

    @Override
    public void addDynamicPartitions(
            long txnId,
            String dbName,
            String tblName,
            List<String> partNames,
            DataOperationType operationType
    ) throws TException {
        clientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames, operationType);
    }

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

    @Override
    @Deprecated
    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
        //this method has been deprecated;
        return tableExists(DEFAULT_DATABASE_NAME, tableName);
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
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
            String partitionValidationRegex = conf.getVar(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN);
            Pattern partitionValidationPattern =
                    Strings.isNullOrEmpty(partitionValidationRegex) ? null : Pattern.compile(partitionValidationRegex);
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
