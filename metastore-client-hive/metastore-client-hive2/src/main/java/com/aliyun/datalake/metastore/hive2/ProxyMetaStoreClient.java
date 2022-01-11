package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.ProxyMode;
import com.aliyun.datalake.metastore.common.Version;
import com.aliyun.datalake.metastore.common.functional.FunctionalUtils;
import com.aliyun.datalake.metastore.common.functional.ThrowingConsumer;
import com.aliyun.datalake.metastore.common.functional.ThrowingFunction;
import com.aliyun.datalake.metastore.common.functional.ThrowingRunnable;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import com.aliyun.datalake.metastore.hive.common.utils.ClientUtils;
import com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyMetaStoreClient implements IMetaStoreClient {
    private static final Logger logger = LoggerFactory.getLogger(ProxyMetaStoreClient.class);
    private final static String HIVE_FACTORY_CLASS = "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClientFactory";

    private final ProxyMode proxyMode;

    // Dlf Client
    private IMetaStoreClient dlfSessionMetaStoreClient;

    // Hive Client
    private IMetaStoreClient hiveSessionMetaStoreClient;

    // ReadWrite Client
    private IMetaStoreClient readWriteClient;

    // Extra Write Client
    private Optional<IMetaStoreClient> extraClient;

    // Allow failure
    private boolean allowFailure = false;

    // copy Hive conf
    private HiveConf hiveConf;

    private final String readWriteClientType;

    public ProxyMetaStoreClient(HiveConf hiveConf) throws MetaException {
        this(hiveConf, null, false);
    }

    public ProxyMetaStoreClient(HiveConf hiveConf, HiveMetaHookLoader hiveMetaHookLoader, Boolean allowEmbedded) throws MetaException {

        long startTime = System.currentTimeMillis();
        logger.info("ProxyMetaStoreClient start, datalake-metastore-client-version:{}", Version.DATALAKE_METASTORE_CLIENT_VERSION);
        this.hiveConf = new HiveConf(hiveConf);

        proxyMode = ConfigUtils.getProxyMode(hiveConf);

        // init logging if needed
        ProxyLogUtils.initLogUtils(proxyMode, hiveConf.get(DataLakeConfig.CATALOG_PROXY_LOGSTORE,
                                                           ConfigUtils.getUserId(hiveConf)),
                                   hiveConf.getBoolean(DataLakeConfig.CATALOG_ACTION_LOG_ENABLED, DataLakeConfig.DEFAULT_CATALOG_ACTION_LOG_ENABLED),
                                   hiveConf.getBoolean(DataLakeConfig.CATALOG_LOG_ENABLED, DataLakeConfig.DEFAULT_CATALOG_LOG_ENABLED));

        // init Dlf Client if any
        createClient(true, () -> initDlfClient(hiveConf, hiveMetaHookLoader, allowEmbedded, new ConcurrentHashMap<>()));

        // init Hive Client if any
        createClient(false, () -> initHiveClient(hiveConf, hiveMetaHookLoader, allowEmbedded, new ConcurrentHashMap<>()));

        // init extraClient
        initClientByProxyMode();

        readWriteClientType = this.readWriteClient instanceof DlfSessionMetaStoreClient ? "dlf" : "hive";

        logger.info("ProxyMetaStoreClient end, cost:{}ms", System.currentTimeMillis() - startTime);
    }

    private void createClient(boolean isDlf, ThrowingRunnable<MetaException> createClient) throws MetaException {
        try {
            createClient.run();
        } catch (Exception e) {
            if ((isDlf && proxyMode == ProxyMode.METASTORE_DLF_FAILURE)) {
                dlfSessionMetaStoreClient = null;
            } else if (!isDlf && proxyMode == ProxyMode.DLF_METASTORE_FAILURE) {
                hiveSessionMetaStoreClient = null;
            } else {
                throw DataLakeUtil.throwException(new MetaException(e.getMessage()), e);
            }
        }
    }

    public static Map<String, org.apache.hadoop.hive.ql.metadata.Table> getTempTablesForDatabase(String dbName) {
        return getTempTables().get(dbName);
    }

    public static Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> getTempTables() {
        SessionState ss = SessionState.get();
        if (ss == null) {
            return Collections.emptyMap();
        }
        return ss.getTempTables();
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    public void initClientByProxyMode() throws MetaException {
        switch (proxyMode) {
            case METASTORE_ONLY:
                this.readWriteClient = hiveSessionMetaStoreClient;
                this.extraClient = Optional.empty();
                break;
            case METASTORE_DLF_FAILURE:
                this.allowFailure = true;
                this.readWriteClient = hiveSessionMetaStoreClient;
                this.extraClient = Optional.ofNullable(dlfSessionMetaStoreClient);
                break;
            case METASTORE_DLF_SUCCESS:
                this.readWriteClient = hiveSessionMetaStoreClient;
                this.extraClient = Optional.of(dlfSessionMetaStoreClient);
                break;
            case DLF_METASTORE_SUCCESS:
                this.readWriteClient = dlfSessionMetaStoreClient;
                this.extraClient = Optional.of(hiveSessionMetaStoreClient);
                break;
            case DLF_METASTORE_FAILURE:
                this.allowFailure = true;
                this.readWriteClient = dlfSessionMetaStoreClient;
                this.extraClient = Optional.ofNullable(hiveSessionMetaStoreClient);
                break;
            case DLF_ONLY:
                this.readWriteClient = dlfSessionMetaStoreClient;
                this.extraClient = Optional.empty();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    public void initHiveClient(HiveConf hiveConf, HiveMetaHookLoader hiveMetaHookLoader, boolean allowEmbedded,
                               ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException {
        switch (proxyMode) {
            case METASTORE_ONLY:
            case METASTORE_DLF_FAILURE:
            case METASTORE_DLF_SUCCESS:
            case DLF_METASTORE_SUCCESS:
            case DLF_METASTORE_FAILURE:
                this.hiveSessionMetaStoreClient = ClientUtils.createMetaStoreClient(HIVE_FACTORY_CLASS,
                        hiveConf, hiveMetaHookLoader, allowEmbedded, metaCallTimeMap);
                break;
            case DLF_ONLY:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    public void initDlfClient(HiveConf hiveConf, HiveMetaHookLoader hiveMetaHookLoader, boolean allowEmbedded,
                              ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException {
        switch (proxyMode) {
            case METASTORE_ONLY:
                break;
            case METASTORE_DLF_FAILURE:
            case METASTORE_DLF_SUCCESS:
            case DLF_METASTORE_SUCCESS:
            case DLF_METASTORE_FAILURE:
            case DLF_ONLY:
                this.dlfSessionMetaStoreClient = new DlfSessionMetaStoreClient(hiveConf, hiveMetaHookLoader, allowEmbedded);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    @Override
    public boolean isCompatibleWith(HiveConf hiveConf) {
        try {
            return call(readWriteClient, client -> client.isCompatibleWith(hiveConf), "isCompatibleWith", hiveConf);
        } catch (TException e) {
            logger.error("failed to isCompatibleWith:" + e.getMessage(), e);
        }
        return false;
    }

    @Override
    public void setHiveAddedJars(String s) {
        try {
            run(client -> client.setHiveAddedJars(s), "setHiveAddedJars", s);
        } catch (TException e) {
            logger.info(e.getMessage(), e);
        }
    }

    @Override
    public boolean isLocalMetaStore() {
        return !extraClient.isPresent() && readWriteClient.isLocalMetaStore();
    }

    @Override
    public void reconnect() throws MetaException {
        if (hiveSessionMetaStoreClient != null) {
            hiveSessionMetaStoreClient.reconnect();
        }
    }

    @Override
    public void close() {
        if (hiveSessionMetaStoreClient != null) {
            hiveSessionMetaStoreClient.close();
        }
    }

    @Override
    public void createDatabase(Database database)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        run(client -> client.createDatabase(database), "createDatabase", database);
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getDatabase(name), "getDatabase", name);
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getDatabases(pattern), "getDatabases", pattern);
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases(".*");
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws NoSuchObjectException, MetaException, TException {
        run(client -> client.alterDatabase(databaseName, database), "alterDatabase", databaseName, database);
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
        run(client -> client.dropDatabase(name, deleteData, ignoreUnknownDb, cascade), "dropDatabase", name, deleteData,
                ignoreUnknownDb, cascade);
    }

    @Override
    public Partition add_partition(Partition partition)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.add_partition(partition), "add_partition", partition);
    }

    @Override
    public int add_partitions(List<Partition> partitions)
            throws InvalidObjectException, AlreadyExistsException, MetaException,
            TException {
        return call(client -> client.add_partitions(partitions), "add_partitions", partitions);
    }

    @Override
    public List<Partition> add_partitions(
            List<Partition> partitions,
            boolean ifNotExists,
            boolean needResult
    ) throws TException {
        return call(client -> client.add_partitions(partitions, ifNotExists, needResult), "add_partitions", partitions, ifNotExists, needResult);
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy pSpec)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.add_partitions_pspec(pSpec), "add_partitions_pspec", pSpec);
    }

    @Override
    public void alterFunction(String dbName, String functionName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        run(client -> client.alterFunction(dbName, functionName, newFunction), "alterFunction", dbName, functionName, newFunction);
    }

    @Override
    public void alter_index(String dbName, String tblName, String indexName, Index index)
            throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_index(dbName, tblName, indexName, index), "alter_index", dbName, tblName, indexName, index);
    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition partition)
            throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partition(dbName, tblName, partition), "alter_partition", dbName, tblName, partition);
    }

    @Override
    public void alter_partition(
            String dbName,
            String tblName,
            Partition partition,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partition(dbName, tblName, partition, environmentContext), "alter_partition", dbName, tblName, partition, environmentContext);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<Partition> partitions
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partitions(dbName, tblName, partitions), "alter_partitions", dbName, tblName, partitions);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<Partition> partitions,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.alter_partitions(dbName, tblName, partitions, environmentContext), "alter_partitions", dbName, tblName, partitions, environmentContext);
    }

    @Override
    public void alter_table(String dbName, String tblName, Table table)
            throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table(dbName, tblName, table), "alter_table", dbName, tblName, table);
        } else {
            run(client -> client.alter_table(dbName, tblName, table), "alter_table", dbName, tblName, table);
        }
    }

    @Override
    @Deprecated
    public void alter_table(
            String dbName,
            String tblName,
            Table table,
            boolean cascade
    ) throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table(dbName, tblName, table, cascade), "alter_table", dbName, tblName, table, cascade);
        } else {
            run(client -> client.alter_table(dbName, tblName, table, cascade), "alter_table", dbName, tblName, table, cascade);
        }
    }

    @Override
    public void alter_table_with_environmentContext(
            String dbName,
            String tblName,
            Table table,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        if (table.isTemporary()) {
            run(this.readWriteClient, client -> client.alter_table_with_environmentContext(dbName, tblName, table, environmentContext), "alter_table_with_environmentContext", dbName, tblName, table, environmentContext);
        } else {
            run(client -> client.alter_table_with_environmentContext(dbName, tblName, table, environmentContext), "alter_table_with_environmentContext", dbName,
                    tblName, table, environmentContext);
        }
    }

    @Override
    public Partition appendPartition(String dbName, String tblName, List<String> values)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.appendPartition(dbName, tblName, values), "appendPartition", dbName, tblName, values);
    }

    @Override
    public Partition appendPartition(String dbName, String tblName, String partitionName)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return call(client -> client.appendPartition(dbName, tblName, partitionName), "appendPartition", dbName, tblName, partitionName);
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        return call(client -> client.create_role(role), "create_role", role);
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        return call(client -> client.drop_role(roleName), "drop_role", roleName);
    }

    @Override
    public List<Role> list_roles(
            String principalName, PrincipalType principalType
    ) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.list_roles(principalName, principalType), "list_roles", principalName, principalType);
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.listRoleNames(), "listRoleNames");
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.get_principals_in_role(request), "get_principals_in_role", request);
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.get_role_grants_for_principal(request), "get_role_grants_for_principal", request);
    }

    @Override
    public boolean grant_role(
            String roleName,
            String userName,
            PrincipalType principalType,
            String grantor, PrincipalType grantorType,
            boolean grantOption
    ) throws MetaException, TException {
        return call(client -> client.grant_role(roleName, userName, principalType, grantor, grantorType, grantOption)
                , "grant_role", roleName, userName, principalType, grantor, grantorType, grantOption);
    }

    @Override
    public boolean revoke_role(
            String roleName,
            String userName,
            PrincipalType principalType,
            boolean grantOption
    ) throws MetaException, TException {
        return call(client -> client.revoke_role(roleName, userName, principalType, grantOption), "revoke_role", roleName, userName,
                principalType, grantOption);
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        run(client -> client.cancelDelegationToken(tokenStrForm), "cancelDelegationToken", tokenStrForm);
    }

    @Override
    public String getTokenStrForm() throws IOException {
        try {
            return call(this.readWriteClient, client -> {
                try {
                    return client.getTokenStrForm();
                } catch (IOException e) {
                    throw new TException(e.getMessage(), e);
                }
            }, "getTokenStrForm");
        } catch (TException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return call(client -> client.addToken(tokenIdentifier, delegationToken), "addToken", tokenIdentifier, delegationToken);
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return call(client -> client.removeToken(tokenIdentifier), "removeToken", tokenIdentifier);
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return call(this.readWriteClient, client -> client.getToken(tokenIdentifier), "getToken", tokenIdentifier);
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return call(this.readWriteClient, client -> client.getAllTokenIdentifiers(), "getAllTokenIdentifiers");
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return call(client -> client.addMasterKey(key), "addMasterKey", key);
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key)
            throws NoSuchObjectException, MetaException, TException {
        run(client -> client.updateMasterKey(seqNo, key), "updateMasterKey", key);
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return call(client -> client.removeMasterKey(keySeq), "removeMasterKey", keySeq);
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return call(this.readWriteClient, client -> client.getMasterKeys(), "getMasterKeys");
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return call(this.readWriteClient, client -> client.checkLock(lockId), "checkLock", lockId);
    }

    @Override
    public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
        run(client -> client.commitTxn(txnId), "commitTxn", txnId);
    }

    @Override
    public void abortTxns(List<Long> txnIds) throws TException {
        run(client -> client.abortTxns(txnIds), "abortTxns", txnIds);
    }

    @Override
    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType
    ) throws TException {
        run(client -> client.compact(dbName, tblName, partitionName, compactionType), "compact", dbName, tblName, partitionName, compactionType);
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
        run(client -> client.compact(dbName, tblName, partitionName, compactionType, tblProperties), "compact", dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public CompactionResponse compact2(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        return call(client -> client.compact2(dbName, tblName, partitionName, compactionType, tblProperties), "compact2", dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {
        run(client -> client.createFunction(function), "createFunction", function);
    }

    @Override
    public void createIndex(Index index, Table indexTable)
            throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {
        run(client -> client.createIndex(index, indexTable), "createIndex", index, indexTable);
    }

    @Override
    public void createTable(Table tbl)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        createTable(tbl, null);
    }

    public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        // Subclasses can override this step (for example, for temporary tables)
        if (tbl.isTemporary()) {
            run(this.readWriteClient, client -> client.createTable(tbl), "createTable", tbl);
        } else {
            run(client -> client.createTable(tbl), "createTable", tbl);
        }
    }

    @Override
    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return call(client -> client.deletePartitionColumnStatistics(dbName, tableName, partName, colName), "deletePartitionColumnStatistics", dbName,
                tableName, partName, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, InvalidInputException {
        return call(client -> client.deleteTableColumnStatistics(dbName, tableName, colName), "deleteTableColumnStatistics", dbName, tableName, colName);
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
            InvalidObjectException, InvalidInputException, TException {
        run(client -> client.dropFunction(dbName, functionName), "dropFunction", dbName, functionName);
    }

    @Override
    public boolean dropIndex(String dbName, String tblName, String name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropIndex(dbName, tblName, name, deleteData), "dropIndex", dbName, tblName, name, deleteData);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(dbName, tblName, values, deleteData), "dropPartition", dbName, tblName, values, deleteData);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options)
            throws TException {
        return call(client -> client.dropPartition(dbName, tblName, values, options), "dropPartition", dbName, tblName, values, options);
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartition(dbName, tblName, partitionName, deleteData), "dropPartition", dbName, tblName,
                partitionName, deleteData);
    }

    @Override
    public List<Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData,
            boolean ifExists
    ) throws NoSuchObjectException, MetaException, TException {
        return call(client -> client.dropPartitions(dbName, tblName, partExprs, deleteData, ifExists), "dropPartitions", dbName,
                tblName, partExprs, deleteData, ifExists);
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
        return call(client -> client.dropPartitions(dbName, tblName, partExprs, deleteData, false, needResults), "dropPartitions",
                dbName, tblName, partExprs, deleteData, ifExists, needResults);
    }

    @Override
    public List<Partition> dropPartitions(
            String dbName,
            String tblName,
            List<ObjectPair<Integer, byte[]>> partExprs,
            PartitionDropOptions options
    ) throws TException {
        return call(client -> client.dropPartitions(dbName, tblName, partExprs, options), "dropPartitions", dbName, tblName, partExprs, options);
    }

    @Override
    @Deprecated
    public void dropTable(String tableName, boolean deleteData)
            throws MetaException, UnknownTableException, TException, NoSuchObjectException {
        org.apache.hadoop.hive.metastore.api.Table table = getTempTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(tableName, deleteData), "dropTable", tableName, deleteData);
        } else {
            run(client -> client.dropTable(tableName, deleteData), "dropTable", tableName, deleteData);
        }
    }

    private org.apache.hadoop.hive.metastore.api.Table getTempTable(String dbName, String tableName) {
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName.toLowerCase());
        if (tables != null) {
            org.apache.hadoop.hive.ql.metadata.Table table = tables.get(tableName.toLowerCase());
            if (table != null) {
                return table.getTTable();
            }
        }
        return null;
    }

    @Override
    public void dropTable(String dbname, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(dbname, tableName), "dropTable", dbname, tableName);
        } else {
            run(client -> client.dropTable(dbname, tableName), "dropTable", dbname, tableName);
        }
    }

    @Override
    public void dropTable(
            String dbname,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTab
    ) throws MetaException, TException, NoSuchObjectException {
        org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTab), "dropTable", dbname, tableName, deleteData, ignoreUnknownTab);
        } else {
            run(client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTab), "dropTable", dbname, tableName, deleteData, ignoreUnknownTab);
        }
    }

    @Override
    public void dropTable(
            String dbname,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTable,
            boolean ifPurge
    ) throws MetaException, TException, NoSuchObjectException {
        org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, tableName);
        if (table != null) {
            run(this.readWriteClient, client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTable, ifPurge), "dropTable", dbname, tableName, deleteData, ignoreUnknownTable, ifPurge);
        } else {
            run(client -> client.dropTable(dbname, tableName, deleteData, ignoreUnknownTable, ifPurge), "dropTable", dbname, tableName, deleteData, ignoreUnknownTable, ifPurge);
        }
    }

    @Override
    public Partition exchange_partition(
            Map<String, String> partitionSpecs,
            String srcDb,
            String srcTbl,
            String dstDb,
            String dstTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return call(client -> client.exchange_partition(partitionSpecs, srcDb, srcTbl, dstDb, dstTbl), "exchange_partition", partitionSpecs
                , srcDb, srcTbl, dstDb, dstTbl);
    }

    @Override
    public List<Partition> exchange_partitions(
            Map<String, String> partitionSpecs,
            String sourceDb,
            String sourceTbl,
            String destDb,
            String destTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return call(client -> client.exchange_partitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl), "exchange_partitions",
                partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
    }

    @Override
    public AggrStats getAggrColStatsFor(
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partName
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getAggrColStatsFor(dbName, tblName, colNames, partName), "getAggrColStatsFor", dbName, tblName, colNames, partName);
    }

    @Override
    public List<String> getAllTables(String dbname)
            throws MetaException, TException, UnknownDBException {
        return getTables(dbname, ".*");
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
            throws TException, ConfigValSecurityException {
        return call(this.readWriteClient, client -> client.getConfigValue(name, defaultValue), "getConfigValue", name, defaultValue);
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getDelegationToken(owner, renewerKerberosPrincipalName), "getDelegationToken", owner, renewerKerberosPrincipalName);
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws TException {
        return call(this.readWriteClient, client -> client.getFields(db, tableName), "getFields", db, tableName);
    }

    @Override
    public Function getFunction(String dbName, String functionName) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getFunction(dbName, functionName), "getFunction", dbName, functionName);
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getFunctions(dbName, pattern), "getFunctions", dbName, pattern);
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getAllFunctions(), "getAllFunctions");
    }

    @Override
    public Index getIndex(String dbName, String tblName, String indexName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getIndex(dbName, tblName, indexName), "getIndex", dbName, tblName, indexName);
    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.getMetaConf(key), "getMetaConf", key);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> values)
            throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartition(dbName, tblName, values), "getPartition", dbName, tblName, values);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String partitionName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPartition(dbName, tblName, partitionName), "getPartition", dbName, tblName, partitionName);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartitionColumnStatistics(dbName, tableName, partitionNames, columnNames), "getPartitionColumnStatistics", dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Partition getPartitionWithAuthInfo(
            String databaseName,
            String tableName,
            List<String> values,
            String userName,
            List<String> groupNames
    ) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPartitionWithAuthInfo(databaseName, tableName, values, userName, groupNames), "getPartitionWithAuthInfo", databaseName, tableName, values, userName, groupNames);
    }

    @Override
    public List<Partition> getPartitionsByNames(
            String databaseName,
            String tableName,
            List<String> partitionNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getPartitionsByNames(databaseName, tableName, partitionNames), "getPartitionsByNames", databaseName, tableName, partitionNames);
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName) throws TException {
        return call(this.readWriteClient, client -> client.getSchema(db, tableName), "getSchema", db, tableName);
    }

    @Override
    @Deprecated
    public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.getTable(tableName), "getTable", tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.getTable(dbName, tableName), "getTable", dbName, tableName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String dbName,
            String tableName,
            List<String> colNames
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.getTableColumnStatistics(dbName, tableName, colNames), "getTableColumnStatistics", dbName, tableName, colNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return call(this.readWriteClient, client -> client.getTableObjectsByName(dbName, tableNames), "getTableObjectsByName", dbName, tableNames);
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTables(dbname, tablePattern), "getTables", dbname, tablePattern);
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.getTables(dbname, tablePattern, tableType), "getTables", dbname, tablePattern, tableType);
    }

    @Override
    public List<TableMeta> getTableMeta(
            String dbPatterns,
            String tablePatterns,
            List<String> tableTypes
    ) throws MetaException, TException, UnknownDBException, UnsupportedOperationException {
        return call(this.readWriteClient, client -> client.getTableMeta(dbPatterns, tablePatterns, tableTypes), "getTableMeta", dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return call(this.readWriteClient, client -> client.getValidTxns(), "getValidTxns");
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return call(this.readWriteClient, client -> client.getValidTxns(currentTxn), "getValidTxns", currentTxn);
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef obj, String user, List<String> groups)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.get_privilege_set(obj, user, groups), "get_privilege_set", obj, user, groups);
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges)
            throws MetaException, TException {
        return call(client -> client.grant_privileges(privileges), "grant_privileges", privileges);
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption)
            throws MetaException, TException {
        return call(client -> client.revoke_privileges(privileges, grantOption), "revoke_privileges", privileges, grantOption);
    }

    @Override
    public void heartbeat(long txnId, long lockId)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        run(client -> client.heartbeat(txnId, lockId), "heartbeat", txnId, lockId);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return call(client -> client.heartbeatTxnRange(min, max), "heartbeatTxnRange", min, max);
    }

    @Override
    public boolean isPartitionMarkedForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs, PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return call(this.readWriteClient, client -> client.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType), "isPartitionMarkedForEvent", dbName, tblName, partKVs, eventType);
    }

    @Override
    public List<String> listIndexNames(
            String dbName,
            String tblName,
            short max
    ) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.listIndexNames(dbName, tblName, max), "listIndexNames", dbName, tblName, max);
    }

    @Override
    public List<Index> listIndexes(String db_name, String tbl_name, short max) throws TException {
        return call(this.readWriteClient, client -> client.listIndexes(db_name, tbl_name, max), "listIndexes", db_name, tbl_name, max);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitionNames(dbName, tblName, max), "listPartitionNames", dbName, tblName, max);
    }

    @Override
    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionNames(databaseName, tableName, values, max), "listPartitionNames", databaseName, tableName, values, max);
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionValues(partitionValuesRequest), "listPartitionValues", partitionValuesRequest);
    }

    @Override
    public int getNumPartitionsByFilter(
            String dbName,
            String tableName,
            String filter
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getNumPartitionsByFilter(dbName, tableName, filter), "getNumPartitionsByFilter", dbName, tableName, filter);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(
            String dbName,
            String tblName,
            int max
    ) throws TException {
        return call(this.readWriteClient, client -> client.listPartitionSpecs(dbName, tblName, max), "listPartitionSpecs", dbName, tblName, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.listPartitionSpecsByFilter(dbName, tblName, filter, max), "listPartitionSpecsByFilter", dbName, tblName, filter, max);
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitions(dbName, tblName, max), "listPartitions", dbName, tblName, max);
    }

    @Override
    public List<Partition> listPartitions(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws NoSuchObjectException, MetaException, TException {
        return call(this.readWriteClient, client -> client.listPartitions(databaseName, tableName, values, max), "listPartitions", databaseName, tableName, values, max);
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
        return call(this.readWriteClient, client -> client.listPartitionsByExpr(databaseName, tableName, expr, defaultPartitionName, max, result), "listPartitionsByExpr", databaseName, tableName, expr, defaultPartitionName, max, result);
    }

    @Override
    public List<Partition> listPartitionsByFilter(
            String databaseName,
            String tableName,
            String filter,
            short max
    ) throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.listPartitionsByFilter(databaseName, tableName, filter, max), "listPartitionsByFilter", databaseName, tableName, filter, max);
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String database,
            String table,
            short maxParts,
            String user,
            List<String> groups
    ) throws MetaException, TException, NoSuchObjectException {
        return call(this.readWriteClient, client -> client.listPartitionsWithAuthInfo(database, table, maxParts, user, groups), "listPartitionsWithAuthInfo", database, table, maxParts, user, groups);
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
        return call(this.readWriteClient, client -> client.listPartitionsWithAuthInfo(database, table, partVals, maxParts, user, groups), "listPartitionsWithAuthInfo", database, table, partVals, maxParts, user, groups);
    }

    @Override
    public List<String> listTableNamesByFilter(
            String dbName,
            String filter,
            short maxTables
    ) throws MetaException, TException, InvalidOperationException, UnknownDBException, UnsupportedOperationException {
        return call(this.readWriteClient, client -> client.listTableNamesByFilter(dbName, filter, maxTables), "listTableNamesByFilter", dbName, filter, maxTables);
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(
            String principal,
            PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.list_privileges(principal, principalType, objectRef), "list_privileges", principal, principalType, objectRef);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return call(client -> client.lock(lockRequest), "lock", lockRequest);
    }

    @Override
    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        run(client -> client.markPartitionForEvent(dbName, tblName, partKVs, eventType), "markPartitionForEvent", dbName, tblName, partKVs, eventType);
    }

    @Override
    public long openTxn(String user) throws TException {
        return call(client -> client.openTxn(user), "openTxn", user);
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        return call(client -> client.openTxns(user, numTxns), "openTxns", user, numTxns);
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.partitionNameToSpec(name), "partitionNameToSpec", name);
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return call(this.readWriteClient, client -> client.partitionNameToVals(name), "partitionNameToVals", name);
    }

    @Override
    public void renamePartition(
            String dbName,
            String tblName,
            List<String> partitionValues,
            Partition newPartition
    ) throws InvalidOperationException, MetaException, TException {
        run(client -> client.renamePartition(dbName, tblName, partitionValues, newPartition), "renamePartition", dbName, tblName,
                partitionValues, newPartition);
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return call(client -> client.renewDelegationToken(tokenStrForm), "renewDelegationToken", tokenStrForm);
    }

    @Override
    public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
        run(client -> client.rollbackTxn(txnId), "rollbackTxn", txnId);
    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        run(client -> client.setMetaConf(key, value), "setMetaConf", key, value);
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, InvalidInputException {
        SetPartitionsStatsRequest deepCopy = request.deepCopy();
        boolean result = readWriteClient.setPartitionColumnStatistics(deepCopy);
        if (extraClient.isPresent()) {
            try {
                extraClient.get().setPartitionColumnStatistics(request);
            } catch (Exception e) {
                FunctionalUtils.collectLogs(e, "setPartitionColumnStatistics", request);
                if (!allowFailure) {
                    throw e;
                }
            }
        }
        return result;
    }

    @Override
    public void flushCache() {
        try {
            run(client -> client.flushCache(), "flushCache");
        } catch (TException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        return call(this.readWriteClient, client -> client.getFileMetadata(fileIds), "getFileMetadata", fileIds);
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters
    ) throws TException {
        return call(this.readWriteClient, client -> client.getFileMetadataBySarg(fileIds, sarg, doGetFooters), "getFileMetadataBySarg", fileIds, sarg, doGetFooters);
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
        run(client -> client.clearFileMetadata(fileIds), "clearFileMetadata", fileIds);
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        run(client -> client.putFileMetadata(fileIds, metadata), "putFileMetadata", fileIds, metadata);
    }

    @Override
    public boolean isSameConfObj(HiveConf hiveConf) {
        try {
            return call(this.readWriteClient, client -> client.isSameConfObj(hiveConf), "isSameConfObj", hiveConf);
        } catch (TException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean cacheFileMetadata(
            String dbName,
            String tblName,
            String partName,
            boolean allParts
    ) throws TException {
        return call(client -> client.cacheFileMetadata(dbName, tblName, partName, allParts), "cacheFileMetadata", dbName, tblName, partName, allParts);
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        return call(this.readWriteClient, client -> client.getPrimaryKeys(primaryKeysRequest), "getPrimaryKeys", primaryKeysRequest);
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        // PrimaryKeys are currently unsupported
        //return null to allow DESCRIBE (FORMATTED | EXTENDED)
        return call(this.readWriteClient, client -> client.getForeignKeys(foreignKeysRequest), "getForeignKeys", foreignKeysRequest);
    }

    @Override
    public void createTableWithConstraints(
            Table table,
            List<SQLPrimaryKey> primaryKeys,
            List<SQLForeignKey> foreignKeys
    ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        run(client -> client.createTableWithConstraints(table, primaryKeys, foreignKeys), "createTableWithConstraints", table, primaryKeys, foreignKeys);
    }

    @Override
    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws MetaException, NoSuchObjectException, TException {
        run(client -> client.dropConstraint(dbName, tblName, constraintName), "dropConstraint", dbName, tblName, constraintName);
    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addPrimaryKey(primaryKeyCols), "addPrimaryKey", primaryKeyCols);
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        run(client -> client.addForeignKey(foreignKeyCols), "addForeignKey", foreignKeyCols);
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return call(this.readWriteClient, client -> client.showCompactions(), "showCompactions");
    }

    @Override
    @Deprecated
    public void addDynamicPartitions(
            long txnId,
            String dbName,
            String tblName,
            List<String> partNames
    ) throws TException {
        run(client -> client.addDynamicPartitions(txnId, dbName, tblName, partNames), "addDynamicPartitions", txnId, dbName, tblName, partNames);
    }

    @Override
    public void addDynamicPartitions(
            long txnId,
            String dbName,
            String tblName,
            List<String> partNames,
            DataOperationType operationType
    ) throws TException {
        run(client -> client.addDynamicPartitions(txnId, dbName, tblName, partNames, operationType), "addDynamicPartitions", txnId, dbName, tblName, partNames, operationType);
    }

    @Override
    public void insertTable(Table table, boolean overwrite) throws MetaException {
        try {
            run(client -> client.insertTable(table, overwrite), "insertTable", table, overwrite);
        } catch (TException e) {
            throw DataLakeUtil.throwException(new MetaException("failed to insertTable:" + e.getMessage()), e);
        }
    }

    @Override
    public NotificationEventResponse getNextNotification(
            long lastEventId,
            int maxEvents,
            NotificationFilter notificationFilter
    ) throws TException {
        return call(this.readWriteClient, client -> client.getNextNotification(lastEventId, maxEvents, notificationFilter), "getNextNotification", lastEventId, maxEvents, notificationFilter);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return call(this.readWriteClient, client -> client.getCurrentNotificationEventId(), "getCurrentNotificationEventId");
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return call(this.readWriteClient, client -> client.fireListenerEvent(fireEventRequest), "fireListenerEvent", fireEventRequest);
    }

    @Override
    @Deprecated
    public ShowLocksResponse showLocks() throws TException {
        return call(this.readWriteClient, client -> client.showLocks(), "showLocks");
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return call(this.readWriteClient, client -> client.showLocks(showLocksRequest), "showLocks", showLocksRequest);
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return call(this.readWriteClient, client -> client.showTxns(), "showTxns");
    }

    @Override
    @Deprecated
    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.tableExists(tableName), "tableExists", tableName);
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        return call(this.readWriteClient, client -> client.tableExists(databaseName, tableName), "tableExists", databaseName, tableName);
    }

    @Override
    public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
        run(client -> client.unlock(lockId), "unlock", lockId);
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return call(client -> client.updatePartitionColumnStatistics(columnStatistics), "updatePartitionColumnStatistics", columnStatistics);
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        return call(client -> client.updateTableColumnStatistics(columnStatistics), "updateTableColumnStatistics", columnStatistics);
    }

    @Override
    public void validatePartitionNameCharacters(List<String> part_vals) throws TException, MetaException {
        run(this.readWriteClient, client -> client.validatePartitionNameCharacters(part_vals), "validatePartitionNameCharacters", part_vals);
    }

    @VisibleForTesting
    public IMetaStoreClient getDlfSessionMetaStoreClient() {
        return dlfSessionMetaStoreClient;
    }

    @VisibleForTesting
    public IMetaStoreClient getHiveSessionMetaStoreClient() {
        return hiveSessionMetaStoreClient;
    }

    @VisibleForTesting
    boolean isAllowFailure() {
        return allowFailure;
    }

    public void run(ThrowingConsumer<IMetaStoreClient, TException> consumer, String actionName,
                    Object... parameters) throws TException {
        FunctionalUtils.run(this.readWriteClient, extraClient, allowFailure, consumer, this.readWriteClientType, actionName, parameters);
    }

    public void run(IMetaStoreClient client, ThrowingConsumer<IMetaStoreClient, TException> consumer,
                    String actionName, Object... parameters) throws TException {
        FunctionalUtils.run(client, Optional.empty(), allowFailure, consumer, this.readWriteClientType, actionName, parameters);
    }

    public <R> R call(ThrowingFunction<IMetaStoreClient, R, TException> consumer, String actionName, Object... parameters) throws TException {
        return FunctionalUtils.call(this.readWriteClient, extraClient, allowFailure, consumer,
                this.readWriteClientType, actionName, parameters);
    }

    public <R> R call(IMetaStoreClient client, ThrowingFunction<IMetaStoreClient, R, TException> consumer,
                      String actionName, Object... parameters) throws TException {
        return FunctionalUtils.call(client, Optional.empty(), allowFailure, consumer, this.readWriteClientType,
                actionName, parameters);
    }
}