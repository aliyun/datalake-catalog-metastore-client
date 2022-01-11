package com.aliyun.datalake.metastore.common;

import com.aliyun.datalake.metastore.common.api.DataLakeAPIException;
import com.aliyun.datalake.metastore.common.entity.PaginatedResult;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake.metastore.common.entity.StsTokenInfo;
import com.aliyun.datalake.metastore.common.functional.FunctionalUtils;
import com.aliyun.datalake.metastore.common.util.CupidAkUtils;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake20200710.models.ColumnStatisticsObj;
import com.aliyun.datalake20200710.models.Database;
import com.aliyun.datalake20200710.models.Function;
import com.aliyun.datalake20200710.models.FunctionInput;
import com.aliyun.datalake20200710.models.LockObj;
import com.aliyun.datalake20200710.models.LockStatus;
import com.aliyun.datalake20200710.models.Partition;
import com.aliyun.datalake20200710.models.PartitionInput;
import com.aliyun.datalake20200710.models.PrincipalPrivilegeSet;
import com.aliyun.datalake20200710.models.Table;
import com.aliyun.datalake20200710.models.TableInput;
import com.aliyun.datalake20200710.models.TaskStatus;
import com.aliyun.datalake20200710.models.UpdateTablePartitionColumnStatisticsRequest;
import com.aliyun.tea.TeaException;
import com.aliyun.teaopenapi.models.Config;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.aliyun.datalake.metastore.common.Constant.RETRYABLE_ERROR_CODE;
import static com.aliyun.datalake.metastore.common.Constant.TOKEN_RELATED_ERROR_CODE;
import static com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_ACCURATE_BATCH_SIZE;
import static com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_TABLE_COL_STATS_PAGE_SIZE;
import static com.aliyun.datalake.metastore.common.STSHelper.initSTSHelper;

public class DefaultDataLakeMetaStore implements IDataLakeMetaStore {
    private static final String EMPTY_TOKEN = "";
    private final Logger logger = LoggerFactory.getLogger(DefaultDataLakeMetaStore.class);
    private final Set<Action> notNeedRetryActions = new HashSet<Action>() {{
        add(Action.LOCK);
    }};
    private DataLakeClient dataLakeClient;
    private Config dataLakeClientConfig;
    private Properties extendedConfig;  //extend config
    private ExecutorService executorService; // for batchedCall
    private int batchSize;
    private int tableColStatsPageSize;

    public DefaultDataLakeMetaStore(Config conf, Properties extendedConfig, ExecutorService service) throws Exception {
        this.dataLakeClientConfig = conf;
        this.extendedConfig = extendedConfig;
        if (service == null) {
            this.executorService = new DefaultExecutorServiceFactory().getExecutorService(DataLakeConfig.DEFAULT_NUM_EXECUTOR_THREADS);
        } else {
            this.executorService = service;
        }

        this.batchSize = Integer.parseInt(extendedConfig.getProperty(CATALOG_ACCURATE_BATCH_SIZE));
        this.tableColStatsPageSize = Integer.parseInt(extendedConfig.getProperty(CATALOG_TABLE_COL_STATS_PAGE_SIZE));
        reInitDataLakeClient(conf, extendedConfig, false);
    }

    private <M, V extends ResultModel<M>> M call(Callable<V> c, Action action) throws Exception {
        V result;
        try {
            result = retryWhenGetException(c, 1, action);
            logger.debug("Action: {}, RequestId: {}, Code: {}", action, result.requestId, result.code);
        } catch (Exception e) {
            logger.error("Action failed: " + action.name() + ", msg: " + e.getMessage(), e);
            throw new Exception(e.getMessage(), e);
        }
        if (result != null && result.success) {
            return result.data;
        } else {
            throw new DataLakeAPIException(result, action);
        }
    }

    private <V extends ResultModel> V retryWhenGetException(Callable<V> c, int numTries, Action action) throws Exception {
        try {
            V v = c.call();
            if (v != null && !v.success && RETRYABLE_ERROR_CODE.contains(v.code)) {
                throw new RetryableException(v.code, v.message, v.requestId);
            }
            return v;
        } catch (TeaException | RetryableException e) {
            if (numTries <= DataLakeConfig.MAX_RETRY_TIMES && isNeedRetry(action)) {
                logger.info(String.format("Exception got: [%s], now retry.", e.getMessage()));
                if (e instanceof RetryableException &&
                        TOKEN_RELATED_ERROR_CODE.contains(((RetryableException) e).getErrorCode())) {
                    logger.info("Token error occurs, now re-apply the token and re-init the client.");
                    reInitDataLakeClient(dataLakeClientConfig, extendedConfig, true);
                }
                // sleep for a random period in case of temporary unavailable of the service.
                try {
                    Thread.sleep((long) (Math.random() * Math.pow(2, numTries)));
                } catch (InterruptedException ie) {
                    // do nothing
                    logger.info("client unavailable of service");
                }
                return retryWhenGetException(c, numTries + 1, action);
            } else {
                throw e;
            }
        }
    }

    public boolean isNeedRetry(Action action) {
        if (notNeedRetryActions.contains(action)) {
            return false;
        } else {
            return true;
        }
    }

    private void reInitDataLakeClient(Config conf, Properties extendedConfig, boolean force) throws Exception {
        try {
            String accessKeyId = null;
            String accessKeySecret = null;
            String regionId = null;
            String stsToken = null;
            DataLakeConfig.AKMode akMode = DataLakeConfig.AKMode.valueOf(extendedConfig.getProperty(DataLakeConfig.CATALOG_AK_MODE));
            if (akMode == DataLakeConfig.AKMode.MANUAL) {
                accessKeyId = conf.getAccessKeyId();
                accessKeySecret = conf.getAccessKeySecret();
                regionId = conf.getRegionId();
                stsToken = conf.getSecurityToken();
            } else if (akMode == DataLakeConfig.AKMode.EMR_AUTO) {
                try {
                    // only init daemon update at EMR_AUTO mode
                    boolean isNewStsMode = Boolean.parseBoolean(extendedConfig.getProperty(DataLakeConfig.CATALOG_STS_IS_NEW_MODE));
                    initSTSHelper(isNewStsMode);
                    Properties props = STSHelper.getLatestSTSToken(force, isNewStsMode);

                    accessKeyId = props.getProperty(STSHelper.STS_ACCESS_KEY_ID);
                    accessKeySecret = props.getProperty(STSHelper.STS_ACCESS_KEY_SECRET);
                    regionId = props.getProperty(STSHelper.STS_REGION);
                    stsToken = props.getProperty(STSHelper.STS_TOKEN);
                } catch (IOException e) {
                    String message = String.format("Cannot obtain STS token from EMR meta-service. Note that AK-Mode[%s] "
                                    + "can only used in EMR clusters, otherwise you should config the %s and %s explicitly.",
                            akMode, DataLakeConfig.CATALOG_ACCESS_KEY_ID,
                            DataLakeConfig.CATALOG_ACCESS_KEY_SECRET);
                    throw new Exception(message + e.getMessage(), e);
                }
            } else if (akMode == DataLakeConfig.AKMode.CUPID) {
                try {
                    String uid = extendedConfig.getProperty(DataLakeConfig.CATALOG_USER_ID);
                    if (uid == null) {
                        throw new Exception("User id not found in  conf, cannot get ak from cupid.");
                    }
                    String role = extendedConfig.getProperty(DataLakeConfig.CATALOG_ROLE);
                    if (role == null) {
                        throw new Exception("Role not found in  conf, cannot get ak from cupid.");
                    }
                    regionId = conf.getRegionId();
                    if (regionId == null) {
                        throw new Exception(DataLakeConfig.CATALOG_REGION_ID + " can not be empty.");
                    }

                    StsTokenInfo stsTokenInfo = CupidAkUtils.fetchStsToken(uid, role);
                    accessKeyId = stsTokenInfo.accessKeyId;
                    accessKeySecret = stsTokenInfo.accessKeySecret;
                    stsToken = stsTokenInfo.stsToken;
                } catch (Exception ex) {
                    String message = String.format("Cannot obtain STS token from CUPID Env with %s. Note that AK-Mode[%s] "
                                    + "can only used in MaxCompute clusters, otherwise you should config the %s and %s explicitly.",
                            ex.getMessage(),
                            akMode, DataLakeConfig.CATALOG_ACCESS_KEY_ID,
                            DataLakeConfig.CATALOG_ACCESS_KEY_SECRET);
                    throw new Exception(message, ex);
                }
            } else {
                String uid = extendedConfig.getProperty(DataLakeConfig.CATALOG_USER_ID);
                if (uid == null) {
                    throw new Exception("User id not found in  conf, cannot get ak from cupid.");
                }
                String role = extendedConfig.getProperty(DataLakeConfig.CATALOG_ROLE);
                if (role == null) {
                    throw new Exception("Role not found in  conf, cannot get ak from cupid.");
                }
                try {
                    Properties props = STSHelper.getEMRSTSToken(uid, role);
                    accessKeyId = props.getProperty(STSHelper.STS_ACCESS_KEY_ID);
                    accessKeySecret = props.getProperty(STSHelper.STS_ACCESS_KEY_SECRET);
                    regionId = props.getProperty(STSHelper.STS_REGION);
                    stsToken = props.getProperty(STSHelper.STS_TOKEN);
                } catch (IOException e) {
                    String message = String.format("Cannot obtain STS token from EMR meta-service. Note that AK-Mode[%s] "
                                    + "can only used in Data Lake Formation, otherwise you should config the %s and %s explicitly.",
                            DataLakeConfig.CATALOG_AK_MODE, DataLakeConfig.CATALOG_ACCESS_KEY_ID,
                            DataLakeConfig.CATALOG_ACCESS_KEY_SECRET);
                    String additionalMessage = e.getMessage();
                    throw new Exception(message + String.format(". Additional Message: uid=%s, role=%s", uid, role)
                            + additionalMessage, e);
                }
            }
            String endpoint = conf.getEndpoint();
            if (endpoint == null) {
                throw new Exception("Empty endpoint, pls set " + DataLakeConfig.CATALOG_ENDPOINT +
                        " or " + DataLakeConfig.CATALOG_REGION_ID + " explicitly.");
            }
            Config config = new Config();
            config.accessKeyId = accessKeyId;
            config.accessKeySecret = accessKeySecret;
            config.endpoint = endpoint;
            config.regionId = regionId;
            config.securityToken = stsToken;
            config.readTimeout = conf.getReadTimeout();
            config.connectTimeout = conf.getConnectTimeout();
            this.dataLakeClient = new DataLakeClient(config);
        } catch (Exception e) {
            throw new Exception("Initialize DlfMetaStoreClient failed: " + e.getMessage(), e);
        }
    }

    public void createDatabase(String catalogId, Database database) throws Exception {
        call(() -> dataLakeClient.getDatabaseApi().createDatabase(catalogId, database.getName(),
                database.getDescription(), database.getLocationUri(), database.getParameters(),
                database.getOwnerName(), database.getOwnerType(),
                database.getPrivileges()),
                Action.CREATE_DATABASE);
    }

    public void createDatabase(String catalogId, String dbName, String description, String location, Map<String, String> parameters, String ownerName, String ownerType, PrincipalPrivilegeSet principalPrivilegeSet) throws Exception {
        call(() -> dataLakeClient.getDatabaseApi().createDatabase(catalogId, dbName,
                description, location, parameters,
                ownerName, ownerType,
                principalPrivilegeSet), Action.CREATE_DATABASE);
    }

    public Database getDatabase(String catalogId, String dbName) throws Exception {
        Database database =
                call(() -> dataLakeClient.getDatabaseApi().getDatabase(catalogId, dbName), Action.GET_DATABASE);
        return database;
    }

    public List<String> getDatabases(String catalogId, String pattern, int pageSize) throws Exception {
        List<String> dbNames = new ArrayList<>();
        String nextToken = EMPTY_TOKEN;
        do {
            final String nextTok = nextToken; // a temp final variable used in lambda expression.
            PaginatedResult<Database> result =
                    call(() -> dataLakeClient.getDatabaseApi().listDatabases(catalogId,
                            pattern, pageSize, nextTok), Action.GET_DATABASES);
            nextToken = result.getNextPageToken();
            dbNames.addAll(result.getData().stream().map(db -> db.name).collect(Collectors.toList()));
        } while (!nextToken.equals(EMPTY_TOKEN));
        return dbNames;
    }

    public void alterDatabase(String catalogId, String dbName, Database database) throws Exception {
        call(() -> dataLakeClient.getDatabaseApi().updateDatabase(catalogId,
                dbName, database),
                Action.ALTER_DATABASE);
    }

    public void dropDatabase(String catalogId,
                             String dbName,
                             boolean deleteData,
                             boolean ignoreUnknownDb,
                             boolean cascade) throws Exception {
        call(() -> dataLakeClient.getDatabaseApi().deleteDatabase(catalogId, dbName, cascade),
                Action.DROP_DATABASE);
    }

    // ==================== Table ========================================

    public void createTable(String catalogId, TableInput tbl) throws Exception {
        call(() -> dataLakeClient.getTableApi().createTable(catalogId,
                tbl.databaseName, tbl),
                Action.CREATE_TABLE);
    }

    public Table getTable(String catalogId, String dbName, String tableName) throws Exception {
        Table result =
                call(() -> dataLakeClient.getTableApi().getTable(catalogId, dbName, tableName), Action.GET_TABLE);
        return result;
    }


    public List<String> getTables(String catalogId, String dbname, String tablePattern, int pageSize, String tableType)
            throws Exception {
        List<String> tables = new ArrayList<>();
        String nextToken = EMPTY_TOKEN;
        do {
            final String nextTok = nextToken;
            PaginatedResult<String> result =
                    call(() -> dataLakeClient.getTableApi().getTables(catalogId, dbname, tablePattern, pageSize, nextTok, tableType),
                            Action.GET_TABLES);
            nextToken = result.getNextPageToken();
            tables.addAll(result.getData());
        } while (!nextToken.equals(EMPTY_TOKEN));

        return tables;
    }

    @Override
    public List<Table> getTableObjects(String catalogId, String dbname, String tablePattern, int pageSize, String tableType) throws Exception {
        List<Table> tables = new ArrayList<>();
        String nextToken = EMPTY_TOKEN;
        do {
            final String nextTok = nextToken;
            PaginatedResult<Table> result =
                    call(() -> dataLakeClient.getTableApi().getTableObjects(catalogId, dbname, tablePattern, pageSize, nextTok, tableType),
                            Action.GET_TABLES);
            nextToken = result.getNextPageToken();
            tables.addAll(result.getData());
        } while (!nextToken.equals(EMPTY_TOKEN));
        return tables;
    }

    @Override
    public List<Table> getTableObjects(String catalogId, String dbname, List<String> tableNames) throws Exception {
        List<Table> tables =
                call(() -> dataLakeClient.getTableApi().getTableObjects(catalogId, dbname, tableNames),
                        Action.GET_TABLES);
        return tables;
    }

    public void alterTable(String catalogId,
                           String dbName,
                           String oldTableName,
                           TableInput newTable) throws Exception {
        alterTable(catalogId, dbName, oldTableName, newTable, false, false);

    }

    public void alterTable(String catalogId,
                           String dbName,
                           String oldTableName,
                           TableInput newTable,
                           boolean cascade,
                           boolean isAsync) throws Exception {
        String taskId = call(() -> dataLakeClient.getTableApi().updateTable(catalogId, dbName,
                newTable, cascade, isAsync), Action.ALTER_TABLE);
        logger.info("alterTable taskId: {}, isAsync: {}, dbName: {}, tblName: {}, cascade: {}",taskId, isAsync, dbName, oldTableName, cascade);
        checkAsyncTaskStatus(catalogId, isAsync, taskId);
    }

    public void checkAsyncTaskStatus(String catalogId, boolean isAsync, String taskId) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            if (isAsync) {
                if (taskId == null) {
                    throw new Exception("task isAsync, but cant' get taskId.");
                }
                TaskStatus resultCode = new TaskStatus();
                Long totalWaitTime = 600 * 1000L;
                Long waitPeriod = 100L;
                Long waitTime = 0L;

                while (!Thread.currentThread().isInterrupted() && waitTime <= totalWaitTime) {
                    resultCode = getCheckRenameTaskStatus(catalogId, taskId);
                    logger.info("taskId: {}, statusCode: {}, costTime: {}", taskId, resultCode.getStatus(), waitTime);
                    if ("Running".equals(resultCode.getStatus())) {
                        waitTime += waitPeriod;
                        try {
                            Thread.sleep(waitPeriod);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        break;
                    }
                }
                if (!"Success".equals(resultCode.getStatus())) {
                    throw new Exception("taskId: " + taskId + " failed: " + resultCode.getMessage());
                }
            }
        } finally {
            logger.info("taskId: {}, costTime: {}ms", taskId, System.currentTimeMillis() - startTime);
        }
    }

    public void dropTable(String catalogId, String dbName, String tableName, boolean deleteData)
            throws Exception {
        call(() -> dataLakeClient.getTableApi().deleteTable(catalogId, dbName, tableName),
                Action.DROP_TABLE);
    }

    @Override
    public void doRenameTableInMs(String catalogId, String dbName, String oldTableName, TableInput newTable, Boolean isAsync) throws Exception {
        if (newTable.getDatabaseName() == null) {
            newTable.setDatabaseName(dbName);
        }
        String taskId =  call(() -> dataLakeClient.getTableApi().renameTable(catalogId, dbName, oldTableName, newTable, isAsync), Action.RENAME_TABLE);
        logger.info("renameTable taskId: {}, isAsync: {}, from: {}.{} to {}.{}, location:{}", taskId, isAsync, dbName, oldTableName, newTable.databaseName, newTable.tableName, newTable.sd != null ? newTable.sd.location : null);
        checkAsyncTaskStatus(catalogId, isAsync, taskId);
    }

    public TaskStatus getCheckRenameTaskStatus(String catalogId, String taskId) throws Exception {
        return call(() -> dataLakeClient.getTableApi().getRenameStatus(catalogId, taskId), Action.RENAME_CHECK_STATUS);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> columnNames) throws Exception {
        List<ColumnStatisticsObj> result = new ArrayList<>(columnNames.size());
        FunctionalUtils.batchedCall(columnNames, result, tableColStatsPageSize,
                                    (batchedInput) ->  call(() -> dataLakeClient.getTableApi().getTableColumnStatistics(catalogId, dbName, tableName, batchedInput), Action.GET_TABLE_COLUMN_STATISTICS), executorService);

        return result;
    }

    @Override
    public boolean updateTableColumnStatistics(UpdateTablePartitionColumnStatisticsRequest columnStatistics) throws Exception {
        return call(() -> dataLakeClient.getTableApi().updateTableColumnStatistics(columnStatistics), Action.UPDATE_TABLE_COLUMN_STATISTICS);
    }

    @Override
    public boolean deleteTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> colNames) throws Exception {
        return call(() -> dataLakeClient.getTableApi().deleteTableColumnStatistics(catalogId, dbName, tableName, colNames), Action.DELETE_TABLE_COLUMN_STATISTICS);
    }
    // ==================== Partition ========================================

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws Exception {
        return call(() -> dataLakeClient.getPartitionApi().getPartitionColumnStatistics(catalogId, dbName, tableName,
                partitionNames, columnNames), Action.GET_PARTITION_COLUMN_STATISTICS);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> batchGetPartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws Exception {
        return call(() -> dataLakeClient.getPartitionApi().batchGetPartitionColumnStatistics(catalogId, dbName, tableName,
                partitionNames, columnNames), Action.GET_PARTITION_COLUMN_STATISTICS);
    }

    @Override
    public boolean updatePartitionColumnStatistics(UpdateTablePartitionColumnStatisticsRequest columnStatistics) throws Exception {
        return call(() -> dataLakeClient.getPartitionApi().updatePartitionColumnStatistics(columnStatistics), Action.UPDATE_PARTITION_COLUMN_STATISTICS);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partNames, List<String> colNames) throws Exception {
        return call(() -> dataLakeClient.getPartitionApi().deletePartitionColumnStatistics(catalogId, dbName, tableName, partNames, colNames), Action.DELETE_PARTITION_COLUMN_STATISTICS);
    }

    private <T, U> T iteratePartitions(DefaultDataLakeMetaStore.PartitionsFetcher<U> partitionsFetcher,
                                       DefaultDataLakeMetaStore.PartitionVisitor<T, U> resultConverter,
                                       int totalNum, int pageSize) throws Exception {
        String nextToken = EMPTY_TOKEN;
        int iteratedCount = 0;
        do {
            PaginatedResult<U> pageResult;
            if (totalNum > 0) {
                pageSize = (totalNum - iteratedCount) > pageSize ? pageSize : (totalNum - iteratedCount);
            }

            pageResult = partitionsFetcher.apply(pageSize, nextToken);
            if (pageResult.getData() != null && pageResult.getData().size() > 0) {
                resultConverter.accept(pageResult.getData());
                iteratedCount += pageResult.getData().size();
                if (totalNum > 0 && iteratedCount >= totalNum) {
                    break;
                }
            }
            nextToken = pageResult.getNextPageToken();
        } while (!nextToken.equals(EMPTY_TOKEN));

        return resultConverter.getResult();
    }

    public Partition addPartition(
            String catalogId,
            String dbName,
            String tableName,
            PartitionInput partitions,
            boolean ifNotExist,
            boolean needResult
    ) throws Exception {
        Partition result =
                call(() -> dataLakeClient.getPartitionApi().createPartition(catalogId, dbName, tableName,
                        partitions, ifNotExist, needResult), Action.CREATE_PARTITIONS);
        return result;
    }

    public List<Partition> addPartitions(
            String catalogId,
            String dbName,
            String tableName,
            List<PartitionInput> partitions,
            boolean ifNotExist,
            boolean needResult
    ) throws Exception {
        List<Partition> result = needResult? new ArrayList<>(partitions.size()): new ArrayList<>();
        try {
            FunctionalUtils.batchedCall(partitions, result, batchSize,
                    (batchedInput) ->  call(() -> dataLakeClient.getPartitionApi().batchCreatePartitions(catalogId, dbName, tableName,
                            batchedInput, ifNotExist, needResult), Action.CREATE_PARTITIONS), executorService);
        } catch (Exception exception) {
            Throwable cause = exception.getCause();
            if (cause != null && cause instanceof Exception) {
                throw (Exception) cause;
            }

            throw exception;
        }
        return result;
    }

    public void alterPartitions(
            String catalogId,
            String dbName,
            String tblName,
            List<PartitionInput> partitions
    ) throws Exception {
        try {
            FunctionalUtils.batchedRunnable(partitions, batchSize,
                    (batchedInput) ->  call(() -> dataLakeClient.getPartitionApi().batchUpdatePartitions(catalogId, dbName, tblName,
                            batchedInput), Action.ALTER_PARTITIONS), executorService);
        } catch (Exception exception) {
            Throwable cause = exception.getCause();
            if (cause != null && cause instanceof Exception) {
                throw (Exception) cause;
            }

            throw exception;
        }
    }

    public <T> T listPartitionsByExpr(String catalogId,
                                      String dbName,
                                      String tblName,
                                      byte[] expr,
                                      String defaultPartName,
                                      int max,
                                      String filter,
                                      int pageSizeIn,
                                      IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter
    ) throws Exception {
        return iteratePartitions(
                (pageSize, nextPageToken) ->
                        call(() -> dataLakeClient.getPartitionApi().listPartitionsByFilter(catalogId,
                                dbName, tblName, filter, pageSize, nextPageToken, true), Action.GET_PARTITIONS),
                resultConverter,
                max, pageSizeIn);
    }

    public void doDropPartitions(
            String catalogId,
            String dbName,
            String tableName,
            List<List<String>> partValuesList,
            boolean ifExist
    ) throws Exception {
        // Delete All Partitions or Not at All, this api has some difference with HMS client
        try {
            FunctionalUtils.batchedRunnable(partValuesList, batchSize,
                    (batchedInput) ->  call(() -> dataLakeClient.getPartitionApi().batchDeletePartitions(catalogId, dbName, tableName,
                            batchedInput, ifExist), Action.DROP_PARTITIONS), executorService);
        } catch (Exception exception) {
            Throwable cause = exception.getCause();
            if (cause != null && cause instanceof Exception) {
                throw (Exception) cause;
            }

            throw exception;
        }
    }

    public void doDropPartition(
            String catalogId,
            String dbName,
            String tableName,
            List<String> partValuesList,
            boolean ifExist
    ) throws Exception {
        // Delete All Partitions or Not at All, this api has some difference with HMS client
        call(() -> dataLakeClient.getPartitionApi().deletePartition(catalogId, dbName, tableName,
                partValuesList, ifExist), Action.DROP_PARTITIONS);
    }

    public Partition getPartition(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> parValues
    ) throws Exception {
        Partition partition =
                call(() -> dataLakeClient.getPartitionApi().getPartition(catalogId, databaseName,
                        tableName, parValues), Action.GET_PARTITION);
        return partition;
    }

    public List<Partition> getPartitionsByValues(
            String catalogId,
            String databaseName,
            String tableName,
            List<List<String>> partValuesList
    ) throws Exception {
        List<Partition> result = new ArrayList<>(partValuesList.size());
        try {
            FunctionalUtils.batchedCall(partValuesList, result, batchSize,
                    (batchedInput) ->  call(() -> dataLakeClient.getPartitionApi(). batchGetPartitions(catalogId,
                            databaseName, tableName, batchedInput, true), Action.GET_PARTITIONS), executorService);
        } catch (Exception exception) {
            Throwable cause = exception.getCause();
            if (cause != null && cause instanceof Exception) {
                throw (Exception) cause;
            }

            throw exception;
        }

        return result;
    }

    public void renamePartitionInCatalog(
            String catalogId,
            String dbName,
            String tbName,
            List<String> partitionValues,
            PartitionInput newPartition
    ) throws Exception {
        call(() -> dataLakeClient.getPartitionApi().renamePartition(catalogId, dbName,
                tbName, partitionValues, newPartition), Action.ALTER_PARTITION);
    }

    public List<String> listPartitionNames(
            String catalogId,
            String dbName,
            String tblName,
            List<String> partialPartValues,
            int max,
            int pageSizeIn
    ) throws Exception {
        DefaultDataLakeMetaStore.PartitionNameVisitor resultConverter = new DefaultDataLakeMetaStore.PartitionNameVisitor();
        return iteratePartitions(
                (pageSize, nextPageToken) ->
                        call(() -> dataLakeClient.getPartitionApi().listPartitionNames(catalogId, dbName, tblName,
                                partialPartValues, pageSize, nextPageToken), Action.LIST_PARTITIONS_NAMES),
                resultConverter,
                max, pageSizeIn);
    }

    public int getNumPartitionsByFilter(
            String catalogId,
            String dbName,
            String tblName,
            String filter,
            int pageSizeIn
    ) throws Exception {
        return iteratePartitions(
                (pageSize, nextPageToken) ->
                        call(() -> dataLakeClient.getPartitionApi().listPartitionsByFilter(catalogId,
                                dbName, tblName, filter, pageSize, nextPageToken, true), Action.GET_PARTITIONS),
                new DefaultDataLakeMetaStore.PartitionCountVisitor(), -1, pageSizeIn);
    }

    public <T> T listPartitionsInternal(
            String catalogId,
            String databaseName,
            String tableName,
            List<String> values,
            String filter,
            int max,
            int pageSizeIn,
            IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter
    ) throws Exception {

        DefaultDataLakeMetaStore.PartitionsFetcher<Partition> partitionsFetcher;
        if (DataLakeUtil.isNotBlank(filter)) {
            partitionsFetcher = (pageSize, nextPageToken) ->
                    call(() -> dataLakeClient.getPartitionApi().listPartitionsByFilter(catalogId,
                            databaseName, tableName, filter, pageSize, nextPageToken, true), Action.GET_PARTITIONS);
        } else {
            partitionsFetcher = (pageSize, nextPageToken) ->
                    call(() -> dataLakeClient.getPartitionApi().listPartitions(catalogId,
                            databaseName, tableName, values, pageSize, nextPageToken, true), Action.GET_PARTITIONS);
            ;
        }
        return iteratePartitions(partitionsFetcher, resultConverter, max, pageSizeIn);
    }

    public <T> T listPartitions(
            String catalogId,
            String databaseName,
            String tableName,
            int max,
            int pageSizeIn,
            IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter
    ) throws Exception {

        DefaultDataLakeMetaStore.PartitionsFetcher<Partition> partitionsFetcher = (pageSize, nextPageToken) ->
                call(() -> dataLakeClient.getPartitionApi().listPartitions(catalogId,
                        databaseName, tableName, new ArrayList<>(), pageSize, nextPageToken, true), Action.GET_PARTITIONS);
        return iteratePartitions(partitionsFetcher, resultConverter, max, pageSizeIn);
    }

    public List<Partition> listPartitionsByFilter(
            String catalogId,
            String databaseName,
            String tableName,
            String filter,
            int pageSizeIn
    ) throws Exception {
        List<Partition> partitions = new ArrayList<>();
        String nextToken = EMPTY_TOKEN;
        do {
            final String nextTok = nextToken;
            PaginatedResult<Partition> result =
                    call(() -> dataLakeClient.getPartitionApi().listPartitionsByFilter(catalogId,
                            databaseName, tableName, filter, pageSizeIn, nextTok, true),
                            Action.GET_PARTITIONS);
            partitions.addAll(result.getData());
            nextToken = result.getNextPageToken();
        } while (!nextToken.equals(EMPTY_TOKEN));
        return partitions;
    }

    // ==================== Function ========================================

    public void createFunction(String catalogId, FunctionInput function, String dbName) throws Exception {
        call(() -> dataLakeClient.getFunctionApi().createFunction(catalogId, dbName,
                function), Action.CREATE_FUNCTION);
    }

    public Function getFunction(String catalogId, String dbName, String functionName) throws Exception {
        Function result =
                call(() -> dataLakeClient.getFunctionApi().getFunction(catalogId, dbName, functionName),
                        Action.GET_FUNCTION);
        return result;
    }

    public List<String> getFunctions(String catalogId, String dbName, String pattern, int pageSize) throws Exception {
        String nextToken = EMPTY_TOKEN;
        List<String> functionNames = new ArrayList<>();
        do {
            final String nextTok = nextToken;
            PaginatedResult<String> result =
                    call(() -> dataLakeClient.getFunctionApi().listFunctionNames(catalogId, dbName, pattern,
                            pageSize, nextTok), Action.GET_FUNCTIONS);
            functionNames.addAll(result.getData());
            nextToken = result.getNextPageToken();
        } while (!nextToken.equals(EMPTY_TOKEN));
        return functionNames;
    }

    public List<Function> getFunctionObjects(String catalogId, String dbName, String pattern, int pageSize) throws Exception {
        String nextToken = EMPTY_TOKEN;
        List<Function> functions = new ArrayList<>();
        do {
            final String nextTok = nextToken;
            PaginatedResult<Function> result =
                    call(() -> dataLakeClient.getFunctionApi().listFunctions(catalogId, dbName, pattern,
                            pageSize, nextTok), Action.GET_FUNCTIONS);
            functions.addAll(result.getData());
            nextToken = result.getNextPageToken();
        } while (!nextToken.equals(EMPTY_TOKEN));
        return functions;
    }

    public void alterFunction(
            String catalogId,
            String dbName,
            String functionName,
            FunctionInput function
    ) throws Exception {
        call(() -> dataLakeClient.getFunctionApi().updateFunction(catalogId, dbName, functionName,
                function), Action.ALTER_FUNCTION);
    }

    public void dropFunction(String catalogId, String dbName, String functionName) throws Exception {
        call(() -> dataLakeClient.getFunctionApi().deleteFunction(catalogId, dbName, functionName),
                Action.DROP_FUNCTION);
    }

    public List<Partition> getNonSubDirectoryPartitionLocations(String catalogId,
                                                                String dbName,
                                                                String tableName,
                                                                int pageSize) throws Exception {
        String nextToken = EMPTY_TOKEN;
        List<Partition> results = new ArrayList<>();
        do {
            final String nextTok = nextToken;
            PaginatedResult<Partition> paginatedResult =
                    call(() -> dataLakeClient.getPartitionApi().listPartitions(catalogId, dbName, tableName,
                            Lists.newArrayList(), pageSize, nextTok, true), Action.GET_PARTITIONS);
            results.addAll(paginatedResult.getData());

            nextToken = paginatedResult.getNextPageToken();
        } while (!nextToken.equals(EMPTY_TOKEN));
        return results;
    }


    //------------------------------------Transaction---------------------------------
    @Override
    public LockStatus lock(List<LockObj> lockObjList) throws Exception {
        return call(() -> dataLakeClient.getTableApi().lock(lockObjList),
                Action.LOCK);
    }

    @Override
    public Boolean unLock(Long lockId) throws Exception {
        return call(() -> dataLakeClient.getTableApi().unLock(lockId), Action.UNLOCK);
    }

    @Override
    public LockStatus getLock(Long lockId) throws Exception {
        return call(() -> dataLakeClient.getTableApi().getLock(lockId), Action.GET_LOCK);
    }

    @Override
    public Boolean refreshLock(Long lockId) throws Exception {
        return call(() -> dataLakeClient.getTableApi().refreshLock(lockId), Action.REFRESH_LOCK);
    }
}
