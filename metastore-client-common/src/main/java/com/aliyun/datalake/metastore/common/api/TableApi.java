package com.aliyun.datalake.metastore.common.api;

import com.aliyun.datalake.metastore.common.entity.PaginatedResult;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.*;
import com.aliyun.teautil.models.RuntimeOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperNullString;
import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperPatternString;

public class TableApi extends AbstractBaseApi {

    private static ThreadLocal<HashMap<Long, List<String>>> threadLocalTableLockInfoReverse = new ThreadLocal<>();
    private static ThreadLocal<HashMap<String, Long>> threadLocalTableLockInfo = new ThreadLocal<>();
    private static String LOCK_HEADER_KEY = "metastore-lock-id";

    public TableApi(Client client) {
        super(client);
    }

    public ResultModel<Void> createTable(String catalogId, String databaseName, TableInput table) throws Exception {
        return call(() -> {
            CreateTableRequest request = new CreateTableRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableInput = table;

            RuntimeOptions runtime = new RuntimeOptions();
            Map<String, String> headers = new HashMap();
            setLockId(catalogId, databaseName, table.getTableName(), headers);
            CreateTableResponseBody response = ((CreateTableResponse)callWithOptions((h, r) -> client.createTableWithOptions(request, h, r), headers, runtime)).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<String> updateTable(String catalogId, String databaseName, TableInput table) throws Exception {
        return updateTable(catalogId, databaseName, table, false, false);
    }

    public ResultModel<String> updateTable(String catalogId, String databaseName, TableInput table, boolean cascade, boolean isAsync) throws Exception {
        return call(() -> {
            UpdateTableRequest request = new UpdateTableRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = table.tableName;
            table.cascade = cascade;
            request.tableInput = table;
            request.isAsync = isAsync;

            RuntimeOptions runtime = new RuntimeOptions();
            Map<String, String> headers = new HashMap();
            setLockId(catalogId, databaseName, table.getTableName(), headers);
            UpdateTableResponseBody response = ((UpdateTableResponse)callWithOptions((h, r) -> client.updateTableWithOptions(request, h, r), headers, runtime)).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.taskId);
        });
    }

    public void setLockId(String catalogId, String databaseName, String tableName, Map<String, String> headers) {
        Long lockId = getTableLock().get(toTableName(catalogId, databaseName, tableName));
        if (lockId != null && lockId > 0) {
            headers.put(LOCK_HEADER_KEY, String.valueOf(lockId));
        }
    }

    public ResultModel<Table> getTable(String catalogId, String databaseName, String tableName) throws Exception {
        return call(() -> {
            GetTableRequest request = new GetTableRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;

            RuntimeOptions runtime = new RuntimeOptions();
            Map<String, String> headers = new HashMap();
            setLockId(catalogId, databaseName, tableName, headers);
            GetTableResponseBody response = ((GetTableResponse)callWithOptions((h ,r) -> client.getTableWithOptions(request, h, r), headers, runtime)).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.table);
        });
    }

    public ResultModel<List<Table>> getTableObjectsByName(String catalogId, String databaseName,
                                                          List<String> tableNames) throws Exception {
        return call(() -> {
            List<Table> tables = new ArrayList();
            String lastReq = null;
            for (String tableName : tableNames) {
                ResultModel<Table> table = getTable(catalogId, databaseName, tableName);
                if (table.success) {
                    tables.add(table.data);
                    lastReq = table.requestId;
                } else {
                    return new ResultModel<>(false, table.code, table.message, table.requestId);
                }
            }
            return new ResultModel(true, "", "", lastReq, tables);
        });
    }

    public ResultModel<PaginatedResult<Table>> getTableObjects(String catalogId, String databaseName, String tableNamePattern,
                                                               int pageSize, String nextPageToken, String tableType) throws Exception {
        return call(() -> {
            ListTablesRequest request = new ListTablesRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableNamePattern = wrapperPatternString(tableNamePattern);
            request.nextPageToken = wrapperNullString(nextPageToken);
            request.pageSize = pageSize;
            request.tableType = tableType;
            ListTablesResponseBody response = ((ListTablesResponse)callWithOptions((h, r) -> client.listTablesWithOptions(request, h, r))).body;

            PaginatedResult<Table> result = new PaginatedResult<>(response.tables,
                    wrapperNullString(response.nextPageToken));
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public ResultModel<List<Table>> getTableObjects(String catalogId, String databaseName, List<String> tableNames) throws Exception {
        return call(() -> {
            BatchGetTablesRequest request = new BatchGetTablesRequest();
            request.setCatalogId(catalogId);
            request.setDatabaseName(databaseName);
            request.setTableNames(tableNames);
            BatchGetTablesResponseBody response = ((BatchGetTablesResponse)callWithOptions((h, r) -> client.batchGetTablesWithOptions(request, h, r))).body;

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.tables);
        });
    }


    public ResultModel<PaginatedResult<String>> getTables(String catalogId, String databaseName,
                                                          String tableNamePattern,
                                                          int pageSize, String nextPageToken, String tableType) throws Exception {
        return call(() -> {
            ListTableNamesRequest request = new ListTableNamesRequest();

            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableNamePattern = wrapperPatternString(tableNamePattern);
            request.nextPageToken = wrapperNullString(nextPageToken);
            request.pageSize = pageSize;
            request.tableType = tableType;
            ListTableNamesResponseBody response = ((ListTableNamesResponse)callWithOptions((h, r) -> client.listTableNamesWithOptions(request, h, r))).body;

            PaginatedResult<String> result = new PaginatedResult<>(response.tableNames, wrapperNullString(response.nextPageToken));
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public ResultModel<String> renameTable(String catalogId, String databaseName, String tableName, TableInput table, Boolean isAsync) throws Exception {
        return call(() -> {
            RenameTableRequest request = new RenameTableRequest();

            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.setIsAsync(isAsync);
            request.tableInput = table;

            RuntimeOptions runtime = new RuntimeOptions();
            Map<String, String> headers = new HashMap<>();
            setLockId(catalogId, databaseName, table.getTableName(), headers);
            RenameTableResponseBody response = ((RenameTableResponse)callWithOptions((h, r) -> client.renameTableWithOptions(request, h, r), headers, runtime)).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.taskId);
        });
    }

    public ResultModel<Void> deleteTable(String catalogId, String databaseName, String tableName)
            throws Exception {
        return call(() -> {
            DeleteTableRequest request = new DeleteTableRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;

            RuntimeOptions runtime = new RuntimeOptions();
            Map<String, String> headers = new HashMap();
            setLockId(catalogId, databaseName, tableName, headers);
            DeleteTableResponseBody response = ((DeleteTableResponse)callWithOptions((h, r) -> client.deleteTableWithOptions(request, h, r), headers, runtime)).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<List<ColumnStatisticsObj>> getTableColumnStatistics(
            String catalogId,
            String dbName,
            String tableName,
            List<String> columnNames
    ) throws Exception {
        return call(() -> {
            GetTableColumnStatisticsRequest request = new GetTableColumnStatisticsRequest();
            request.catalogId = catalogId;
            request.databaseName = dbName;
            request.tableName = tableName;
            request.columnNames = columnNames;
            GetTableColumnStatisticsResponseBody response = ((GetTableColumnStatisticsResponse)callWithOptions((h, r) -> client.getTableColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.columnStatisticsObjList);
        });
    }

    public ResultModel<Boolean> updateTableColumnStatistics(
            UpdateTablePartitionColumnStatisticsRequest columnStatistics
    ) throws Exception {
        return call(() -> {
            UpdateTableColumnStatisticsRequest request = new UpdateTableColumnStatisticsRequest();
            request.setUpdateTablePartitionColumnStatisticsRequest(columnStatistics);

            UpdateTableColumnStatisticsResponseBody response = ((UpdateTableColumnStatisticsResponse)callWithOptions((h, r) -> client.updateTableColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.success);
        });
    }

    public ResultModel<Boolean> deleteTableColumnStatistics(
            String catalogId
            , String dbName
            , String tableName
            , List<String> colNames
    ) throws Exception {
        return call(() -> {
            DeleteTableColumnStatisticsRequest request = new DeleteTableColumnStatisticsRequest();
            request.catalogId = catalogId;
            request.databaseName = dbName;
            request.tableName = tableName;
            request.columnNames = colNames;

            DeleteTableColumnStatisticsResponseBody response = ((DeleteTableColumnStatisticsResponse)callWithOptions((h, r) -> client.deleteTableColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.success);
        });
    }

    public ResultModel<LockStatus> lock(List<LockObj> lockObjList) throws Exception {
        return call(() -> {
            CreateLockRequest request = new CreateLockRequest();
            request.setLockObjList(lockObjList);

            CreateLockResponseBody response = ((CreateLockResponse)callWithOptions((h, r) -> client.createLockWithOptions(request, h, r))).body;
            if (response != null && response.success && response.lockStatus != null) {
                if ("ACQUIRED".equals(response.lockStatus.getLockState())) {
                    addLockId(lockObjList, response.lockStatus.getLockId());
                }
            }
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.lockStatus);
        });
    }

    public ResultModel<Boolean> unLock(Long lockId) throws Exception {
        return call(() -> {
            UnLockRequest request = new UnLockRequest();
            request.setLockId(lockId);
            removeLockId(lockId);
            UnLockResponseBody response = ((UnLockResponse)callWithOptions((h, r) -> client.unLockWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.success);
        });
    }

    public ResultModel<LockStatus> getLock(Long lockId) throws Exception {
        return call(() -> {
            GetLockRequest request = new GetLockRequest();
            request.setLockId(lockId);
            GetLockResponseBody response = ((GetLockResponse)callWithOptions((h, r) -> client.getLockWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.lockStatus);
        });
    }

    public ResultModel<Boolean> refreshLock(Long lockId) throws Exception {
        return call(() -> {
            RefreshLockRequest request = new RefreshLockRequest();
            request.setLockId(lockId);
            RefreshLockResponseBody response = ((RefreshLockResponse)callWithOptions((h, r) -> client.refreshLockWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.success);
        });
    }

    private HashMap<Long, List<String>> getTableLockReverse() {
        HashMap<Long, List<String>> lockTables = threadLocalTableLockInfoReverse.get();
        if (lockTables == null) {
            lockTables = new HashMap<>();
            threadLocalTableLockInfoReverse.set(lockTables);
        }
        return lockTables;
    }

    private HashMap<String, Long> getTableLock() {
        HashMap<String, Long> lockTables = threadLocalTableLockInfo.get();
        if (lockTables == null) {
            lockTables = new HashMap<>();
            threadLocalTableLockInfo.set(lockTables);
        }
        return lockTables;
    }

    private void removeLockId(Long lockId) {
        if (lockId != null) {
            List<String> tableNames = getTableLockReverse().get(lockId);
            if (tableNames != null) {
                for (String table : tableNames) {
                    getTableLock().remove(table);
                }
            }
            getTableLockReverse().remove(lockId);
        }
    }

    private void addLockId(List<LockObj> lockObjList, Long lockId) {
        List<String> tableNames = new ArrayList<>();
        for (LockObj lockObj : lockObjList) {
            String tableName = toTableName(lockObj.getCatalogId(), lockObj.getDatabaseName(), lockObj.getTableName());
            tableNames.add(tableName);
            getTableLock().put(tableName, lockId);
        }
        getTableLockReverse().put(lockId, tableNames);
    }

    private String toTableName(String catalogId, String databaseName, String tableName) {
        return tableName = catalogId + "." + databaseName + "." + tableName;
    }

    public ResultModel<TaskStatus> getRenameStatus(String catalogId, String taskId) throws Exception {
        return call(() -> {
            GetAsyncTaskStatusRequest getAsyncTaskStatusRequest = new GetAsyncTaskStatusRequest();
            getAsyncTaskStatusRequest.setCatalogId(catalogId);
            getAsyncTaskStatusRequest.setTaskId(taskId);
            GetAsyncTaskStatusResponseBody response = ((GetAsyncTaskStatusResponse)callWithOptions((h, r) -> client.getAsyncTaskStatusWithOptions(getAsyncTaskStatusRequest, h, r))).body;
            return new ResultModel<TaskStatus>(response.success, response.code, response.message,
                    response.requestId, response.taskStatus);
        });
    }
}
