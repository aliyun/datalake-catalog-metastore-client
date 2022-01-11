package com.aliyun.datalake.metastore.common;

import com.aliyun.datalake.metastore.common.entity.PaginatedResult;
import com.aliyun.datalake20200710.models.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface IDataLakeMetaStore {
    public void createDatabase(String catalogId, Database database) throws Exception;

    public void createDatabase(String catalogId, String dbName, String description, String location, Map<String, String> parameters, String ownerName, String ownerType, PrincipalPrivilegeSet principalPrivilegeSet) throws Exception;

    public Database getDatabase(String catalogId, String dbName) throws Exception;

    public List<String> getDatabases(String catalogId, String pattern, int pageSize) throws Exception;

    public void alterDatabase(String catalogId, String dbName, Database database) throws Exception;

    public void dropDatabase(String catalogId, String dbName, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws Exception;

    // ==================== Table ========================================

    public void createTable(String catalogId, TableInput tbl) throws Exception;

    public Table getTable(String catalogId, String dbName, String tableName) throws Exception;

    public List<String> getTables(String catalogId, String dbname, String tablePattern, int pageSize, String tableType) throws Exception;

    public List<Table> getTableObjects(String catalogId, String dbname, String tablePattern, int pageSize, String tableType) throws Exception;

    public List<Table> getTableObjects(String catalogId, String dbname, List<String> tableNames) throws Exception;

    public void alterTable(String catalogId, String dbName, String oldTableName, TableInput newTable) throws Exception;

    public void alterTable(String catalogId, String dbName, String oldTableName, TableInput newTable, boolean cascade, boolean isAsync) throws Exception;

    public void dropTable(String catalogId, String dbName, String tbName, boolean deleteData) throws Exception;

    public void doRenameTableInMs(String catalogId, String dbName, String oldTableName, TableInput newTable, Boolean isAsync) throws Exception;

    public List<ColumnStatisticsObj> getTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> columnNames) throws Exception;

    public boolean updateTableColumnStatistics(UpdateTablePartitionColumnStatisticsRequest columnStatistics) throws Exception;

    public boolean deleteTableColumnStatistics(String catalogId, String dbName, String tableName, List<String> colNames) throws Exception;

    // ==================== Partition ========================================

    public Partition addPartition(String catalogId, String dbName, String tableName, PartitionInput partitions, boolean ifNotExist, boolean needResult) throws Exception;

    public List<Partition> addPartitions(String catalogId, String dbName, String tableName, List<PartitionInput> partitions, boolean ifNotExist, boolean needResult) throws Exception;

    public void alterPartitions(String catalogId, String dbName, String tblName, List<PartitionInput> partitions) throws Exception;

    public <T> T listPartitionsByExpr(String catalogId, String dbName, String tblName, byte[] expr, String defaultPartName, int max, String filter, int pageSizeIn, IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter) throws Exception;

    public void doDropPartitions(String catalogId, String dbName, String tableName, List<List<String>> partValuesList, boolean ifExist) throws Exception;

    public Partition getPartition(String catalogId, String databaseName, String tableName, List<String> parValues) throws Exception;

    public List<Partition> getPartitionsByValues(String catalogId, String databaseName, String tableName, List<List<String>> partValuesList) throws Exception;

    public void renamePartitionInCatalog(String catalogId, String dbName, String tbName, List<String> partitionValues, PartitionInput newPartition) throws Exception;

    public List<String> listPartitionNames(String catalogId, String dbName, String tblName, List<String> partialPartValues, int max, int pageSizeIn) throws Exception;

    public int getNumPartitionsByFilter(String catalogId, String dbName, String tblName, String filter, int pageSizeIn) throws Exception;

    public <T> T listPartitionsInternal(String catalogId, String databaseName, String tableName, List<String> values, String filter, int max, int pageSizeIn, IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter) throws Exception;

    public <T> T listPartitions(String catalogId, String databaseName, String tableName, int max, int pageSizeIn, IDataLakeMetaStore.PartitionVisitor<T, Partition> resultConverter) throws Exception;

    public List<Partition> listPartitionsByFilter(String catalogId, String databaseName, String tableName, String filter, int pageSizeIn) throws Exception;

    public void doDropPartition(String catalogId, String dbName, String tableName, List<String> partValuesList, boolean ifExist) throws Exception;

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws Exception;

    public Map<String, List<ColumnStatisticsObj>> batchGetPartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws Exception;

    public boolean updatePartitionColumnStatistics(UpdateTablePartitionColumnStatisticsRequest columnStatistics) throws Exception;

    public boolean deletePartitionColumnStatistics(String catalogId, String dbName, String tableName, List<String> partNames, List<String> colNames) throws Exception;

    public void createFunction(String catalogId, FunctionInput function, String dbName) throws Exception;

    public Function getFunction(String catalogId, String dbName, String functionName) throws Exception;

    public List<String> getFunctions(String catalogId, String dbName, String pattern, int pageSize) throws Exception;

    public List<Function> getFunctionObjects(String catalogId, String dbName, String pattern, int pageSize) throws Exception;

    // ==================== Function ========================================

    public void alterFunction(String catalogId, String dbName, String functionName, FunctionInput function) throws Exception;

    public void dropFunction(String catalogId, String dbName, String functionName) throws Exception;

    public List<Partition> getNonSubDirectoryPartitionLocations(String catalogId, String dbName, String tableName, int pageSize) throws Exception;

    //-----------------------Transaction------------------------------------------
    public LockStatus lock(List<LockObj> lockObjList) throws Exception;

    public Boolean unLock(Long lockId) throws Exception;

    public LockStatus getLock(Long lockId) throws Exception;

    public Boolean refreshLock(Long lockId) throws Exception;

    @FunctionalInterface
    public interface PartitionsFetcher<T> {
        PaginatedResult<T> apply(int pageSize, String nextToken) throws Exception;
    }

    public interface PartitionVisitor<T, U> {
        void accept(List<U> partition);

        T getResult();
    }

    public static class PartitionCountVisitor implements PartitionVisitor<Integer, Partition> {
        private int count;

        public PartitionCountVisitor() {
            count = 0;
        }

        @Override
        public void accept(List<Partition> partition) {
            count += partition.size();
        }

        @Override
        public Integer getResult() {
            return count;
        }
    }

    public static class PartitionNameVisitor implements PartitionVisitor<List<String>, String> {
        private final List<String> result;

        PartitionNameVisitor() {
            result = new ArrayList<>();
        }

        @Override
        public void accept(List<String> partitions) {
            result.addAll(partitions);
        }

        @Override
        public List<String> getResult() {
            return result;
        }
    }
}
