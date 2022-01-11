package com.aliyun.datalake.metastore.common.api;

import com.aliyun.datalake.metastore.common.entity.PaginatedResult;
import com.aliyun.datalake.metastore.common.entity.PartitionBatchResultModel;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.*;
import com.aliyun.datalake20200710.models.BatchDeletePartitionsRequest.BatchDeletePartitionsRequestPartitionValueList;
import com.aliyun.datalake20200710.models.BatchGetPartitionsRequest.BatchGetPartitionsRequestPartitionValueList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperNullList;
import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperNullString;

public class PartitionApi extends AbstractBaseApi {

    public PartitionApi(Client client) {
        super(client);
    }

    public ResultModel<Partition> getPartition(String catalogId, String databaseName, String tableName,
                                               List<String> values) throws Exception {
        return call(() -> {
            GetPartitionRequest request = new GetPartitionRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partitionValues = values;

            GetPartitionResponseBody response = ((GetPartitionResponse)callWithOptions((h, r) -> client.getPartitionWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.partition);
        });
    }

    public PartitionBatchResultModel<List<Partition>> batchGetPartitions(String catalogId,
                                                                         String databaseName,
                                                                         String tableName,
                                                                         List<List<String>> partValuesList,
                                                                         boolean isShareSd) throws Exception {
        return batchCall(() -> {
            BatchGetPartitionsRequest request = new BatchGetPartitionsRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partitionValueList = new ArrayList<>();
            request.isShareSd = isShareSd;

            for (List<String> strings : partValuesList) {
                BatchGetPartitionsRequestPartitionValueList valueList = new BatchGetPartitionsRequestPartitionValueList();
                valueList.values = strings;
                request.partitionValueList.add(valueList);
            }

            BatchGetPartitionsResponseBody response = ((BatchGetPartitionsResponse)callWithOptions((h, r) -> client.batchGetPartitionsWithOptions(request, h, r))).body;
            combinePartitionsWithShareSd(isShareSd, response.partitions, response.partitionSpecs);

            return new PartitionBatchResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.partitions, response.partitionErrors);
        }, new PartitionBatchResultModel());
    }

    public PartitionBatchResultModel<List<Partition>> batchGetPartitions(String catalogId,
                                                                         String databaseName,
                                                                         String tableName,
                                                                         List<List<String>> partValuesList) throws Exception {
        return batchGetPartitions(catalogId, databaseName, tableName, partValuesList, true);
    }

    public ResultModel<Partition> createPartition(String catalogId,
                                                  String databaseName,
                                                  String tableName,
                                                  PartitionInput partition,
                                                  boolean ifNotExists,
                                                  Boolean needResult) throws Exception {
        return call(() -> {
            CreatePartitionRequest request = new CreatePartitionRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partitionInput = partition;
            request.ifNotExists = ifNotExists;
            request.needResult = needResult;

            CreatePartitionResponseBody response = ((CreatePartitionResponse)callWithOptions((h, r) -> client.createPartitionWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.partition);
        });
    }


    public PartitionBatchResultModel<List<Partition>> batchCreatePartitions(String catalogId,
                                                                            String databaseName,
                                                                            String tableName,
                                                                            List<PartitionInput> partitions,
                                                                            boolean ifNotExists,
                                                                            Boolean needResult) throws Exception {
        return batchCall(() -> {
            BatchCreatePartitionsRequest request = new BatchCreatePartitionsRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.ifNotExists = ifNotExists;
            request.needResult = needResult;
            request.partitionInputs = partitions;

            BatchCreatePartitionsResponseBody response = ((BatchCreatePartitionsResponse)callWithOptions((h, r) -> client.batchCreatePartitionsWithOptions(request, h, r))).body;

            return new PartitionBatchResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.partitions, response.partitionErrors);
        }, new PartitionBatchResultModel());
    }

    public ResultModel<Void> deletePartition(String catalogId,
                                             String databaseName,
                                             String tableName,
                                             List<String> partValues,
                                             boolean ifExists) throws Exception {
        return call(() -> {
            DeletePartitionRequest request = new DeletePartitionRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.ifExists = ifExists;
            request.partitionValues = wrapperNullList(partValues);

            DeletePartitionResponseBody response = ((DeletePartitionResponse)callWithOptions((h, r) -> client.deletePartitionWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<Void> renamePartition(String catalogName,
                                             String databaseName,
                                             String tableName,
                                             List<String> partitionValues,
                                             PartitionInput partition) throws Exception {
        return call(() -> {
            RenamePartitionRequest request = new RenamePartitionRequest();
            request.catalogId = catalogName;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partitionValues = wrapperNullList(partitionValues);
            request.partitionInput = partition;

            RenamePartitionResponseBody response = ((RenamePartitionResponse)callWithOptions((h, r) -> client.renamePartitionWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<PaginatedResult<String>> listPartitionNames(String catalogId,
                                                                   String databaseName,
                                                                   String tableName,
                                                                   List<String> partialPartValues,
                                                                   int pageSize,
                                                                   String nextPageToken) throws Exception {
        return call(() -> {
            ListPartitionNamesRequest request = new ListPartitionNamesRequest();

            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partialPartValues = wrapperNullList(partialPartValues);
            request.nextPageToken = wrapperNullString(nextPageToken);
            request.pageSize = pageSize;

            ListPartitionNamesResponseBody response = ((ListPartitionNamesResponse)callWithOptions((h, r) -> client.listPartitionNamesWithOptions(request, h, r))).body;

            PaginatedResult<String> result = new PaginatedResult<>(response.partitionNames, wrapperNullString(response.nextPageToken));
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public ResultModel<PaginatedResult<Partition>> listPartitions(String catalogId,
                                                                  String databaseName,
                                                                  String tableName,
                                                                  List<String> partialPartValues,
                                                                  int pageSize,
                                                                  String nextPageToken,
                                                                  boolean isShareSd) throws Exception {
        return call(() -> {
            ListPartitionsRequest request = new ListPartitionsRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partialPartValues = wrapperNullList(partialPartValues);
            request.pageSize = pageSize;
            request.nextPageToken = wrapperNullString(nextPageToken);
            request.isShareSd = isShareSd;

            ListPartitionsResponseBody response = ((ListPartitionsResponse)callWithOptions((h, r) -> client.listPartitionsWithOptions(request, h, r))).body;

            combinePartitionsWithShareSd(isShareSd, response.partitions, response.partitionSpecs);

            PaginatedResult<Partition> result = new PaginatedResult<>(response.partitions,
                    wrapperNullString(response.nextPageToken));
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public ResultModel<PaginatedResult<Partition>> listPartitions(String catalogId,
                                                                  String databaseName,
                                                                  String tableName,
                                                                  List<String> partialPartValues,
                                                                  int pageSize,
                                                                  String nextPageToken) throws Exception {
        return listPartitions(catalogId, databaseName, tableName, partialPartValues, pageSize, nextPageToken, true);
    }

    public ResultModel<PaginatedResult<Partition>> listPartitionsByFilter(String catalogId,
                                                                          String databaseName,
                                                                          String tableName,
                                                                          String filter,
                                                                          int pageSize,
                                                                          String nextPageToken,
                                                                          boolean isShareSd) throws Exception {
        return call(() -> {
            ListPartitionsByFilterRequest request = new ListPartitionsByFilterRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.filter = filter;
            request.pageSize = pageSize;
            request.nextPageToken = wrapperNullString(nextPageToken);
            request.isShareSd = isShareSd;

            ListPartitionsByFilterResponseBody response = ((ListPartitionsByFilterResponse)callWithOptions((h, r) -> client.listPartitionsByFilterWithOptions(request, h, r))).body;

            combinePartitionsWithShareSd(isShareSd, response.partitions, response.partitionSpecs);

            PaginatedResult<Partition> result = new PaginatedResult<>(response.partitions, response.nextPageToken);

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public void combinePartitionsWithShareSd(boolean isShareSd, List<Partition> partitions, List<PartitionSpec> partitionSpecs) {
        if (isShareSd && partitionSpecs != null) {
            partitionSpecs.stream().forEach(partitionSpec -> partitionSpec.getSharedSDPartitions().stream().forEach(p -> {
                p.getSd().setCols(partitionSpec.getSharedStorageDescriptor().cols);
                p.getSd().setLocation(partitionSpec.getSharedStorageDescriptor().getLocation() + p.getSd().getLocation());
                partitions.add(p);
            }));
        }
    }

    public ResultModel<PaginatedResult<Partition>> listPartitionsByFilter(String catalogId,
                                                                          String databaseName,
                                                                          String tableName,
                                                                          String filter,
                                                                          int pageSize,
                                                                          String nextPageToken) throws Exception {
        return listPartitionsByFilter(catalogId, databaseName, tableName, filter, pageSize, nextPageToken, false);
    }


    public PartitionBatchResultModel<Void> batchDeletePartitions(String catalogId,
                                                                 String databaseName,
                                                                 String tableName,
                                                                 List<List<String>> partValuesList,
                                                                 boolean ifExists) throws Exception {
        return batchCall(() -> {
            BatchDeletePartitionsRequest request = new BatchDeletePartitionsRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partitionValueList = new ArrayList<>();
            request.ifExists = ifExists;

            for (List<String> strings : partValuesList) {
                BatchDeletePartitionsRequestPartitionValueList valueList = new BatchDeletePartitionsRequestPartitionValueList();
                valueList.values = strings;
                request.partitionValueList.add(valueList);
            }

            BatchDeletePartitionsResponseBody response = ((BatchDeletePartitionsResponse)callWithOptions((h, r) -> client.batchDeletePartitionsWithOptions(request, h, r))).body;
            return new PartitionBatchResultModel<>(response.success, response.code, response.message,
                    response.requestId, null, response.partitionErrors);
        }, new PartitionBatchResultModel());
    }

    public PartitionBatchResultModel<Void> batchUpdatePartitions(String catalogId,
                                                                 String databaseName,
                                                                 String tableName,
                                                                 List<PartitionInput> partitions) throws Exception {
        return batchCall(() -> {
            BatchUpdatePartitionsRequest request = new BatchUpdatePartitionsRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.tableName = tableName;
            request.partitionInputs = partitions;

            BatchUpdatePartitionsResponseBody response = ((BatchUpdatePartitionsResponse)callWithOptions((h, r) -> client.batchUpdatePartitionsWithOptions(request, h, r))).body;
            return new PartitionBatchResultModel<>(response.success, response.code, response.message,
                    response.requestId, null, response.partitionErrors);
        }, new PartitionBatchResultModel());
    }

    public ResultModel<Map<String, List<ColumnStatisticsObj>>> getPartitionColumnStatistics(
            String catalogId,
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws Exception {
        return call(() -> {
            GetPartitionColumnStatisticsRequest request = new GetPartitionColumnStatisticsRequest();
            request.catalogId = catalogId;
            request.databaseName = dbName;
            request.tableName = tableName;
            request.partitionNames = partitionNames;
            request.columnNames = columnNames;
            GetPartitionColumnStatisticsResponseBody response = ((GetPartitionColumnStatisticsResponse)callWithOptions((h, r) -> client.getPartitionColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.partitionStatisticsMap);
        });
    }

    public ResultModel<Map<String, List<ColumnStatisticsObj>>> batchGetPartitionColumnStatistics(
            String catalogId,
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws Exception {
        return call(() -> {
            BatchGetPartitionColumnStatisticsRequest request = new BatchGetPartitionColumnStatisticsRequest();
            request.catalogId = catalogId;
            request.databaseName = dbName;
            request.tableName = tableName;
            request.partitionNames = partitionNames;
            request.columnNames = columnNames;
            BatchGetPartitionColumnStatisticsResponseBody response = ((BatchGetPartitionColumnStatisticsResponse)callWithOptions((h, r) -> client.batchGetPartitionColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.partitionStatisticsMap);
        });
    }

    public ResultModel<Boolean> updatePartitionColumnStatistics(
            UpdateTablePartitionColumnStatisticsRequest columnStatistics
    ) throws Exception {
        return call(() -> {
            UpdatePartitionColumnStatisticsRequest request = new UpdatePartitionColumnStatisticsRequest();
            request.setUpdateTablePartitionColumnStatisticsRequest(columnStatistics);
            UpdatePartitionColumnStatisticsResponseBody response = ((UpdatePartitionColumnStatisticsResponse)callWithOptions((h, r) -> client.updatePartitionColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.success);
        });
    }

    public ResultModel<Boolean> deletePartitionColumnStatistics(
            String catalogId
            , String dbName
            , String tableName
            , List<String> partNames
            , List<String> colNames
    ) throws Exception {
        return call(() -> {
            DeletePartitionColumnStatisticsRequest request = new DeletePartitionColumnStatisticsRequest();
            request.catalogId = catalogId;
            request.databaseName = dbName;
            request.tableName = tableName;
            request.partitionNames = partNames;
            request.columnNames = colNames;
            DeletePartitionColumnStatisticsResponseBody response = ((DeletePartitionColumnStatisticsResponse)callWithOptions((h, r) -> client.deletePartitionColumnStatisticsWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.success);
        });
    }
}
