package com.aliyun.datalake.metastore.common.api;


import com.aliyun.datalake.metastore.common.entity.PaginatedResult;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.*;

import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperNullString;
import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperPatternString;

public class FunctionApi extends AbstractBaseApi {

    public FunctionApi(Client client) {
        super(client);
    }

    public ResultModel<Function> getFunction(String catalogId, String databaseName, String functionName) throws Exception {
        return call(() -> {
            GetFunctionRequest request = new GetFunctionRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.functionName = functionName;

            GetFunctionResponseBody response = ((GetFunctionResponse)callWithOptions((h, r) -> client.getFunctionWithOptions(request, h, r))).body;

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.function);
        });
    }

    public ResultModel<Void> createFunction(String catalogId,
                                            String databaseName,
                                            FunctionInput functionInput) throws Exception {
        return call(() -> {
            CreateFunctionRequest request = new CreateFunctionRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.functionInput = functionInput;

            CreateFunctionResponseBody response = ((CreateFunctionResponse) callWithOptions((h, r) -> client.createFunctionWithOptions(request, h, r))).body;

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<Void> updateFunction(String catalogId,
                                            String databaseName,
                                            String functionName,
                                            FunctionInput functionInput) throws Exception {
        return call(() -> {
            UpdateFunctionRequest request = new UpdateFunctionRequest();

            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.functionName = functionName;

            request.functionInput = functionInput;

            UpdateFunctionResponseBody response = ((UpdateFunctionResponse)callWithOptions((h, r) -> client.updateFunctionWithOptions(request, h, r))).body;

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<Void> deleteFunction(String catalogId, String databaseName, String functionName) throws Exception {
        return call(() -> {
            DeleteFunctionRequest request = new DeleteFunctionRequest();

            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.functionName = functionName;

            DeleteFunctionResponseBody response = ((DeleteFunctionResponse)callWithOptions((h, r) -> client.deleteFunctionWithOptions(request, h, r))).body;

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<PaginatedResult<String>> listFunctionNames(String catalogId, String databaseName,
                                                                  String functionNamePattern, int pageSize,
                                                                  String nextPageToken) throws Exception {
        return call(() -> {
            ListFunctionNamesRequest request = new ListFunctionNamesRequest();
            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.functionNamePattern = wrapperPatternString(functionNamePattern);
            request.pageSize = pageSize;
            request.nextPageToken = wrapperNullString(nextPageToken);

            ListFunctionNamesResponseBody response = ((ListFunctionNamesResponse)callWithOptions((h, r) -> client.listFunctionNamesWithOptions(request, h, r))).body;
            PaginatedResult<String> result = new PaginatedResult<>(response.functionNames,
                    wrapperNullString(response.nextPageToken));

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public ResultModel<PaginatedResult<Function>> listFunctions(String catalogId, String databaseName,
                                                                String functionNamePattern, int pageSize,
                                                                String nextPageToken) throws Exception {
        return call(() -> {
            ListFunctionsRequest request = new ListFunctionsRequest();

            request.catalogId = catalogId;
            request.databaseName = databaseName;
            request.functionNamePattern = wrapperPatternString(functionNamePattern);
            ;
            request.pageSize = pageSize;
            request.nextPageToken = wrapperNullString(nextPageToken);

            ListFunctionsResponseBody response = ((ListFunctionsResponse)callWithOptions((h, r) -> client.listFunctionsWithOptions(request, h, r))).body;
            PaginatedResult<Function> result = new PaginatedResult<>(response.functions,
                    wrapperNullString(response.nextPageToken));

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }
}
