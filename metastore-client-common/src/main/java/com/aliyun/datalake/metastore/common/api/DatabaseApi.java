package com.aliyun.datalake.metastore.common.api;

import com.aliyun.datalake.metastore.common.entity.PaginatedResult;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperNullString;
import static com.aliyun.datalake.metastore.common.util.DataLakeUtil.wrapperPatternString;

public class DatabaseApi extends AbstractBaseApi {

    public DatabaseApi(Client client) {
        super(client);
    }

    public ResultModel<Database> getDatabase(String catalogId, String databaseName) throws Exception {
        return call(() -> {
            GetDatabaseRequest query = new GetDatabaseRequest();
            query.catalogId = catalogId;
            query.name = databaseName;

            GetDatabaseResponseBody response = ((GetDatabaseResponse)callWithOptions((h, r) -> client.getDatabaseWithOptions(query, h, r))).body;

            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, response.database);
        });
    }

    public ResultModel<Void> createDatabase(String catalogId, String databaseName, String description,
                                            String locationUri, Map<String, String> parameters,
                                            String ownerName, String ownerType, PrincipalPrivilegeSet privileges) throws Exception {
        return call(() -> {
            CreateDatabaseRequest request = new CreateDatabaseRequest();
            request.catalogId = catalogId;

            DatabaseInput input = new DatabaseInput();
            input.description = description;
            input.locationUri = locationUri;
            input.parameters = parameters;
            input.name = databaseName;
            input.ownerName = ownerName;
            input.ownerType = ownerType;
            input.privileges = privileges;

            request.databaseInput = input;

            CreateDatabaseResponseBody response = ((CreateDatabaseResponse)callWithOptions((h, r) -> client.createDatabaseWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<List<String>> listAllDatabases(String catalogId) throws Exception {
        return call(() -> {
            ListDatabasesRequest request = new ListDatabasesRequest();
            request.catalogId = catalogId;
            request.pageSize = -1;
            request.nextPageToken = "";
            request.namePattern = ".*";

            ListDatabasesResponseBody response = ((ListDatabasesResponse)callWithOptions((h, r) -> client.listDatabasesWithOptions(request, h, r))).body;
            List<String> data = null;
            if (response.success) {
                List<Database> databases = response.databases;
                data = databases.stream().map(database -> database.name).collect(Collectors.toList());
            }
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, data);
        });
    }

    public ResultModel<PaginatedResult<Database>> listDatabases(String catalogId, String databasePattern,
                                                                int pageSize,
                                                                String nextPageToken) throws Exception {
        return call(() -> {
            ListDatabasesRequest request = new ListDatabasesRequest();
            request.catalogId = catalogId;
            request.namePattern = wrapperPatternString(databasePattern);
            request.pageSize = pageSize;
            request.nextPageToken = wrapperNullString(nextPageToken);

            ListDatabasesResponseBody response = ((ListDatabasesResponse)callWithOptions((h, r) -> client.listDatabasesWithOptions(request, h, r))).body;

            PaginatedResult<Database> result = new PaginatedResult<>(response.databases, wrapperNullString(response.nextPageToken));
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId, result);
        });
    }

    public ResultModel<Void> updateDatabase(String catalogId, String databaseName, Database database) throws Exception {
        return call(() -> {
            UpdateDatabaseRequest request = new UpdateDatabaseRequest();
            request.catalogId = catalogId;
            request.name = databaseName;
            DatabaseInput input = new DatabaseInput();
            input.name = database.name;
            input.description = database.description;
            input.locationUri = database.locationUri;
            input.parameters = database.parameters;
            input.ownerName = database.ownerName;
            input.ownerType = database.ownerType;
            input.privileges = database.privileges;

            request.databaseInput = input;
            UpdateDatabaseResponseBody response = ((UpdateDatabaseResponse)callWithOptions((h, r) -> client.updateDatabaseWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }

    public ResultModel<Void> deleteDatabase(String catalogId, String databaseName, Boolean cascade) throws Exception {
        return call(() -> {
            DeleteDatabaseRequest request = new DeleteDatabaseRequest();
            request.catalogId = catalogId;
            request.name = databaseName;
            request.cascade = cascade;

            DeleteDatabaseResponseBody response = ((DeleteDatabaseResponse)callWithOptions((h, r) -> client.deleteDatabaseWithOptions(request, h, r))).body;
            return new ResultModel<>(response.success, response.code, response.message,
                    response.requestId);
        });
    }
}
