package com.aliyun.datalake.metastore.common;

import com.aliyun.datalake.metastore.common.api.DatabaseApi;
import com.aliyun.datalake.metastore.common.api.FunctionApi;
import com.aliyun.datalake.metastore.common.api.PartitionApi;
import com.aliyun.datalake.metastore.common.api.TableApi;
import com.aliyun.datalake20200710.Client;
import com.aliyun.teaopenapi.models.Config;

public class DataLakeClient {

    private final Client client;
    private final DatabaseApi databaseApi;
    private final TableApi tableApi;
    private final FunctionApi functionApi;
    private final PartitionApi partitionApi;

    public DataLakeClient(String regionId, String accessKeyId, String accessKeySecret, String endpoint) throws Exception {
        this(regionId, accessKeyId, accessKeySecret, endpoint, accessKeySecret);
    }

    public DataLakeClient(Config config) throws Exception {
        client = new Client(config);
        databaseApi = new DatabaseApi(client);
        tableApi = new TableApi(client);
        functionApi = new FunctionApi(client);
        partitionApi = new PartitionApi(client);
    }

    public DataLakeClient(String regionId, String accessKeyId, String accessKeySecret, String endpoint,
                          String securityToken) throws Exception {
        Config config = new Config();
        config.accessKeyId = accessKeyId;
        config.accessKeySecret = accessKeySecret;
        config.endpoint = endpoint;
        config.regionId = regionId;
        config.securityToken = securityToken;
        client = new Client(config);
        databaseApi = new DatabaseApi(client);
        tableApi = new TableApi(client);
        functionApi = new FunctionApi(client);
        partitionApi = new PartitionApi(client);
    }

    public DatabaseApi getDatabaseApi() {
        return databaseApi;
    }

    public TableApi getTableApi() {
        return tableApi;
    }

    public FunctionApi getFunctionApi() {
        return functionApi;
    }

    public PartitionApi getPartitionApi() {
        return partitionApi;
    }

}
