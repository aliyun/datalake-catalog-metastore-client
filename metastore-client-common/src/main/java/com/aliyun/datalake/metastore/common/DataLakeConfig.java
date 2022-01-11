package com.aliyun.datalake.metastore.common;

/**
 * This class interacts with customized hive config properties
 * such as getting metastore region, AK information, etc.
 */
public class DataLakeConfig {
    // public configs
    public static final String CATALOG_REGION_ID = "dlf.catalog.region";
    public static final String CATALOG_ENDPOINT = "dlf.catalog.endpoint";
    public static final String CATALOG_ACCESS_KEY_ID = "dlf.catalog.accessKeyId";
    public static final String CATALOG_ACCESS_KEY_SECRET = "dlf.catalog.accessKeySecret";
    public static final String CATALOG_USER_ID = "dlf.catalog.uid";
    public static final String CATALOG_ROLE = "dlf.catalog.role";
    public static final String CATALOG_PAGE_SIZE = "dlf.catalog.pageSize";
    public static final String CATALOG_ACCURATE_BATCH_SIZE = "dlf.catalog.accurate.batchSize";
    public static final String CATALOG_AK_MODE = "dlf.catalog.akMode";
    public static final String CATALOG_STS_IS_NEW_MODE = "dlf.catalog.sts.isNewMode";

    public static final String CATALOG_PROXY_MODE = "dlf.catalog.proxyMode";
    public static final String CATALOG_PROXY_LOGSTORE = "dlf.catalog.proxyLogStore";
    public static final String CATALOG_ACTION_LOG_ENABLED = "dlf.catalog.action.log.enabled";
    public static final boolean DEFAULT_CATALOG_ACTION_LOG_ENABLED = false;
    public static final String CATALOG_LOG_ENABLED = "dlf.catalog.log.enabled";
    public static final boolean DEFAULT_CATALOG_LOG_ENABLED = true;

    public static final String CATALOG_ASSUME_CANONICAL_PARTITION_KEYS = "dlf.catalog.assume-canonical-partition-keys";

    public static final String CATALOG_CREATE_DEFAULT_DB = "dlf.catalog.createDefaultDBIfNotExist";
    public static final String CATALOG_SERVER_CONN_TIMEOUT_MILLS = "dlf.catalog.server.conn.timeout.mills";
    public static final String CATALOG_SERVER_READ_TIMEOUT_MILLS = "dlf.catalog.server.read.timeout.mills";
    public static final String CATALOG_CLIENT_NUM_THREADS = "dlf.catalog.client.threads";
    public static final String CATALOG_COL_STATS_PAGE_SIZE = "dlf.catalog.client.col.stats.pageSize";
    public static final String CATALOG_TABLE_COL_STATS_PAGE_SIZE = "dlf.catalog.client.table.col.stats.pageSize";
    public static final String CATALOG_SECURITY_TOKEN = "dlf.catalog.securityToken";
    public static final AKMode DEFAULT_AK_MODE = AKMode.MANUAL;
    public static final int DEFAULT_CATALOG_PAGE_SIZE = 500;
    public static final int DEFAULT_CATALOG_COL_STATS_PAGE_SIZE = 1000;
    public static final int DEFAULT_CATALOG_TABLE_COL_STATS_PAGE_SIZE = 50;
    public static final String DEFAULT_WAREHOUSE_DIR_KEY = "dlf.catalog.default-warehouse-dir";
    public static final String DEFAULT_WAREHOUSE_DIR_VALUE = "/user/hive/warehouse";
    public static final String DELETE_DIR_WHEN_DROP_SCHEMA_KEY = "dlf.catalog.delete-dir-when-drop-schema";
    public static final boolean DELETE_DIR_WHEN_DROP_SCHEMA_VALUE = true;

    public static final int MAX_RETRY_TIMES = 3;

    public static final int DEFAULT_NUM_EXECUTOR_THREADS = 5;
    // according to hive default retrieve size, default to 60 is better for large table
    public static final int DEFAULT_CATALOG_ACCURATE_BATCH_SIZE = 300 / DEFAULT_NUM_EXECUTOR_THREADS;

    public static final int DEFAULT_CATALOG_SERVER_CONN_TIMEOUT_MILLS = 30 * 1000;
    public static final int DEFAULT_CATALOG_SERVER_READ_TIMEOUT_MILLS = 30 * 1000;
    public static final int MAX_CATALOG_SERVER_CONN_TIMEOUT_MILLS = 60 * 1000;
    public static final int MAX_CATALOG_SERVER_READ_TIMEOUT_MILLS = 60 * 1000;

    // index configs
    public static final String INDEX_DEFERRED_REBUILD = "DeferredRebuild";
    public static final String INDEX_TABLE_NAME = "IndexTableName";
    public static final String INDEX_HANDLER_CLASS = "IndexHandlerClass";
    public static final String INDEX_DB_NAME = "DbName";
    public static final String INDEX_ORIGIN_TABLE_NAME = "OriginTableName";

    // internal configs
    public static final String CATALOG_ID = "dlf.catalog.id";
    public static final String DEFAULT_CATALOG_ID = "";
    // for kerberos
    public static final String HIVE_METASTORE_AUTHENTICATION_TYPE = "hive.metastore.authentication.type";
    public static final String HIVE_METASTORE_SERVICE_CLIENT_PRINCIPAL = "hive.metastore.client.principal";
    public static final String HIVE_METASTORE_SERVICE_PRINCIPAL = "hive.metastore.service.principal";
    public static final String HIVE_METASTORE_CLIENT_KEYTAB = "hive.metastore.client.keytab";
    public static final String ENABLE_BIT_VECTOR = "dlf.catalog.enable.bit.vector";
    public static final String ENABLE_FILE_OPERATION = "dlf.catalog.enable.file.operation";
    public static final String ENABLE_FILE_OPERATION_GRAY_RATE = "dlf.catalog.enable.file.operation.gray.rate";

    public enum AKMode {
        MANUAL,
        EMR_AUTO,
        DLF_AUTO,
        CUPID
    }

    public static final String CATALOG_DEFAULT_DB_CREATE_TABLE_USE_CURRENT_DB_LOCATION = "dlf.catalog.defaultdb.createtable.use.current.db.location";
}
