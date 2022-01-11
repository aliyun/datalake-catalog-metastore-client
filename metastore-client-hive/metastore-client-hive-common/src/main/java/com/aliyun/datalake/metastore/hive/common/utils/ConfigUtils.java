package com.aliyun.datalake.metastore.hive.common.utils;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.ProxyMode;
import org.apache.hadoop.conf.Configuration;

import java.util.Optional;

import static com.aliyun.datalake.metastore.common.DataLakeConfig.*;

public class ConfigUtils {
    public static ProxyMode getProxyMode(Configuration conf) {
        String proxyModeString = conf.get(CATALOG_PROXY_MODE, ProxyMode.DLF_ONLY.name());
        return ProxyMode.valueOf(proxyModeString);
    }

    public static Optional<String> getAccessKeyId(Configuration conf) {
        String keyId = conf.get(CATALOG_ACCESS_KEY_ID);
        if (keyId == null || keyId.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(keyId);
        }
    }

    public static Optional<String> getAccessKeySecret(Configuration conf) {
        String keySecret = conf.get(CATALOG_ACCESS_KEY_SECRET);
        if (keySecret == null || keySecret.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(keySecret);
        }
    }

    public static Optional<String> getRegionId(Configuration conf) {
        String regionId = conf.get(CATALOG_REGION_ID);
        if (regionId == null || regionId.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(regionId);
        }
    }

    public static Optional<String> getSecurityToken(Configuration conf) {
        String securityToken = conf.get(CATALOG_SECURITY_TOKEN);
        if (securityToken == null || securityToken.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(securityToken);
        }
    }

    public static Optional<String> getEndPoint(Configuration conf) {
        String endpoint = conf.get(CATALOG_ENDPOINT);
        if (endpoint == null || endpoint.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(endpoint);
        }
    }

    public static String getCatalogId(Configuration conf) {
        String catalog = conf.get(CATALOG_ID);
        if (catalog == null || catalog.isEmpty()) {
            return DEFAULT_CATALOG_ID;
        }
        return catalog;
    }

    public static short getPageSize(Configuration conf) {
        try {
            int size = conf.getInt(CATALOG_PAGE_SIZE, DEFAULT_CATALOG_PAGE_SIZE);
            if (size > Short.MAX_VALUE || size < 0) {
                return DEFAULT_CATALOG_PAGE_SIZE;
            } else {
                return (short) size;
            }
        } catch (Exception e) {
            return DEFAULT_CATALOG_PAGE_SIZE;
        }
    }

    public static int getAccurateListBatchSize(Configuration conf) {
        int size;
        try {
            size = conf.getInt(CATALOG_ACCURATE_BATCH_SIZE, DEFAULT_CATALOG_ACCURATE_BATCH_SIZE);
            if (size > Short.MAX_VALUE || size < 0) {
                size = DEFAULT_CATALOG_ACCURATE_BATCH_SIZE;
            }
        } catch (Exception e) {
            size = DEFAULT_CATALOG_ACCURATE_BATCH_SIZE;
        }

        return size;
    }

    public static short getColStatsPageSize(Configuration conf) {
        try {
            int size = conf.getInt(CATALOG_COL_STATS_PAGE_SIZE, DEFAULT_CATALOG_COL_STATS_PAGE_SIZE);
            if (size > Short.MAX_VALUE || (size < 0 && size != -1)) {
                return DEFAULT_CATALOG_COL_STATS_PAGE_SIZE;
            } else {
                return (short) size;
            }
        } catch (Exception e) {
            return DEFAULT_CATALOG_COL_STATS_PAGE_SIZE;
        }
    }

    public static short getTableColStatsPageSize(Configuration conf) {
        try {
            int size = conf.getInt(CATALOG_TABLE_COL_STATS_PAGE_SIZE, DEFAULT_CATALOG_TABLE_COL_STATS_PAGE_SIZE);
            if (size > Short.MAX_VALUE || (size < 0 && size != -1)) {
                return DEFAULT_CATALOG_TABLE_COL_STATS_PAGE_SIZE;
            } else {
                return (short) size;
            }
        } catch (Exception e) {
            return DEFAULT_CATALOG_TABLE_COL_STATS_PAGE_SIZE;
        }
    }



    public static AKMode akMode(Configuration conf) {
        if (conf.get(CATALOG_AK_MODE) != null) {
            return AKMode.valueOf(conf.get(CATALOG_AK_MODE));
        } else {
            return DEFAULT_AK_MODE;
        }
    }

    public static boolean isNewSTSMode(Configuration conf) {
        return conf.getBoolean(CATALOG_STS_IS_NEW_MODE, true);
    }

    public static String getUserId(Configuration conf) {
        return conf.get(DataLakeConfig.CATALOG_USER_ID);
    }

    public static int getReadTimeout(Configuration conf) {
        try {
            int timeout = conf.getInt(CATALOG_SERVER_READ_TIMEOUT_MILLS, DEFAULT_CATALOG_SERVER_READ_TIMEOUT_MILLS);
            if (timeout > MAX_CATALOG_SERVER_READ_TIMEOUT_MILLS || timeout < 0) {
                return DEFAULT_CATALOG_SERVER_READ_TIMEOUT_MILLS;
            } else {
                return timeout;
            }
        } catch (Exception e) {
            return DEFAULT_CATALOG_SERVER_READ_TIMEOUT_MILLS;
        }
    }

    public static int getConnTimeout(Configuration conf) {
        try {
            int timeout = conf.getInt(CATALOG_SERVER_CONN_TIMEOUT_MILLS, DEFAULT_CATALOG_SERVER_CONN_TIMEOUT_MILLS);
            if (timeout > MAX_CATALOG_SERVER_CONN_TIMEOUT_MILLS || timeout < 0) {
                return DEFAULT_CATALOG_SERVER_CONN_TIMEOUT_MILLS;
            } else {
                return timeout;
            }
        } catch (Exception e) {
            return DEFAULT_CATALOG_SERVER_CONN_TIMEOUT_MILLS;
        }
    }

    public static int getMetaStoreNumThreads(Configuration conf) {
        return conf.getInt(CATALOG_CLIENT_NUM_THREADS, DEFAULT_NUM_EXECUTOR_THREADS);
    }

    public static String getRole(Configuration conf) {
        return conf.get(DataLakeConfig.CATALOG_ROLE);
    }

    public static Boolean getEnableFileOperation(Configuration conf) {
        return conf.getBoolean(ENABLE_FILE_OPERATION, false);
    }

    public static double getEnableFileOperationGrayRate(Configuration conf) {
        double gray = conf.getDouble(ENABLE_FILE_OPERATION_GRAY_RATE, 0.0d);
        return gray;
    }

    public static boolean getDefaultDbCreateTableUseCurrentDbLocation(Configuration conf) {
        boolean enableModify = conf.getBoolean(CATALOG_DEFAULT_DB_CREATE_TABLE_USE_CURRENT_DB_LOCATION, false);
        return enableModify;
    }
}
