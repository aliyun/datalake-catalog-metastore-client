package com.aliyun.datalake.metastore.hive.common;

import com.aliyun.datalake.metastore.common.*;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.aliyun.teaopenapi.models.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class MetastoreFactory {
    private static final Logger logger = LoggerFactory.getLogger(MetastoreFactory.class);

    public IDataLakeMetaStore getMetaStore(Configuration conf, ExecutorService service) throws MetaException {
        String accessKeyId = ConfigUtils.getAccessKeyId(conf).orElse(null);
        String accessKeySecret = ConfigUtils.getAccessKeySecret(conf).orElse(null);
        String regionId = ConfigUtils.getRegionId(conf).orElse(null);
        String securityToken = ConfigUtils.getSecurityToken(conf).orElse(null);
        DataLakeConfig.AKMode akMode = ConfigUtils.akMode(conf);
        String uid = ConfigUtils.getUserId(conf);
        String role = ConfigUtils.getRole(conf);
        Properties extendedConfig = new Properties();
        if (akMode != null) {
            extendedConfig.setProperty(DataLakeConfig.CATALOG_AK_MODE, akMode.name());
        }
        if (uid != null) {
            extendedConfig.setProperty(DataLakeConfig.CATALOG_USER_ID, uid);
        }
        if (role != null) {
            extendedConfig.setProperty(DataLakeConfig.CATALOG_ROLE, role);
        }

        extendedConfig.setProperty(DataLakeConfig.CATALOG_STS_IS_NEW_MODE, String.valueOf(ConfigUtils.isNewSTSMode(conf)));
        extendedConfig.setProperty(DataLakeConfig.CATALOG_ACCURATE_BATCH_SIZE, String.valueOf(ConfigUtils.getAccurateListBatchSize(conf)));
        extendedConfig.setProperty(DataLakeConfig.CATALOG_TABLE_COL_STATS_PAGE_SIZE, String.valueOf(ConfigUtils.getTableColStatsPageSize(conf)));

        String endpoint = ConfigUtils.getEndPoint(conf).orElse(null);
        try {
            Config config = new Config();
            config.accessKeyId = accessKeyId;
            config.accessKeySecret = accessKeySecret;
            config.endpoint = endpoint;
            config.regionId = regionId;
            config.securityToken = securityToken;
            config.readTimeout = ConfigUtils.getReadTimeout(conf);
            config.connectTimeout = ConfigUtils.getConnTimeout(conf);
            //create metastore
            IDataLakeMetaStore dataLakeMetaStore = new DefaultDataLakeMetaStore(config, extendedConfig, service);

            //create cache metastore
            if (isCacheEnabled(conf)) {
                boolean databaseCacheEnabled = conf.getBoolean(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_ENABLE, false);
                int dbCacheSize = conf.getInt(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_SIZE, 0);
                int dbCacheTtlMins = conf.getInt(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_TTL_MINS, 0);
                boolean tableCacheEnabled = conf.getBoolean(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_ENABLE, false);
                int tbCacheSize = conf.getInt(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_SIZE, 0);
                int tbCacheTtlMins = conf.getInt(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_TTL_MINS, 0);
                CacheDataLakeMetaStoreConfig cacheConfig = new CacheDataLakeMetaStoreConfig(databaseCacheEnabled, dbCacheSize, dbCacheTtlMins, tableCacheEnabled, tbCacheSize, tbCacheTtlMins);
                return new CacheDataLakeMetaStore(cacheConfig, dataLakeMetaStore);
            }
            return dataLakeMetaStore;
        } catch (Exception e) {
            throw DataLakeUtil.throwException(new MetaException("Initialize DlfMetaStoreClient failed: " + e.getMessage()), e);
        }
    }

    private boolean isCacheEnabled(Configuration conf) {
        boolean databaseCacheEnabled = conf.getBoolean(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_ENABLE, false);
        boolean tableCacheEnabled = conf.getBoolean(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_ENABLE, false);
        return (databaseCacheEnabled || tableCacheEnabled);
    }
}
