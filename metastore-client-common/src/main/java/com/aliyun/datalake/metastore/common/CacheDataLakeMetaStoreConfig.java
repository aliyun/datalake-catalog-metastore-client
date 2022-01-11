package com.aliyun.datalake.metastore.common;

public class CacheDataLakeMetaStoreConfig {
    //cache
    public static final String DATA_LAKE_DB_CACHE_ENABLE = "data.lake.cache.db.enable";
    public static final String DATA_LAKE_DB_CACHE_SIZE = "data.lake.cache.db.size";
    public static final String DATA_LAKE_DB_CACHE_TTL_MINS = "data.lake.cache.db.ttl.mins";
    public static final String DATA_LAKE_TB_CACHE_ENABLE = "data.lake.cache.tb.enable";
    public static final String DATA_LAKE_TB_CACHE_SIZE = "data.lake.cache.tb.size";
    public static final String DATA_LAKE_TB_CACHE_TTL_MINS = "data.lake.cache.tb.ttl.mins";

    //cache
    private boolean dataLakeDbCacheEnable;
    private int dataLakeDbCacheSize;
    private int dataLakeDbCacheTTLMins;
    private boolean dataLakeTbCacheEnable;
    private int dataLakeTbCacheSize;
    private int dataLakeTbCacheTTLMins;

    public CacheDataLakeMetaStoreConfig() {

    }

    public CacheDataLakeMetaStoreConfig(
            boolean dataLakeDbCacheEnable
            , int dataLakeDbCacheSize
            , int dataLakeDbCacheTTLMins
            , boolean dataLakeTbCacheEnable
            , int dataLakeTbCacheSize
            , int dataLakeTbCacheTTLMins) {

        this.dataLakeDbCacheEnable = dataLakeDbCacheEnable;
        this.dataLakeDbCacheSize = dataLakeDbCacheSize;
        this.dataLakeDbCacheTTLMins = dataLakeDbCacheTTLMins;
        this.dataLakeTbCacheEnable = dataLakeTbCacheEnable;
        this.dataLakeTbCacheSize = dataLakeTbCacheSize;
        this.dataLakeTbCacheTTLMins = dataLakeTbCacheTTLMins;
    }

    public boolean isDataLakeDbCacheEnable() {
        return dataLakeDbCacheEnable;
    }

    public void setDataLakeDbCacheEnable(boolean dataLakeDbCacheEnable) {
        this.dataLakeDbCacheEnable = dataLakeDbCacheEnable;
    }

    public int getDataLakeDbCacheSize() {
        return dataLakeDbCacheSize;
    }

    public void setDataLakeDbCacheSize(int dataLakeDbCacheSize) {
        this.dataLakeDbCacheSize = dataLakeDbCacheSize;
    }

    public int getDataLakeDbCacheTTLMins() {
        return dataLakeDbCacheTTLMins;
    }

    public void setDataLakeDbCacheTTLMins(int dataLakeDbCacheTTLMins) {
        this.dataLakeDbCacheTTLMins = dataLakeDbCacheTTLMins;
    }

    public boolean isDataLakeTbCacheEnable() {
        return dataLakeTbCacheEnable;
    }

    public void setDataLakeTbCacheEnable(boolean dataLakeTbCacheEnable) {
        this.dataLakeTbCacheEnable = dataLakeTbCacheEnable;
    }

    public int getDataLakeTbCacheSize() {
        return dataLakeTbCacheSize;
    }

    public void setDataLakeTbCacheSize(int dataLakeTbCacheSize) {
        this.dataLakeTbCacheSize = dataLakeTbCacheSize;
    }

    public int getDataLakeTbCacheTTLMins() {
        return dataLakeTbCacheTTLMins;
    }

    public void setDataLakeTbCacheTTLMins(int dataLakeTbCacheTTLMins) {
        this.dataLakeTbCacheTTLMins = dataLakeTbCacheTTLMins;
    }
}
