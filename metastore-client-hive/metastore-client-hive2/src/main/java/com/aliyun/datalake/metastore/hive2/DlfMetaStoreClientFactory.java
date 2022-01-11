package com.aliyun.datalake.metastore.hive2;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientFactory;
import org.apache.hive.hcatalog.common.HiveClientCache;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Data Lake Formation MetaStore Client Factory.
 */
public class DlfMetaStoreClientFactory implements HiveMetaStoreClientFactory {
    public static HiveClientCache.ICacheableMetaStoreClient createHCatalogICacheableMetaStoreClient(HiveConf hiveConf, Integer timeout) throws MetaException, IOException {
        try {
            return
                    (HiveClientCache.ICacheableMetaStoreClient) RetryingMetaStoreClient.getProxy(hiveConf,
                            new Class<?>[]{HiveConf.class, Integer.class, Boolean.class},
                            new Object[]{hiveConf, timeout, true},
                            DLFCacheableMetaStoreClient.class.getName());
        } catch (MetaException e) {
            throw new IOException("Couldn't create hiveMetaStoreClient, Error getting UGI for user", e);
        }
    }

    @Override
    public IMetaStoreClient createMetaStoreClient(
            HiveConf hiveConf,
            HiveMetaHookLoader hiveMetaHookLoader,
            boolean allowEmbedded,
            ConcurrentHashMap<String, Long> metaCallTimeMap
    ) throws MetaException {
        checkNotNull(hiveConf, "conf cannot be null!");
        checkNotNull(metaCallTimeMap, "metaCallTimeMap cannot be null!");

        // TODO try direct to use DlfMetaStoreClient/SessionHiveMetaStoreClient
        if (hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH)) {
            return new ProxyMetaStoreClient(hiveConf, hiveMetaHookLoader, allowEmbedded);
        } else {
            return RetryingMetaStoreClient.getProxy(hiveConf, hiveMetaHookLoader, metaCallTimeMap,
                    ProxyMetaStoreClient.class.getName(), allowEmbedded);
        }
    }
}
