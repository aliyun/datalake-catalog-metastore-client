package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.ProxyMode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;

public class ProxyClientInitTest {
    @Test
    public void testAllowFailure() throws MetaException, NoSuchFieldException, IllegalAccessException {

        for (ProxyMode proxyMode : ProxyMode.values()) {
            if (proxyMode == ProxyMode.DLF_METASTORE_FAILURE) {

                HiveConf hiveConf = new HiveConf();
                hiveConf.set("dlf.catalog.proxyMode", proxyMode.name());
                //hive failed, will not throw exception
                hiveConf.set("hive.metastore.uris", "xx");
                ProxyMetaStoreClient proxyMetaStoreClient = new ProxyMetaStoreClient(hiveConf);
                Assert.assertTrue(proxyMetaStoreClient.isAllowFailure());
                Assert.assertTrue(proxyMetaStoreClient.getHiveSessionMetaStoreClient() == null);

                hiveConf.set("dlf.catalog.akMode", "xx");
                Assert.assertThrows(MetaException.class, () -> new ProxyMetaStoreClient(hiveConf));

            } else if (proxyMode == ProxyMode.METASTORE_DLF_FAILURE) {

                //dlf failed, will not throw exception. but hive client failed to, so can't create proxymetastoreclient too.
                HiveConf hiveConf = new HiveConf();
                hiveConf.set("dlf.catalog.proxyMode", proxyMode.name());
                hiveConf.set("hive.metastore.uris", "");
                hiveConf.set("dlf.catalog.akMode", "xx");
                Assert.assertThrows(MetaException.class, () -> new ProxyMetaStoreClient(hiveConf));

            } else if (proxyMode == ProxyMode.METASTORE_DLF_SUCCESS || proxyMode == ProxyMode.DLF_METASTORE_SUCCESS || proxyMode == ProxyMode.METASTORE_ONLY) {

                //hive failed, will throw exception
                HiveConf hiveConf = new HiveConf();
                hiveConf.set("dlf.catalog.proxyMode", proxyMode.name());
                hiveConf.set("hive.metastore.uris", "xx");
                Assert.assertThrows(Exception.class, () -> new ProxyMetaStoreClient(hiveConf));

            } else {
                //dlf only is ok
                HiveConf hiveConf = new HiveConf();
                hiveConf.set("dlf.catalog.proxyMode", proxyMode.name());
                ProxyMetaStoreClient proxyMetaStoreClient = new ProxyMetaStoreClient(hiveConf);
                Assert.assertFalse(proxyMetaStoreClient.isAllowFailure());
            }
        }
    }
}
