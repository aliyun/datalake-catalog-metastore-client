package com.aliyun.datalake.metastore.hive.common.utils;

import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientFactory;
import org.apache.hive.common.util.ReflectionUtil;

import java.util.concurrent.ConcurrentHashMap;

public class ClientUtils {

    public static IMetaStoreClient createMetaStoreClient(String factoryClass,
                                                         HiveConf conf,
                                                         HiveMetaHookLoader hookLoader,
                                                         boolean allowEmbedded,
                                                         ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException {

        HiveMetaStoreClientFactory factory;

        try {
            factory = (HiveMetaStoreClientFactory) ReflectionUtil.newInstance(
                    conf.getClassByName(factoryClass), null);
        } catch (Exception e) {
            String errorMessage = "Unable to instantiate a metastore client factory "
                    + factoryClass + ": " + e;
            throw DataLakeUtil.throwException(new MetaException(errorMessage), e);
        }

        return factory.createMetaStoreClient(conf, hookLoader, allowEmbedded, metaCallTimeMap);
    }
}
