package com.aliyun.datalake.metastore.hive2;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;

public abstract class BaseTest {

    public static Collection<Object[]> clients() throws MetaException, FileNotFoundException {
        IMetaStoreClient dlfClient = TestUtil.getDlfClient();
        return Arrays.asList(new Object[][]{{dlfClient}});
    }
}
