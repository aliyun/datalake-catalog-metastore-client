package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.hive.common.utils.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UtilsTest extends BaseTest{
    static Warehouse warehouse;
    private static HiveConf hiveConf;

    @BeforeClass
    public static void initClient() throws TException, FileNotFoundException {
        hiveConf = new HiveConf();
        warehouse = new Warehouse(hiveConf);
    }

    @Test
    public void testEmptyDir() throws MetaException, IOException {
        String basePath = TestUtil.WAREHOUSE_PATH + "test_empty_dir";
        warehouse.mkdirs(new Path(basePath));
        warehouse.mkdirs(new Path(basePath + "/test1"));
        warehouse.mkdirs(new Path(basePath + "/test2"));
        warehouse.mkdirs(new Path(basePath + "/test1/test1_1"));
        assertFalse("is empty", warehouse.isEmpty(new Path(basePath + "/test1")));
        assertFalse("is empty", Utils.isEmptyDir(warehouse, new Path(basePath + "/test1")));
        assertFalse("is empty", warehouse.isEmpty(new Path(basePath)));
        assertFalse("is empty", Utils.isEmptyDir(warehouse, new Path(basePath)));

        warehouse.deleteDir(new Path(basePath + "/test1"), true, false, false);
        assertFalse("is empty", warehouse.isEmpty(new Path(basePath)));
        assertFalse("is empty", Utils.isEmptyDir(warehouse, new Path(basePath)));

        warehouse.deleteDir(new Path(basePath + "/test2"), true, false, false);
        assertTrue("is not empty", warehouse.isEmpty(new Path(basePath)));
        assertTrue("is not empty", Utils.isEmptyDir(warehouse, new Path(basePath)));
    }
}
