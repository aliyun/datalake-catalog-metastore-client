package com.aliyun.datalake.metastore.hive2;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;

public class ValidatorUtils {

    public static void validateExist(FileSystem fs, Path path, boolean exist) throws IOException {
        Assert.assertEquals(fs.exists(path), exist);
    }

    public static void validateExist(FileSystem fs, Path path, boolean exist, String message) throws IOException {
        Assert.assertEquals(message, fs.exists(path), exist);
    }

    public static void validateDatabases(Database expect, Database actual) {
        if (expect != null && actual != null) {
            Assert.assertEquals(expect.getDescription(), actual.getDescription());
            Assert.assertEquals(expect.getLocationUri(), actual.getLocationUri());
            Assert.assertEquals(expect.getName(), actual.getName());
//            Assert.assertEquals(expect.getOwnerName(), actual.getOwnerName());
//            Assert.assertEquals(expect.getOwnerType(), actual.getOwnerType());
            Assert.assertEquals(wrapper(expect.getParameters()), wrapper(actual.getParameters()));
            Assert.assertEquals(expect.getPrivileges(), actual.getPrivileges());
        }
    }

    public static Map<String, String> wrapper(Map<String, String> parameters) {
        if (parameters == null) {
            return Maps.newHashMap();
        }
        return parameters;
    }

    public static void validateTables(Table expect, Table actual) {
        if (expect == null && actual == null) {
            //
        } else if (expect != null && actual != null) {
            Assert.assertEquals(expect.getTableName(), actual.getTableName());
            Assert.assertEquals(expect.getLastAccessTime(), actual.getLastAccessTime());
            Assert.assertEquals(expect.isTemporary(), actual.isTemporary());
            Assert.assertEquals(expect.isRewriteEnabled(), actual.isRewriteEnabled());
            Assert.assertEquals(expect.getPrivileges(), actual.getPrivileges());
            Assert.assertEquals(expect.getTableType(), actual.getTableType());
            Assert.assertEquals(expect.getViewOriginalText(), actual.getViewOriginalText());
            Assert.assertEquals(expect.getViewExpandedText(), actual.getViewExpandedText());
            Assert.assertEquals(wrapper(expect.getParameters()), wrapper(actual.getParameters()));
            Assert.assertEquals(expect.getPartitionKeys(), actual.getPartitionKeys());
            Assert.assertEquals(expect.getCreateTime(), actual.getCreateTime());
            Assert.assertEquals(expect.getOwner(), actual.getOwner());
            Assert.assertEquals(expect.getDbName(), actual.getDbName());
            Assert.assertEquals(expect.getParametersSize(), actual.getParametersSize());
            Assert.assertEquals(expect.getPartitionKeysSize(), actual.getPartitionKeysSize());
            Assert.assertEquals(expect.getSd(), actual.getSd());
        } else {
            Assert.assertTrue(false);
        }
    }
}