package com.aliyun.datalake.metastore.hive.common.functional;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

@FunctionalInterface
public interface DropIndexesForTable {
    public void dropIndexesForTable(String catalogId, String dbName, Table originTable, boolean deleteData) throws TException;
}
