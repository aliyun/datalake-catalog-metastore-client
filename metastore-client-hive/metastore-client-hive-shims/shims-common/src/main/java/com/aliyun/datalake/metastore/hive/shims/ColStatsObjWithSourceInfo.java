package com.aliyun.datalake.metastore.hive.shims;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;

public class ColStatsObjWithSourceInfo {
    private final ColumnStatisticsObj colStatsObj;
    private final String catName;
    private final String dbName;
    private final String tblName;
    private final String partName;

    public ColStatsObjWithSourceInfo(ColumnStatisticsObj colStatsObj, String catName, String dbName, String tblName,
                                     String partName) {
        this.colStatsObj = colStatsObj;
        this.catName = catName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.partName = partName;
    }

    public ColumnStatisticsObj getColStatsObj() {
        return colStatsObj;
    }

    public String getCatName() {
        return catName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTblName() {
        return tblName;
    }

    public String getPartName() {
        return partName;
    }
}