package com.aliyun.datalake.metastore.hive.shims;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;

public class ColumnStatsAggregator {
    private ColumnStatisticsData._Fields type;
    private int numBitVectors;
    private boolean useDensityFunctionForNDVEstimation;
    private double ndvTuner;
    private String colName;

    public ColumnStatsAggregator(ColumnStatisticsData._Fields type, int numBitVectors, boolean useDensityFunctionForNDVEstimation, double ndvTuner, String colName) {
        this.type = type;
        this.numBitVectors = numBitVectors;
        this.useDensityFunctionForNDVEstimation = useDensityFunctionForNDVEstimation;
        this.ndvTuner = ndvTuner;
        this.colName = colName;
    }

    public ColumnStatisticsData._Fields getType() {
        return type;
    }

    public void setType(ColumnStatisticsData._Fields type) {
        this.type = type;
    }

    public int getNumBitVectors() {
        return numBitVectors;
    }

    public void setNumBitVectors(int numBitVectors) {
        this.numBitVectors = numBitVectors;
    }

    public boolean isUseDensityFunctionForNDVEstimation() {
        return useDensityFunctionForNDVEstimation;
    }

    public void setUseDensityFunctionForNDVEstimation(boolean useDensityFunctionForNDVEstimation) {
        this.useDensityFunctionForNDVEstimation = useDensityFunctionForNDVEstimation;
    }

    public double getNdvTuner() {
        return ndvTuner;
    }

    public void setNdvTuner(double ndvTuner) {
        this.ndvTuner = ndvTuner;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }
}
