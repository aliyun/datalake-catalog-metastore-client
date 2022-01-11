package com.aliyun.datalake.metastore.hive.common;

import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.api.DataLakeAPIException;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.hive.common.converters.CatalogToHiveConverter;
import com.aliyun.datalake.metastore.hive.common.converters.HiveToCatalogConverter;
import com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils;
import com.aliyun.datalake.metastore.hive.common.utils.Utils;
import com.aliyun.datalake.metastore.hive.shims.ColStatsObjWithSourceInfo;
import com.aliyun.datalake.metastore.hive.shims.ColumnStatsAggregator;
import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.aliyun.datalake20200710.models.UpdateTablePartitionColumnStatisticsRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CommonMetaStoreClientDelegate {
    private static final Logger logger = LoggerFactory.getLogger(CommonMetaStoreClientDelegate.class);
    private static final int NO_BATCHING = -1, DETECT_BATCHING = 0;
    private final IDataLakeMetaStore dataLakeMetaStore;
    private final IHiveShims hiveShims;
    private final Configuration conf;
    private int batchSize;
    private int batchSizeForGetPartititon;

    public CommonMetaStoreClientDelegate(IDataLakeMetaStore dataLakeMetaStore
            , IHiveShims hiveShims
            , Configuration conf) {
        this.dataLakeMetaStore = dataLakeMetaStore;
        this.hiveShims = hiveShims;
        this.conf = conf;
        this.batchSize = hiveShims.getMetastoreDirectSqlPartitionBatchSize(this.conf);
        if (batchSize == DETECT_BATCHING) {
            batchSize = NO_BATCHING;
        }
        this.batchSizeForGetPartititon = ConfigUtils.getColStatsPageSize(conf);
    }

    public List<ColumnStatisticsObj> getTableColumnStatisticsObjs(
            String catalogId
            , String dbName
            , String tableName
            , List<String> colNames)
            throws UnsupportedOperationException, TException {
        try {
            List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> results = dataLakeMetaStore.getTableColumnStatistics(catalogId, dbName, tableName, colNames);
            return CatalogToHiveConverter.toHiveColumnStatsObjs(results, hiveShims.enableBitVector(conf), hiveShims);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get table column statistics: " + catalogId + "." + dbName + "." + tableName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e), e);
        }
    }

    public ColumnStatistics getTableColumnStatistics(
            String catalogId
            , String dbName
            , String tableName
            , List<String> colNames)
            throws UnsupportedOperationException, TException {
        try {
            List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> results = dataLakeMetaStore.getTableColumnStatistics(catalogId, dbName, tableName, colNames);
            ColumnStatistics columnStatistics = new ColumnStatistics();
            ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
            desc.setDbName(dbName);
            desc.setTableName(tableName);
            desc.setIsTblLevel(true);
            columnStatistics.setStatsObj(CatalogToHiveConverter.toHiveColumnStatsObjs(results, hiveShims.enableBitVector(conf), hiveShims));
            columnStatistics.setStatsDesc(desc);
            return columnStatistics;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get table column statistics: " + catalogId + "." + dbName + "." + tableName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public boolean deletePartitionColumnStatistics(
            String catalogId
            , String dbName
            , String tableName
            , String partName
            , String colName) throws TException {
        try {
            List<String> partNames = new ArrayList<>();
            partNames.add(partName);
            List<String> colNames = new ArrayList<>();
            if (colName != null) {
                colNames.add(colName);
            } else {
                com.aliyun.datalake20200710.models.Table table = this.dataLakeMetaStore.getTable(catalogId, dbName, tableName);
                if (table != null && table.getSd() != null && table.getSd().getCols() != null && table.getSd().getCols().size() > 0) {
                    colNames = table.getSd().getCols().stream().map(t -> t.getName()).collect(Collectors.toList());
                }
            }
            return dataLakeMetaStore.deletePartitionColumnStatistics(catalogId, dbName, tableName, partNames, colNames);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to delete table column partition statistics: " + catalogId + "." + dbName + "." + tableName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public boolean deleteTableColumnStatistics(
            String catalogId
            , String dbName
            , String tableName
            , String colName)
            throws UnsupportedOperationException, TException {
        List<String> cols = new ArrayList<>();
        try {
            if (colName != null) {
                cols.add(colName);
            } else {
                com.aliyun.datalake20200710.models.Table table = this.dataLakeMetaStore.getTable(catalogId, dbName, tableName);
                if (table != null && table.getSd() != null && table.getSd().getCols() != null && table.getSd().getCols().size() > 0) {
                    cols = table.getSd().getCols().stream().map(t -> t.getName()).collect(Collectors.toList());
                }
            }
            return this.dataLakeMetaStore.deleteTableColumnStatistics(catalogId, dbName, tableName, cols);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to delete table column statistics: " + catalogId + "." + dbName + "." + tableName + "." + colName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public boolean updatePartitionColumnStatistics(String catalogId, Table tbl, ColumnStatistics columnStatistics) throws TException {
        try {
            String partName = Utils.lowerCaseConvertPartName(columnStatistics.getStatsDesc().getPartName());

            //check whether partName exists
            getPartValsFromName(tbl, partName);

            UpdateTablePartitionColumnStatisticsRequest request = new UpdateTablePartitionColumnStatisticsRequest();
            request.setCatalogId(catalogId);
            request.setDatabaseName(columnStatistics.getStatsDesc().getDbName());
            request.setTableName(columnStatistics.getStatsDesc().getTableName());
            columnStatistics.getStatsDesc().setPartName(partName);

            //Map<String, MTableColumnStatistics> oldStats = getPartitionColStats(table, colNames);

            com.aliyun.datalake20200710.models.ColumnStatistics catalogColumnStats = HiveToCatalogConverter.toCatalogColumnStats(hiveShims, columnStatistics);
            List<com.aliyun.datalake20200710.models.ColumnStatistics> catalogColumnStatsList = new ArrayList<>();
            catalogColumnStatsList.add(catalogColumnStats);
            request.setColumnStatisticsList(catalogColumnStatsList);

            return this.dataLakeMetaStore.updatePartitionColumnStatistics(request);

        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to update table column partition statistics: " + catalogId;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public boolean updateTableColumnStatistics(String catalogId, ColumnStatistics columnStatistics)
            throws TException {
        try {
            ColumnStatisticsDesc columnStatisticsDesc = columnStatistics.getStatsDesc();
            UpdateTablePartitionColumnStatisticsRequest request = new UpdateTablePartitionColumnStatisticsRequest();
            String dbNameLowercase = columnStatisticsDesc.getDbName();
            String tableNameLowercase = columnStatisticsDesc.getTableName();
            request.setCatalogId(catalogId);
            request.setDatabaseName(dbNameLowercase);
            request.setTableName(tableNameLowercase);
            com.aliyun.datalake20200710.models.ColumnStatistics catalogColumnStats = HiveToCatalogConverter.toCatalogColumnStats(hiveShims, columnStatistics);
            List<com.aliyun.datalake20200710.models.ColumnStatistics> catalogColumnStatsList = new ArrayList<>();
            catalogColumnStatsList.add(catalogColumnStats);
            request.setColumnStatisticsList(catalogColumnStatsList);
            //update table paramters

            com.aliyun.datalake20200710.models.Table tbl = this.dataLakeMetaStore.getTable(catalogId, dbNameLowercase, tableNameLowercase);
            Map<String, String> params = tbl.getParameters();
            List<String> colNames = catalogColumnStats.getColumnStatisticsObjList().stream().map(obj -> obj.getColumnName()).collect(Collectors.toList());
            StatsSetupConst.setColumnStatsState(tbl.getParameters(), colNames);
            tbl.setParameters(params);
            this.dataLakeMetaStore.alterTable(catalogId, dbNameLowercase, tableNameLowercase, HiveToCatalogConverter.toCatalogTableInput(CatalogToHiveConverter.toHiveTable(tbl)));

            return dataLakeMetaStore.updateTableColumnStatistics(request);
            //update table paramters
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to update table column statistics: " + catalogId;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatisticsObj(
            String catalogId
            , String dbName
            , String tableName
            , List<String> partitionNames
            , List<String> columnNames) throws TException {
        try {
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> results = this.dataLakeMetaStore.batchGetPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames);
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> newResults = results.entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (newResults.size() == 0) {
                return new HashMap<String, List<ColumnStatisticsObj>>();
            }
            return CatalogToHiveConverter.toHiveColumnStatsObjMaps(newResults, hiveShims.enableBitVector(conf), hiveShims);
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get table column partiton statistics: " + catalogId + "." + dbName + "." + tableName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> getPartitionColumnStatisticsObjOrigin(
            String catalogId
            , String dbName
            , String tableName
            , List<String> partitionNames
            , List<String> columnNames) throws TException {
        try {
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> results = this.dataLakeMetaStore.batchGetPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames);
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> newResults = results.entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (newResults.size() == 0) {
                return new HashMap<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>();
            }
            return newResults;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get table column partiton statistics: " + catalogId + "." + dbName + "." + tableName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public Map<String, ColumnStatistics> getPartitionColumnStatistics(String catalogId
            , String dbName
            , String tableName
            , List<String> partitionNames
            , List<String> columnNames) throws TException {
        try {
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> results = this.dataLakeMetaStore.batchGetPartitionColumnStatistics(catalogId, dbName, tableName, partitionNames, columnNames);
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> newResults = results.entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (newResults.size() == 0) {
                return new HashMap<String, ColumnStatistics>();
            }
            Map<String, ColumnStatistics> hivestats = CatalogToHiveConverter.toHiveColumnStatsMaps(catalogId, dbName, tableName, newResults, false, hiveShims.enableBitVector(conf), hiveShims);
            return hivestats;
        } catch (DataLakeAPIException e) {
            throw CatalogToHiveConverter.toHiveException(e.getResult(), e.getAction(), e);
        } catch (Exception e) {
            String msg = "Unable to get table column partiton statistics: " + catalogId + "." + dbName + "." + tableName;
            logger.error(msg, e);
            throw DataLakeUtil.throwException(new MetaException(msg + e),e);
        }
    }

    public boolean setPartitionColumnStatistics(String catalogId
            , SetPartitionsStatsRequest request
            , Table t
            , List<Partition> partitions) throws TException {
        boolean ret = true;
        List<ColumnStatistics> csNews = request.getColStats();
        if (csNews == null || csNews.isEmpty()) {
            return ret;
        }
        // figure out if it is table level or partition level
        ColumnStatistics firstColStats = csNews.get(0);
        ColumnStatisticsDesc statsDesc = firstColStats.getStatsDesc();
        String dbName = statsDesc.getDbName();
        String tableName = statsDesc.getTableName();
        List<String> colNames = new ArrayList<>();
        for (ColumnStatisticsObj obj : firstColStats.getStatsObj()) {
            colNames.add(obj.getColName());
        }
        if (statsDesc.isIsTblLevel()) {
            // there should be only one ColumnStatistics
            if (request.getColStatsSize() != 1) {
                throw new MetaException(
                        "Expecting only 1 ColumnStatistics for table's column stats, but find "
                                + request.getColStatsSize());
            } else {
                if (request.isSetNeedMerge() && request.isNeedMerge()) {
                    // one single call to get all column stats
                    ColumnStatistics csOld = getTableColumnStatistics(catalogId, dbName, tableName, colNames);
//                    Table t = getTable(catName, dbName, tableName);
                    // we first use t.getParameters() to prune the stats
                    hiveShims.getMergableCols(firstColStats, t.getParameters());
                    // we merge those that can be merged
                    if (csOld != null && csOld.getStatsObjSize() != 0
                            && !firstColStats.getStatsObj().isEmpty()) {
                        hiveShims.mergeColStats(firstColStats, csOld);
                    }
                    if (!firstColStats.getStatsObj().isEmpty()) {
                        return updateTableColumnStatistics(catalogId, firstColStats);
                    } else {
                        logger.debug("All the column stats are not accurate to merge.");
                        return true;
                    }
                } else {
                    // This is the overwrite case, we do not care about the accuracy.
                    return updateTableColumnStatistics(catalogId, firstColStats);
                }
            }
        } else {
            // partition level column stats merging
//            List<Partition> partitions = new ArrayList<>();
            // note that we may have two or more duplicate partition names.
            // see autoColumnStats_2.q under TestMiniLlapLocalCliDriver
            Map<String, ColumnStatistics> newStatsMap = new HashMap<>();
            for (ColumnStatistics csNew : csNews) {
                String partName = csNew.getStatsDesc().getPartName();
                if (newStatsMap.containsKey(partName)) {
                    hiveShims.mergeColStats(csNew, newStatsMap.get(partName));
                }
                newStatsMap.put(partName, csNew);
            }
            Map<String, ColumnStatistics> oldStatsMap = new HashMap<>();
            Map<String, Partition> mapToPart = new HashMap<>();
            if (request.isSetNeedMerge() && request.isNeedMerge()) {
                // a single call to get all column stats for all partitions
                List<String> partitionNames = new ArrayList<>();
                partitionNames.addAll(newStatsMap.keySet());
                oldStatsMap = getPartitionColumnStatistics(catalogId, dbName,
                        tableName, partitionNames, colNames);
                if (newStatsMap.values().size() != oldStatsMap.values().size()) {
                    // some of the partitions miss stats.
                    logger.debug("Some of the partitions miss stats.");
                }
                // another single call to get all the partition objects
//                partitions = getPartitionsByNames(catName, dbName, tableName, partitionNames);
                for (int index = 0; index < partitionNames.size(); index++) {
                    mapToPart.put(partitionNames.get(index), partitions.get(index));
                }
            }
//            Table t = getTable(catName, dbName, tableName);
            for (Map.Entry<String, ColumnStatistics> entry : newStatsMap.entrySet()) {
                ColumnStatistics csNew = entry.getValue();
                ColumnStatistics csOld = oldStatsMap.get(entry.getKey());
                if (request.isSetNeedMerge() && request.isNeedMerge()) {
                    // we first use getParameters() to prune the stats
                    hiveShims.getMergableCols(csNew, mapToPart.get(entry.getKey()).getParameters());
                    // we merge those that can be merged
                    if (csOld != null && csOld.getStatsObjSize() != 0 && !csNew.getStatsObj().isEmpty()) {
                        hiveShims.mergeColStats(csNew, csOld);
                    }
                    if (!csNew.getStatsObj().isEmpty()) {
                        ret = ret && updatePartitionColumnStatistics(catalogId, t, csNew);
                    } else {
                        logger.debug("All the column stats " + csNew.getStatsDesc().getPartName()
                                + " are not accurate to merge.");
                    }
                } else {
                    ret = ret && updatePartitionColumnStatistics(catalogId, t, csNew);
                }
            }
        }
        return ret;
    }

    public AggrStats getAggrStatsFor(String catalogId, PartitionsStatsRequest request) throws TException {
        String dbName = request.getDbName();
        String tblName = request.getTblName();
        if (request.getColNames().isEmpty() || request.getPartNames().isEmpty()) {
            return new AggrStats(new ArrayList<ColumnStatisticsObj>(), 0); // Nothing to aggregate
        }
        List<String> lowerCaseColNames = request.getColNames();
        List<String> lowerCasePartNames = request.getPartNames();

        long partsFound = 0;
        List<ColumnStatisticsObj> colStatsList;

        // Try to read from the cache first, not it not supported
        //

        partsFound = partsFoundForPartitions(catalogId, dbName, tblName, lowerCasePartNames, lowerCaseColNames);
        colStatsList =
                columnStatisticsObjForPartitions(catalogId, dbName, tblName, lowerCasePartNames, lowerCaseColNames, partsFound,
                        hiveShims.getDensityFunctionForNDVEstimation(conf), hiveShims.getNdvTuner(conf));

        return new AggrStats(colStatsList, partsFound);
    }

    private List<String> getPartValsFromName(Table t, String partName)
            throws MetaException, InvalidObjectException {
        Preconditions.checkArgument(t != null, "Table can not be null");
        // Unescape the partition name
        LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

        List<String> partVals = new ArrayList<>();
        for (FieldSchema field : t.getPartitionKeys()) {
            String key = field.getName();
            String val = hm.get(key);
            if (val == null) {
                throw new InvalidObjectException("incomplete partition name - missing " + key);
            }
            partVals.add(val);
        }
        return partVals;
    }

    public long partsFoundForPartitions(final String catalogId, final String dbName, final String tableName,
                                        final List<String> partNames, List<String> colNames) throws TException {

        long start = System.currentTimeMillis();
        assert !colNames.isEmpty() && !partNames.isEmpty();
        List<Long> allCounts = runBatchedGetPartitions(colNames, new Batchable<String, Long>() {
            public List<Long> run(final List<String> inputColName) throws TException {
                return runBatchedGetPartitions(partNames, new Batchable<String, Long>() {
                    public List<Long> run(List<String> inputPartNames) throws TException {
                        long partsFound = 0;
                        Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> statsObjs = getPartitionColumnStatisticsObjOrigin(catalogId, dbName, tableName, inputPartNames, inputColName);
                        Iterator<Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = statsObjs.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> entry = iterator.next();
                            if (entry.getValue().size() == inputColName.size()) {
                                partsFound++;
                            }
                        }
                        return Lists.<Long>newArrayList(partsFound);
                    }
                });
            }
        });
        long partsFound = 0;
        for (Long val : allCounts) {
            partsFound += val;
        }
        logger.debug("partsFoundForPartitions time: " + catalogId + "." + dbName + "." + tableName + ":" + (System.currentTimeMillis() - start) + "ms");
        return partsFound;
    }

    public List<ColumnStatisticsObj> columnStatisticsObjForPartitions(final String catalogId
            , final String dbName
            , final String tableName
            , final List<String> partNames
            , List<String> colNames
            , long partsFound
            , final boolean useDensityFunctionForNDVEstimation
            , final double ndvTuner) throws TException {
        final boolean areAllPartsFound = (partsFound == partNames.size());
        long start = System.currentTimeMillis();
        List<ColumnStatisticsObj> columnsObjs = runBatched(colNames, new Batchable<String, ColumnStatisticsObj>() {
            public List<ColumnStatisticsObj> run(final List<String> inputColNames) throws TException {
                return runBatched(partNames, new Batchable<String, ColumnStatisticsObj>() {
                    public List<ColumnStatisticsObj> run(List<String> inputPartNames) throws TException {
                        return columnStatisticsObjForPartitionsBatch(catalogId, dbName, tableName, inputPartNames,
                                inputColNames, areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner);
                    }
                });
            }
        });
        logger.debug("columnStatisticsObjForPartitions time: " + catalogId + "." + dbName + "." + tableName + ":" + (System.currentTimeMillis() - start) + "ms");
        return columnsObjs;
    }

    private <I, R> List<R> runBatched(List<I> input, Batchable<I, R> runnable) throws TException {
        if (batchSize == NO_BATCHING || batchSize >= input.size()) {
            return runnable.run(input);
        }
        List<R> result = new ArrayList<R>(input.size());
        for (int fromIndex = 0, toIndex = 0; toIndex < input.size(); fromIndex = toIndex) {
            toIndex = Math.min(fromIndex + batchSize, input.size());
            List<I> batchedInput = input.subList(fromIndex, toIndex);
            List<R> batchedOutput = runnable.run(batchedInput);
            if (batchedOutput != null) {
                result.addAll(batchedOutput);
            }
        }
        return result;
    }

    private <I, R> List<R> runBatchedGetPartitions(List<I> input, Batchable<I, R> runnable) throws TException {
        if (batchSizeForGetPartititon == NO_BATCHING || batchSizeForGetPartititon >= input.size()) {
            return runnable.run(input);
        }
        List<R> result = new ArrayList<R>(input.size());
        for (int fromIndex = 0, toIndex = 0; toIndex < input.size(); fromIndex = toIndex) {
            toIndex = Math.min(fromIndex + batchSizeForGetPartititon, input.size());
            List<I> batchedInput = input.subList(fromIndex, toIndex);
            List<R> batchedOutput = runnable.run(batchedInput);
            if (batchedOutput != null) {
                result.addAll(batchedOutput);
            }
        }
        return result;
    }

    /**
     * Should be called with the list short enough to not trip up Oracle/etc.
     */
    private List<ColumnStatisticsObj> columnStatisticsObjForPartitionsBatch(
            String catalogId
            , String dbName
            , String tableName
            , List<String> partNames
            , List<String> colNames
            , boolean areAllPartsFound
            , boolean useDensityFunctionForNDVEstimation
            , double ndvTuner) throws TException {
        // 1. get all the stats for colNames in partNames;
        Map<String, List<ColumnStatisticsObj>> partStatsObjs = getPartitionStats(catalogId, dbName, tableName, partNames, colNames, hiveShims.enableBitVector(conf));
        // 2. use util function to aggr stats
        return aggrPartitionStats(partStatsObjs, catalogId, dbName, tableName, partNames, colNames,
                areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner);
    }

    public List<ColumnStatisticsObj> aggrPartitionStats(
            Map<String, List<ColumnStatisticsObj>> partStats
            , String catalogId
            , String dbName
            , String tableName
            , List<String> partNames
            , List<String> colNames
            , boolean areAllPartsFound
            , boolean useDensityFunctionForNDVEstimation
            , double ndvTuner)
            throws MetaException {

        Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap =
                new HashMap<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>>();
        // Group stats by colName for each partition
        Map<String, ColumnStatsAggregator> aliasToAggregator =
                new HashMap<String, ColumnStatsAggregator>();
        for (String partName : partStats.keySet()) {
            List<ColumnStatisticsObj> objs = partStats.get(partName);
            for (ColumnStatisticsObj obj : objs) {
                if (aliasToAggregator.get(obj.getColName()) == null) {
                    aliasToAggregator.put(obj.getColName(),
                            new ColumnStatsAggregator(obj.getStatsData().getSetField(), hiveShims.getNumBitVectors(conf), useDensityFunctionForNDVEstimation, ndvTuner, obj.getColName()));
                    colStatsMap.put(aliasToAggregator.get(obj.getColName()),
                            new ArrayList<ColStatsObjWithSourceInfo>());
                }

                colStatsMap.get(aliasToAggregator.get(obj.getColName()))
                        .add(new ColStatsObjWithSourceInfo(obj, catalogId, dbName, tableName, partName));
            }
        }
        if (colStatsMap.size() < 1) {

            return new ArrayList<ColumnStatisticsObj>();
        }
        return aggrPartitionStats(colStatsMap, partNames, areAllPartsFound,
                useDensityFunctionForNDVEstimation, ndvTuner);
    }

    public List<ColumnStatisticsObj> aggrPartitionStats(
            Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap,
            final List<String> partNames, final boolean areAllPartsFound,
            final boolean useDensityFunctionForNDVEstimation, final double ndvTuner)
            throws MetaException {
        List<ColumnStatisticsObj> aggrColStatObjs = new ArrayList<ColumnStatisticsObj>();
        int numProcessors = Runtime.getRuntime().availableProcessors();
        final ExecutorService pool =
                Executors.newFixedThreadPool(Math.min(colStatsMap.size(), numProcessors),
                        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("aggr-col-stats-%d").build());
        final List<Future<ColumnStatisticsObj>> futures = Lists.newLinkedList();
        logger.debug("Aggregating column stats. Threads used: {}" +
                Math.min(colStatsMap.size(), numProcessors));
        long start = System.currentTimeMillis();
        for (final Map.Entry<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> entry : colStatsMap
                .entrySet()) {
            futures.add(pool.submit(new Callable<ColumnStatisticsObj>() {
                @Override
                public ColumnStatisticsObj call() throws MetaException {
                    List<ColStatsObjWithSourceInfo> colStatWithSourceInfo = entry.getValue();
                    ColumnStatsAggregator aggregator = entry.getKey();
                    try {
                        ColumnStatisticsObj statsObj =
                                hiveShims.getAggregate(aggregator, colStatWithSourceInfo, partNames, areAllPartsFound);
                        return statsObj;
                    } catch (MetaException e) {
                        logger.debug(e.getMessage());
                        throw e;
                    }
                }
            }));
        }
        pool.shutdown();
        if (!futures.isEmpty()) {
            for (Future<ColumnStatisticsObj> future : futures) {
                try {
                    if (future.get() != null) {
                        aggrColStatObjs.add(future.get());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.debug(e.getMessage());
                    pool.shutdownNow();
                    throw DataLakeUtil.throwException(new MetaException(e.toString()), e);
                }

            }
        }
        logger.debug("Time for aggr col stats in seconds: {} Threads used: {}" +
                ((System.currentTimeMillis() - (double) start)) / 1000 +
                Math.min(colStatsMap.size(), numProcessors));
        return aggrColStatObjs;
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionStats(
            final String catalogid, final String dbName, final String tableName, final List<String> partNames,
            List<String> colNames, boolean enableBitVector) throws TException {
        if (colNames.isEmpty() || partNames.isEmpty()) {
            return new HashMap<>();
        }
        Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics> b = new Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics>() {
            @Override
            public List<com.aliyun.datalake20200710.models.ColumnStatistics> run(final List<String> inputColNames) throws TException {
                Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics> b2 = new Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics>() {
                    @Override
                    public List<com.aliyun.datalake20200710.models.ColumnStatistics> run(List<String> inputPartNames) throws TException {
                        List<com.aliyun.datalake20200710.models.ColumnStatistics> statistics = new ArrayList<>();
                        Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> objs = getPartitionColumnStatisticsObjOrigin(catalogid, dbName, tableName, inputPartNames, inputColNames);
                        Iterator<Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = objs.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> entry = iterator.next();
                            com.aliyun.datalake20200710.models.ColumnStatisticsDesc desc = new com.aliyun.datalake20200710.models.ColumnStatisticsDesc();
                            desc.setPartitionName(entry.getKey());
                            com.aliyun.datalake20200710.models.ColumnStatistics stat = new com.aliyun.datalake20200710.models.ColumnStatistics();
                            stat.setColumnStatisticsDesc(desc);
                            stat.setColumnStatisticsObjList(entry.getValue());
                            statistics.add(stat);
                        }
                        return statistics;
                    }
                };
                return runBatchedGetPartitions(partNames, b2);
            }
        };
        List<com.aliyun.datalake20200710.models.ColumnStatistics> columnStatisticsList = runBatchedGetPartitions(colNames, b);

        Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> objMap = new HashMap<>();
        for (com.aliyun.datalake20200710.models.ColumnStatistics columnStatistics : columnStatisticsList) {
            if (objMap.get(columnStatistics.getColumnStatisticsDesc().getPartitionName()) != null) {
                objMap.get(columnStatistics.getColumnStatisticsDesc().getPartitionName()).addAll(columnStatistics.getColumnStatisticsObjList());
            } else {
                List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> objs = new ArrayList<>();
                objs.addAll(columnStatistics.getColumnStatisticsObjList());
                objMap.put(columnStatistics.getColumnStatisticsDesc().getPartitionName(), objs);
            }
        }
        try {
            return CatalogToHiveConverter.toHiveColumnStatsObjMaps(objMap, hiveShims.enableBitVector(conf), hiveShims);
        } catch (IOException e) {
            throw new TException("catalog to HiveColumnStatsObjMaps error " + e, e);
        }


    }

    private static abstract class Batchable<I, R> {
        public abstract List<R> run(List<I> input) throws TException;
    }

}
