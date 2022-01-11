package com.aliyun.datalake.metastore.hive.shims;

import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.hbase.stats.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.aliyun.datalake.metastore.hive.shims.IHiveShims.realMessage;

public class Hive2Shims implements IHiveShims {
    private static final String HIVE_VERSION = "2.";
    private static final Logger logger = LoggerFactory.getLogger(Hive2Shims.class);

    static boolean supportsVersion(String version) {
        return version.startsWith(HIVE_VERSION);
    }

    @Override
    public <T> Class<? extends RawStore> getClass(String className, Class<? extends RawStore> clazz) throws MetaException {
        return MetaStoreUtils.getClass(className);
    }

    @Override
    public <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes, Object[] initargs) {
        return MetaStoreUtils.newInstance(theClass, parameterTypes, initargs);
    }

    @Override
    public <T> List<ObjectPair<Integer, byte[]>> objectPairConvert(T partExprs) {
        List<ObjectPair<Integer, byte[]>> objectPairs = new ArrayList<>();
        for (org.apache.hadoop.hive.common.ObjectPair<Integer, byte[]> part : (List<org.apache.hadoop.hive.common.ObjectPair<Integer, byte[]>>) partExprs) {
            ObjectPair<Integer, byte[]> objectPair = ObjectPair.create(part.getFirst(), part.getSecond());
            objectPairs.add(objectPair);
        }
        return objectPairs;
    }

    @Override
    public boolean mkdirs(Warehouse wh, Path f, boolean inheritPermCandidate, Boolean enableFsOperation) throws MetaException {
        long startTime = System.currentTimeMillis();
        boolean mkdir = false;
        try {
            if (enableFsOperation) {
                mkdir = wh.mkdirs(f, inheritPermCandidate);
            } else {
                mkdir = true;
            }
            return mkdir;
        } finally {
            final boolean mkdirFinal = mkdir;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.mkdir, result:{}, cost:{}ms, path : {}, candidate : {}",
                                                 realMessage(enableFsOperation), mkdirFinal, System.currentTimeMillis() - startTime, f,
                                                 inheritPermCandidate));
        }
    }

    @Override
    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, Boolean enableFsOperation) throws MetaException {
        return deleteDir(wh, f, recursive, false, false, enableFsOperation);
    }

    @Override
    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, Database db, Boolean enableFsOperation) throws MetaException {
        return deleteDir(wh, f, recursive, false, false, enableFsOperation);
    }

    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, boolean ifPurge, Database db, Boolean enableFsOperation) throws MetaException {
        return deleteDir(wh, f, recursive, ifPurge, false, enableFsOperation);
    }

    @Override
    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, boolean ifPurge, boolean needCmRecycle, Boolean enableFsOperation) throws MetaException {
        long startTime = System.currentTimeMillis();
        boolean deleteDir = false;
        try {
            if (enableFsOperation) {
                deleteDir = wh.deleteDir(f, recursive, ifPurge);
            } else {
                deleteDir = true;
            }
            return deleteDir;
        } finally {
            final boolean deleteDirFinal = deleteDir;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.deleteDir, result:{}, cost:{}ms, path : {}, recursive : {}, ifPurge : {}, needCm : {}",
                                                     realMessage(enableFsOperation), deleteDirFinal, System.currentTimeMillis() - startTime, f,
                                                     recursive, ifPurge, needCmRecycle));
        }
    }

    @Override
    public void addCatalogForDb(Database db, String catalogId) {

    }

    @Override
    public void updateTableStatsFast(Database db, Table tbl, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {
        MetaStoreUtils.updateTableStatsFast(db, tbl, wh, madeDir, forceRecompute, environmentContext);
    }

    @Override
    public boolean requireCalStats(Configuration conf, Partition oldPart, Partition newPart, Table tbl, EnvironmentContext environmentContext) {
        return MetaStoreUtils.requireCalStats(conf, oldPart, newPart, tbl, environmentContext);
    }

    // in hive2 isFastStatsSame is not public, but hive3 is public
    @Override
    public boolean isFastStatsSame(Partition oldPart, Partition newPart) {
        // requires to calculate stats if new and old have different fast stats
        if ((oldPart != null) && (oldPart.getParameters() != null)) {
            for (String stat : StatsSetupConst.fastStats) {
                if (oldPart.getParameters().containsKey(stat)) {
                    Long oldStat = Long.parseLong(oldPart.getParameters().get(stat));
                    String newStat = newPart.getParameters().get(stat);
                    if (newStat == null || !oldStat.equals(Long.parseLong(newStat))) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    // in hive2 updateBasicState is not public, but hive3 is public
    public void updateBasicState(EnvironmentContext environmentContext, Map<String, String> params) {
        if (params == null) {
            return;
        }
        if (environmentContext != null
                && environmentContext.isSetProperties()
                && StatsSetupConst.TASK.equals(environmentContext.getProperties().get(
                StatsSetupConst.STATS_GENERATED))) {
            StatsSetupConst.setBasicStatsState(params, StatsSetupConst.TRUE);
        } else {
            StatsSetupConst.setBasicStatsState(params, StatsSetupConst.FALSE);
        }
    }

    @Override
    public boolean isIndexTable(Table table) {
        return MetaStoreUtils.isIndexTable(table);
    }

    @Override
    public boolean filterPartitionsByExpr(PartitionExpressionProxy partitionExpressionProxy, List<FieldSchema> partColumns, byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException {
        List<String> columnNames = new ArrayList<String>();
        List<PrimitiveTypeInfo> typeInfos = new ArrayList<PrimitiveTypeInfo>();
        for (FieldSchema fs : partColumns) {
            columnNames.add(fs.getName());
            typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()));
        }
        return partitionExpressionProxy.filterPartitionsByExpr(columnNames, typeInfos, expr, defaultPartitionName, partitionNames);
    }

    @Override
    public Deserializer getDeserializer(Configuration conf, Table table, boolean skipConfError) throws MetaException {
        return MetaStoreUtils.getDeserializer(conf, table, skipConfError);
    }

    @Override
    public List<FieldSchema> getFieldsFromDeserializer(String tableName, Deserializer deserializer) throws SerDeException, MetaException {
        return MetaStoreUtils.getFieldsFromDeserializer(tableName, deserializer);
    }

    @Override
    public String validateSkewedColNames(List<String> cols) {
        return MetaStoreUtils.validateSkewedColNames(cols);
    }

    @Override
    public String validateSkewedColNamesSubsetCol(List<String> skewedColNames, List<FieldSchema> cols) {
        return MetaStoreUtils.validateSkewedColNamesSubsetCol(skewedColNames, cols);
    }

    @Override
    public String validateTblColumns(List<FieldSchema> cols) {
        return MetaStoreUtils.validateTblColumns(cols);
    }

    @Override
    public boolean validateName(String name, Configuration conf) {
        return MetaStoreUtils.validateName(name, conf);
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals, Pattern partitionValidationPattern) throws MetaException {
        MetaStoreUtils.validatePartitionNameCharacters(partVals, partitionValidationPattern);
    }

    @Override
    public boolean updatePartitionStatsFast(Partition part, Table table, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext, boolean isCreate) throws MetaException {
        return MetaStoreUtils.updatePartitionStatsFast(part, wh, madeDir, forceRecompute, environmentContext);
    }

    @Override
    public boolean isExternalTable(Table table) {
        return MetaStoreUtils.isExternalTable(table);
    }

    @Override
    public boolean isArchived(Partition part) {
        return MetaStoreUtils.isArchived(part);
    }

    @Override
    public Path getOriginalLocation(Partition part) {
        return MetaStoreUtils.getOriginalLocation(part);
    }

    @Override
    public ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception {
        return MetaStoreUtils.addToClassPath(cloader, newPaths);
    }

    @Override
    public boolean isView(Table table) {
        return MetaStoreUtils.isView(table);
    }

    @Override
    public void getMergableCols(ColumnStatistics csNew, Map<String, String> parameters) {
    }

    @Override
    public void mergeColStats(ColumnStatistics csNew, ColumnStatistics csOld) throws InvalidObjectException {
        MetaStoreUtils.mergeColStats(csNew, csOld);
    }

    @Override
    public boolean enableBitVector(Configuration conf) {
        return false;
    }

    @Override
    public void setLongBitVector(LongColumnStatsData longColumnStatsData, byte[] bitVector) {
    }

    @Override
    public void setDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData, byte[] bitVector) {
    }

    @Override
    public void setDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData, byte[] bitVector) {
    }

    @Override
    public void setDateBitVector(DateColumnStatsData dateColumnStatsData, byte[] bitVector) {
    }

    @Override
    public void setStringBitVector(StringColumnStatsData stringColumnStatsData, byte[] bitVector) {
    }

    @Override
    public byte[] getLongBitVector(LongColumnStatsData longColumnStatsData) {
        return null;
    }

    @Override
    public byte[] getDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData) {
        return null;
    }

    @Override
    public byte[] getDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData) {
        return null;
    }

    @Override
    public byte[] getDateBitVector(DateColumnStatsData dateColumnStatsData) {
        return null;
    }

    @Override
    public byte[] getStringBitVector(StringColumnStatsData stringColumnStatsData) {
        return null;
    }

    @Override
    public Decimal createThriftDecimal(String s) {
        BigDecimal d = new BigDecimal(s);
        return new Decimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()), (short) d.scale());
    }

    @Override
    public String createJdoDecimalString(Decimal d) {
        return new BigDecimal(new BigInteger(d.getUnscaled()), d.getScale()).toString();
    }

    @Override
    public boolean getDensityFunctionForNDVEstimation(Configuration conf) {
        return HiveConf.getBoolVar(conf,
                HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_DENSITY_FUNCTION);
    }

    @Override
    public double getNdvTuner(Configuration conf) {
        return HiveConf.getFloatVar(conf,
                HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_TUNER);
    }

    @Override
    public int getMetastoreDirectSqlPartitionBatchSize(Configuration conf) {
        return HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_DIRECT_SQL_PARTITION_BATCH_SIZE);
    }

    @Override
    public ColumnStatisticsObj getAggregate(ColumnStatsAggregator aggregator, List<ColStatsObjWithSourceInfo> colStatWithSourceInfos, List<String> partNames, boolean areAllPartsFound) throws MetaException {

        org.apache.hadoop.hive.metastore.hbase.stats.ColumnStatsAggregator aggr = ColumnStatsAggregatorFactory.getColumnStatsAggregator(aggregator.getType(), aggregator.getNumBitVectors(), aggregator.isUseDensityFunctionForNDVEstimation());
        List<ColumnStatistics> columnStatistics = new ArrayList<>();
        for (ColStatsObjWithSourceInfo colStatsObjWithSourceInfo : colStatWithSourceInfos) {
            ColumnStatistics columnStatistic = new ColumnStatistics();
            List<ColumnStatisticsObj> objs = new ArrayList<>();
            objs.add(colStatsObjWithSourceInfo.getColStatsObj());
            columnStatistic.setStatsObj(objs);
            ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc();
            columnStatisticsDesc.setPartName(colStatsObjWithSourceInfo.getPartName());
            columnStatisticsDesc.setDbName(colStatsObjWithSourceInfo.getDbName());
            columnStatisticsDesc.setTableName(colStatsObjWithSourceInfo.getTblName());
            columnStatisticsDesc.setIsTblLevel(false);
            columnStatistic.setStatsDesc(columnStatisticsDesc);
            columnStatistics.add(columnStatistic);
        }
        return aggr.aggregate(aggregator.getColName(), partNames, columnStatistics);
    }

    @Override
    public int getNumBitVectors(Configuration conf) throws MetaException {
        try {
            return HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
        } catch (Exception e) {
            MetaException exception =  new MetaException("exception in getNumBitVectorsForNDVEstimation " + e);
            exception.initCause(e);
            throw exception;
        }

    }

    @Override
    public void setColumnStatisticsDescCatalog(String catalogId, ColumnStatisticsDesc desc) {

    }

    @Override
    public LongColumnStatsData getLongColumnStatsData(Long numNulls,
                                                      Long numDVs,
                                                      Long lowValue,
                                                      Long highValue,
                                                      byte[] bitVectors,
                                                      boolean enableBitVector) {
        LongColumnStatsData longStats = new LongColumnStatsData();
        longStats.setNumNulls(numNulls);
        longStats.setNumDVs(numDVs);
        longStats.setLowValue(lowValue);
        longStats.setHighValue(highValue);
        if (enableBitVector) {
            setLongBitVector(longStats, bitVectors);
        }
        return longStats;
    }

    @Override
    public StringColumnStatsData getStringColumnStatsData(Long numNulls,
                                                          Long numDVs,
                                                          double avglen,
                                                          Long maxlen,
                                                          byte[] bitVectors,
                                                          boolean enableBitVector) {
        StringColumnStatsData stringStats = new StringColumnStatsData();
        stringStats.setNumNulls(numNulls);
        stringStats.setNumDVs(numDVs);
        stringStats.setMaxColLen(maxlen);
        stringStats.setAvgColLen(avglen);
        if (enableBitVector) {
            setStringBitVector(stringStats, bitVectors);
        }
        return stringStats;
    }

    @Override
    public DoubleColumnStatsData getDoubleColumnStatsData(Long numNulls,
                                                          Long numDVs,
                                                          Double lowValue,
                                                          Double highValue,
                                                          byte[] bitVectors,
                                                          boolean enableBitVector) {
        DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
        doubleStats.setNumNulls(numNulls);
        doubleStats.setNumDVs(numDVs);
        doubleStats.setLowValue(lowValue);
        doubleStats.setHighValue(highValue);
        if (enableBitVector) {
            setDoubleBitVector(doubleStats, bitVectors);
        }
        return doubleStats;
    }

    @Override
    public DecimalColumnStatsData getDecimalColumnStatsData(Long numNulls,
                                                            Long numDVs,
                                                            Decimal lowValue,
                                                            Decimal highValue,
                                                            byte[] bitVectors,
                                                            boolean enableBitVector) {
        DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
        decimalStats.setNumNulls(numNulls);
        decimalStats.setNumDVs(numDVs);
        decimalStats.setLowValue(lowValue);
        decimalStats.setHighValue(highValue);
        if (enableBitVector) {
            setDecimalBitVector(decimalStats, bitVectors);
        }
        return decimalStats;
    }

    @Override
    public DateColumnStatsData getDateColumnStatsData(Long numNulls,
                                                      Long numDVs,
                                                      Date lowValue,
                                                      Date highValue,
                                                      byte[] bitVectors,
                                                      boolean enableBitVector) {
        DateColumnStatsData dateStats = new DateColumnStatsData();
        dateStats.setNumNulls(numNulls);
        dateStats.setNumDVs(numDVs);
        dateStats.setLowValue(lowValue);
        dateStats.setHighValue(highValue);
        if (enableBitVector) {
            setDateBitVector(dateStats, bitVectors);
        }
        return dateStats;
    }

    @Override
    public BooleanColumnStatsData getBooleanColumnStatsData(Long numFalses,
                                                            Long numTrues,
                                                            Long numNulls) {
        BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
        boolStats.setNumFalses(numFalses);
        boolStats.setNumTrues(numTrues);
        boolStats.setNumNulls(numNulls);
        return boolStats;
    }

    @Override
    public BinaryColumnStatsData getBinaryColumnStatsData(Long numNulls,
                                                          double avglen,
                                                          Long maxlen) {
        BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
        binaryStats.setNumNulls(numNulls);
        binaryStats.setAvgColLen(avglen);
        binaryStats.setMaxColLen(maxlen);
        return binaryStats;
    }
}
