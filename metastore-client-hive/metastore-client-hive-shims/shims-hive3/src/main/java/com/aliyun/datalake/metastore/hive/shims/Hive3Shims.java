package com.aliyun.datalake.metastore.hive.shims;

import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.metastore.columnstats.cache.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
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

public class Hive3Shims implements IHiveShims {
    private static final String HIVE_VERSION = "3.";
    private static final Logger logger = LoggerFactory.getLogger(Hive3Shims.class);

    static boolean supportsVersion(String version) {
        return version.startsWith(HIVE_VERSION);
    }

    //getclass and newinstance are different methods in hive3 and hive2
    @Override
    public <T> Class<? extends RawStore> getClass(String className, Class<? extends RawStore> clazz) throws MetaException {
        return JavaUtils.getClass(className, clazz);
    }

    @Override
    public <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes, Object[] initargs) {
        return JavaUtils.newInstance(theClass, parameterTypes, initargs);
    }

    //objectPairs is different class in hive3 and hive2;
    @Override
    public <T> List<ObjectPair<Integer, byte[]>> objectPairConvert(T partExprs) {
        List<ObjectPair<Integer, byte[]>> objectPairs = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.utils.ObjectPair part : (List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>>) partExprs) {
            ObjectPair<Integer, byte[]> objectPair = ObjectPair.create((Integer) part.getFirst(), (byte[]) part.getSecond());
            objectPairs.add(objectPair);
        }
        return objectPairs;
    }

    //mkdirs and deletedirs are different
    @Override
    public boolean mkdirs(Warehouse wh, Path f, boolean inheritPermCandidate, Boolean enableFsOperation) throws MetaException {
        long startTime = System.currentTimeMillis();
        boolean mkdir = false;
        try {
            if (enableFsOperation) {
                mkdir = wh.mkdirs(f);
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
        return deleteDir(wh, f, recursive, false, db, enableFsOperation);
    }

    @Override
    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, boolean ifPurge, Database db, Boolean enableFsOperation) throws MetaException {
        long startTime = System.currentTimeMillis();
        boolean deleteDir = false;
        try {
            if (enableFsOperation) {
                deleteDir = wh.deleteDir(f, recursive, db);
            } else {
                deleteDir = true;
            }
            return deleteDir;
        } finally {
            final boolean deleteDirFinal = deleteDir;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.deleteDir, result:{}, cost:{}ms, path : {}, db : {}, recursive : {}",
                                                     realMessage(enableFsOperation), deleteDirFinal, System.currentTimeMillis() - startTime, f,
                                                     ReplChangeManager.isSourceOfReplication(db), recursive));
        }
    }

    @Override
    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, boolean ifPurge, boolean needCmRecycle, Boolean enableFsOperation) throws MetaException {
        long startTime = System.currentTimeMillis();
        boolean deleteDir = false;
        try {
            if (enableFsOperation) {
                deleteDir = wh.deleteDir(f, recursive, ifPurge, needCmRecycle);
            } else {
                deleteDir = true;
            }
            return deleteDir;
        } finally {
            final boolean deleteDirFinal = deleteDir;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.deleteDir, result:{}, cost:{}ms, path : {}, recursive : {}, ifPurge : {}, needCm : {}", realMessage(enableFsOperation), deleteDirFinal,
                                                     System.currentTimeMillis() - startTime, f, recursive, ifPurge, needCmRecycle));
        }
    }

    //hive3's database needs catalog
    @Override
    public void addCatalogForDb(Database db, String catalogId) {
        db.setCatalogName(catalogId);
    }

    @Override
    public void updateTableStatsFast(Database db, Table tbl, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {
        MetaStoreUtils.updateTableStatsSlow(db, tbl, wh, madeDir, forceRecompute, environmentContext);
    }

    @Override
    public boolean requireCalStats(Configuration conf, Partition oldPart, Partition newPart, Table tbl, EnvironmentContext environmentContext) {
        return MetaStoreUtils.requireCalStats(oldPart, newPart, tbl, environmentContext);
    }

    @Override
    public boolean isIndexTable(Table table) {
        return false;
    }

    @Override
    public boolean filterPartitionsByExpr(PartitionExpressionProxy partitionExpressionProxy, List<FieldSchema> partColumns, byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException {
        return partitionExpressionProxy.filterPartitionsByExpr(partColumns, expr, defaultPartitionName, partitionNames);
    }

    @Override
    public Deserializer getDeserializer(Configuration conf, Table table, boolean skipConfError) throws MetaException {
        return HiveMetaStoreUtils.getDeserializer(conf, table, skipConfError);
    }

    @Override
    public List<FieldSchema> getFieldsFromDeserializer(String tableName, Deserializer deserializer) throws SerDeException, MetaException {
        return HiveMetaStoreUtils.getFieldsFromDeserializer(tableName, deserializer);
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
        return MetaStoreUtils.updatePartitionStatsFast(part, table, wh, madeDir, forceRecompute, environmentContext, isCreate);
    }

    @Override
    public boolean isFastStatsSame(Partition oldPart, Partition newPart) {
        return MetaStoreUtils.isFastStatsSame(oldPart, newPart);
    }

    @Override
    public void updateBasicState(EnvironmentContext environmentContext, Map<String, String> params) {
        MetaStoreUtils.updateBasicState(environmentContext, params);
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
        MetaStoreUtils.getMergableCols(csNew, parameters);
    }

    @Override
    public void mergeColStats(ColumnStatistics csNew, ColumnStatistics csOld) throws InvalidObjectException {
        MetaStoreUtils.mergeColStats(csNew, csOld);
    }

    @Override
    public boolean enableBitVector(Configuration conf) {
        return MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR);
    }

    @Override
    public void setLongBitVector(LongColumnStatsData longColumnStatsData, byte[] bitVector) {
        longColumnStatsData.setBitVectors(bitVector);
    }

    @Override
    public void setDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData, byte[] bitVector) {
        doubleColumnStatsData.setBitVectors(bitVector);
    }

    @Override
    public void setDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData, byte[] bitVector) {
        decimalColumnStatsData.setBitVectors(bitVector);
    }

    @Override
    public void setDateBitVector(DateColumnStatsData dateColumnStatsData, byte[] bitVector) {
        dateColumnStatsData.setBitVectors(bitVector);
    }

    @Override
    public void setStringBitVector(StringColumnStatsData stringColumnStatsData, byte[] bitVector) {
        stringColumnStatsData.setBitVectors(bitVector);
    }

    @Override
    public byte[] getLongBitVector(LongColumnStatsData longColumnStatsData) {
        return longColumnStatsData.isSetBitVectors() ? longColumnStatsData.getBitVectors() : null;
    }

    @Override
    public byte[] getDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData) {
        return doubleColumnStatsData.isSetBitVectors() ? doubleColumnStatsData.getBitVectors() : null;
    }

    @Override
    public byte[] getDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData) {
        return decimalColumnStatsData.isSetBitVectors() ? decimalColumnStatsData.getBitVectors() : null;
    }

    @Override
    public byte[] getDateBitVector(DateColumnStatsData dateColumnStatsData) {
        return dateColumnStatsData.isSetBitVectors() ? dateColumnStatsData.getBitVectors() : null;
    }

    @Override
    public byte[] getStringBitVector(StringColumnStatsData stringColumnStatsData) {
        return stringColumnStatsData.isSetBitVectors() ? stringColumnStatsData.getBitVectors() : null;
    }

    @Override
    public Decimal createThriftDecimal(String s) {
        BigDecimal d = new BigDecimal(s);
        return new Decimal((short) d.scale(), ByteBuffer.wrap(d.unscaledValue().toByteArray()));
    }

    @Override
    public String createJdoDecimalString(Decimal d) {
        return new BigDecimal(new BigInteger(d.getUnscaled()), d.getScale()).toString();
    }

    @Override
    public boolean getDensityFunctionForNDVEstimation(Configuration conf) {
        return MetastoreConf.getBoolVar(conf,
                MetastoreConf.ConfVars.STATS_NDV_DENSITY_FUNCTION);
    }

    @Override
    public double getNdvTuner(Configuration conf) {
        return MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.STATS_NDV_TUNER);
    }

    @Override
    public int getMetastoreDirectSqlPartitionBatchSize(Configuration conf) {
        return MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.DIRECT_SQL_PARTITION_BATCH_SIZE);
    }

    @Override
    public ColumnStatisticsObj getAggregate(ColumnStatsAggregator aggregator, List<ColStatsObjWithSourceInfo> colStatWithSourceInfos, List<String> partNames, boolean areAllPartsFound) throws MetaException {
        org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator aggr = ColumnStatsAggregatorFactory.getColumnStatsAggregator(aggregator.getType(), aggregator.isUseDensityFunctionForNDVEstimation(), aggregator.getNdvTuner());
        List<org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo> colStatWithSourceInfoHives = new ArrayList<>();
        colStatWithSourceInfos.stream().forEach(stat -> colStatWithSourceInfoHives.add(new org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo(stat.getColStatsObj(), stat.getCatName(), stat.getDbName(), stat.getTblName(), stat.getPartName())));
        return aggr.aggregate(colStatWithSourceInfoHives, partNames, areAllPartsFound);
    }

    @Override
    public int getNumBitVectors(Configuration conf) throws MetaException {
        return 0;
    }

    @Override
    public void setColumnStatisticsDescCatalog(String catalogId, ColumnStatisticsDesc desc) {
        desc.setCatName(catalogId);
    }

    @Override
    public LongColumnStatsData getLongColumnStatsData(Long numNulls,
                                                      Long numDVs,
                                                      Long lowValue,
                                                      Long highValue,
                                                      byte[] bitVectors,
                                                      boolean enableBitVector) {
        LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
        longStats.setNumNulls(numNulls);
        if (highValue != null) {
            longStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            longStats.setLowValue(lowValue);
        }
        longStats.setNumDVs(numDVs);
        if (enableBitVector) {
            longStats.setBitVectors(bitVectors);
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
        StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
        stringStats.setNumNulls(numNulls);
        stringStats.setAvgColLen(avglen);
        stringStats.setMaxColLen(maxlen);
        stringStats.setNumDVs(numDVs);
        if (enableBitVector) {
            stringStats.setBitVectors(bitVectors);
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
        DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
        doubleStats.setNumNulls(numNulls);
        if (highValue != null) {
            doubleStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            doubleStats.setLowValue(lowValue);
        }
        doubleStats.setNumDVs(numDVs);
        if (enableBitVector) {
            doubleStats.setBitVectors(bitVectors);
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
        DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
        decimalStats.setNumNulls(numNulls);
        if (highValue != null) {
            decimalStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            decimalStats.setLowValue(lowValue);
        }
        decimalStats.setNumDVs(numDVs);
        if (enableBitVector) {
            decimalStats.setBitVectors(bitVectors);
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
        DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
        dateStats.setNumNulls(numNulls);
        if (highValue != null) {
            dateStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            dateStats.setLowValue(lowValue);
        }
        dateStats.setNumDVs(numDVs);
        if (enableBitVector) {
            dateStats.setBitVectors(bitVectors);
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
