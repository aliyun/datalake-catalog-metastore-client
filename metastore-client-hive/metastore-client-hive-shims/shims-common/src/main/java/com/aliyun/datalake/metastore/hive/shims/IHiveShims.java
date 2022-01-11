package com.aliyun.datalake.metastore.hive.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public interface IHiveShims {

    static String realMessage(boolean enableFsOperation) {
        return enableFsOperation ? "real" : "mock";
    }

    public <T> Class<? extends RawStore> getClass(String className, Class<? extends RawStore> clazz) throws MetaException;

    public <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes, Object[] initargs);

    public <T> List<ObjectPair<Integer, byte[]>> objectPairConvert(T partExprs);

    public boolean mkdirs(Warehouse wh, Path f, boolean inheritPermCandidate, Boolean enableFsOperation) throws MetaException;

    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, Boolean enableFsOperation) throws MetaException;

    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, Database db, Boolean enableFsOperation) throws MetaException;

    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, boolean ifPurge, boolean needCmRecycle, Boolean enableFsOperation) throws MetaException;

    public boolean deleteDir(Warehouse wh, Path f, boolean recursive, boolean ifPurge, Database db, Boolean enableFsOperation) throws MetaException;

    public void addCatalogForDb(Database db, String catalogId);

    public void updateTableStatsFast(Database db, Table tbl, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException;

    public boolean requireCalStats(Configuration conf, Partition oldPart, Partition newPart, Table tbl, EnvironmentContext environmentContext);

    public boolean isFastStatsSame(Partition oldPart, Partition newPart);

    public void updateBasicState(EnvironmentContext environmentContext, Map<String,String> params);

    public boolean isIndexTable(Table table);

    public boolean filterPartitionsByExpr(PartitionExpressionProxy partitionExpressionProxy, List<FieldSchema> partColumns, byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException;

    public Deserializer getDeserializer(Configuration conf, Table table, boolean skipConfError) throws MetaException;

    public List<FieldSchema> getFieldsFromDeserializer(String tableName, Deserializer deserializer) throws SerDeException, MetaException;

    public String validateSkewedColNames(List<String> cols);

    public String validateSkewedColNamesSubsetCol(List<String> skewedColNames, List<FieldSchema> cols);

    public String validateTblColumns(List<FieldSchema> cols);

    public boolean validateName(String name, Configuration conf);

    public void validatePartitionNameCharacters(List<String> partVals, Pattern partitionValidationPattern) throws MetaException;

    public boolean updatePartitionStatsFast(Partition part, Table table, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext, boolean isCreate) throws MetaException;

    public boolean isExternalTable(Table table);

    public boolean isArchived(Partition part);

    public Path getOriginalLocation(Partition part);

    public ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception;

    public boolean isView(Table table);

    public void getMergableCols(ColumnStatistics csNew, Map<String, String> parameters);

    public void mergeColStats(ColumnStatistics csNew, ColumnStatistics csOld) throws InvalidObjectException;

    public boolean enableBitVector(Configuration conf);

    public void setLongBitVector(LongColumnStatsData longColumnStatsData, byte[] bitVector);

    public void setDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData, byte[] bitVector);

    public void setDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData, byte[] bitVector);

    public void setDateBitVector(DateColumnStatsData dateColumnStatsData, byte[] bitVector);

    public void setStringBitVector(StringColumnStatsData stringColumnStatsData, byte[] bitVector);

    public byte[] getLongBitVector(LongColumnStatsData longColumnStatsData);

    public byte[] getDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData);

    public byte[] getDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData);

    public byte[] getDateBitVector(DateColumnStatsData dateColumnStatsData);

    public byte[] getStringBitVector(StringColumnStatsData stringColumnStatsData);

    public Decimal createThriftDecimal(String s);

    public String createJdoDecimalString(Decimal d);

    public boolean getDensityFunctionForNDVEstimation(Configuration conf);

    public double getNdvTuner(Configuration conf);

    public int getMetastoreDirectSqlPartitionBatchSize(Configuration conf);

    public ColumnStatisticsObj getAggregate(ColumnStatsAggregator aggregator, List<ColStatsObjWithSourceInfo> colStatWithSourceInfos, List<String> partNames, boolean areAllPartsFound) throws MetaException;

    public int getNumBitVectors(Configuration conf) throws MetaException;

    public void setColumnStatisticsDescCatalog(String catalogId, ColumnStatisticsDesc desc);

    public LongColumnStatsData getLongColumnStatsData(Long numNulls, Long numDVs, Long lowValue, Long highValue, byte[] bitVectors, boolean enableBitVector);

    public StringColumnStatsData getStringColumnStatsData(Long numNulls, Long numDVs, double avglen, Long maxlen, byte[] bitVectors, boolean enableBitVector);

    public DoubleColumnStatsData getDoubleColumnStatsData(Long numNulls, Long numDVs, Double lowValue, Double highValue, byte[] bitVectors, boolean enableBitVector);

    public DecimalColumnStatsData getDecimalColumnStatsData(Long numNulls, Long numDVs, Decimal lowValue, Decimal highValue, byte[] bitVectors, boolean enableBitVector);

    public DateColumnStatsData getDateColumnStatsData(Long numNulls, Long numDVs, Date lowValue, Date highValue, byte[] bitVectors, boolean enableBitVector);

    public BooleanColumnStatsData getBooleanColumnStatsData(Long numFalses, Long numTrues, Long numNulls);

    public BinaryColumnStatsData getBinaryColumnStatsData(Long numNulls, double avglen, Long maxlen);
}
