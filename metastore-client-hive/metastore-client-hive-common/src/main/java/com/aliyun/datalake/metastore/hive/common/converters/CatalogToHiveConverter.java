package com.aliyun.datalake.metastore.hive.common.converters;

import com.aliyun.datalake.metastore.common.Action;
import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake.metastore.hive.common.utils.Utils;
import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.*;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

public class CatalogToHiveConverter {

    private static final ImmutableMap<String, MetaStoreException> EXCEPTION_MAP
            = ImmutableMap.<String, MetaStoreException>builder()
            .put("UnknownTable", UnknownTableException::new)
            .put("UnknownDB", UnknownDBException::new)
            .put("AlreadyExists", AlreadyExistsException::new)
            .put("InvalidPartition", InvalidPartitionException::new)
            .put("UnknownPartition", UnknownPartitionException::new)
            .put("InvalidObject", InvalidObjectException::new)
            .put("NoSuchObject", NoSuchObjectException::new)
            .put("InvalidOperation", InvalidOperationException::new)
            .put("ConfigValSecurityError", ConfigValSecurityException::new)
            .put("InvalidInput", InvalidInputException::new)
            .put("NoSuchTxn", NoSuchTxnException::new)
            .put("TxnAbortedError", TxnAbortedException::new)
            .put("TxnOpenError", TxnOpenException::new)
            .put("NoSuchLock", NoSuchLockException::new)
            .build();

    public static TException toHiveException(ResultModel<?> result, Action action, Exception e) {
        if (result == null) {
            return DataLakeUtil.throwException(new MetaException("Empty result returned from catalog."), e);
        }
        String code = result.code;
        String msg = result.message;
        String requestId = result.requestId;
        String message = String.join(" ", "Action:", action.name(), "ErrorCode:", code,
                "Message:", msg, "RequestId:", requestId);
        if (EXCEPTION_MAP.containsKey(code)) {
            return DataLakeUtil.throwException(EXCEPTION_MAP.get(code).get(message), e);
        }
        return DataLakeUtil.throwException(new MetaException(message), e);
    }

    public static Database toHiveDatabase(com.aliyun.datalake20200710.models.Database catalogDatabase) {
        Database hiveDatabase = new Database();
        hiveDatabase.setName(catalogDatabase.name);
        hiveDatabase.setDescription(catalogDatabase.description);
        String location = catalogDatabase.locationUri;
        hiveDatabase.setLocationUri(location == null ? "" : location);
        hiveDatabase.setParameters(firstNonNull(catalogDatabase.parameters, Maps.newHashMap()));
        hiveDatabase.setOwnerName(catalogDatabase.ownerName);
        hiveDatabase.setOwnerType(toHivePrincipalTypeString(catalogDatabase.ownerType));
        hiveDatabase.setParameters(catalogDatabase.parameters);
        hiveDatabase.setPrivileges(toHivePrivilegeSet(catalogDatabase.privileges));
        return hiveDatabase;
    }

    public static Table toHiveTable(com.aliyun.datalake20200710.models.Table catalogTable) {
        if (catalogTable == null) {
            return null;
        }
        Table table = new Table();
        table.setTableName(catalogTable.tableName);
        table.setDbName(catalogTable.databaseName);
        table.setOwner(catalogTable.owner);
        table.setSd(toHiveStorageDescriptor(catalogTable.sd));
        table.setPartitionKeys(toHiveCols(catalogTable.partitionKeys));
        table.setParameters(catalogTable.parameters);
        table.setViewOriginalText(catalogTable.viewOriginalText);
        table.setViewExpandedText(catalogTable.viewExpandedText);
        table.setTableType(catalogTable.tableType);
        table.setPrivileges(toHivePrivilegeSet(catalogTable.privileges));
        if (null != catalogTable.createTime) {
            table.setCreateTime(catalogTable.createTime);
        }
        if (null != catalogTable.lastAccessTime) {
            table.setLastAccessTime(catalogTable.lastAccessTime);
        }
        if (null != catalogTable.retention) {
            table.setRetention(catalogTable.retention);
        }
        if (null != catalogTable.temporary) {
            table.setTemporary(catalogTable.temporary);
        }
        if (Utils.supportRewrite()) {
            if (null != catalogTable.rewriteEnabled) {
                table.setRewriteEnabled(catalogTable.rewriteEnabled);
            }
        }
        return table;
    }

    public static StorageDescriptor toHiveStorageDescriptor(com.aliyun.datalake20200710.models.StorageDescriptor sd) {
        if (sd == null) {
            return null;
        }

        StorageDescriptor storageDescriptor = new StorageDescriptor();

        storageDescriptor.setCols(toHiveCols(sd.cols));
        storageDescriptor.setLocation(sd.location);
        storageDescriptor.setInputFormat(sd.inputFormat);
        storageDescriptor.setOutputFormat(sd.outputFormat);
        storageDescriptor.setSerdeInfo(toHiveSerDeInfo(sd.serDeInfo));
        storageDescriptor.setBucketCols(sd.bucketCols);
        storageDescriptor.setSortCols(toHiveSortCols(sd.sortCols));
        storageDescriptor.setParameters(sd.parameters);
        storageDescriptor.setSkewedInfo(toHiveSkewedInfo(sd.skewedInfo));
        if (null != sd.compressed) {
            storageDescriptor.setCompressed(sd.compressed);
        }
        if (null != sd.numBuckets) {
            storageDescriptor.setNumBuckets(sd.numBuckets);
        }
        if (null != sd.storedAsSubDirectories) {
            storageDescriptor.setStoredAsSubDirectories(sd.storedAsSubDirectories);
        }

        return storageDescriptor;
    }

    public static List<FieldSchema> toHiveCols(List<com.aliyun.datalake20200710.models.FieldSchema> catalogCols) {
        if (null == catalogCols) {
            return null;
        }
        List<FieldSchema> fieldSchemas = new ArrayList<>();
        catalogCols.forEach(c -> fieldSchemas.add(toHiveColSchema(c)));
        return fieldSchemas;
    }

    public static FieldSchema toHiveColSchema(com.aliyun.datalake20200710.models.FieldSchema catalogColSchema) {
        if (null == catalogColSchema) {
            return null;
        }
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName(catalogColSchema.name);
        fieldSchema.setType(catalogColSchema.type);
        fieldSchema.setComment(catalogColSchema.comment);
        return fieldSchema;
    }

    public static SerDeInfo toHiveSerDeInfo(com.aliyun.datalake20200710.models.SerDeInfo catalogSerDeInfo) {
        if (null == catalogSerDeInfo) {
            return null;
        }
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setName(catalogSerDeInfo.name);
        serDeInfo.setSerializationLib(catalogSerDeInfo.serializationLib);
        serDeInfo.setParameters(catalogSerDeInfo.parameters);
        return serDeInfo;
    }

    public static SkewedInfo toHiveSkewedInfo(com.aliyun.datalake20200710.models.SkewedInfo catalogSkewedInfo) {
        if (null == catalogSkewedInfo) {
            return null;
        }
        SkewedInfo skewedInfo = new SkewedInfo();
        skewedInfo.setSkewedColNames(catalogSkewedInfo.skewedColNames);
        skewedInfo.setSkewedColValues(catalogSkewedInfo.skewedColValues);

        if (catalogSkewedInfo.skewedColValueLocationMaps != null) {
            skewedInfo.setSkewedColValueLocationMaps(toHiveSkewedColValueLocationMaps(catalogSkewedInfo.getSkewedColValueLocationMaps()));
        }

        return skewedInfo;
    }

    public static Map<List<String>, String> toHiveSkewedColValueLocationMaps(Map<String, String> skewedColValueLocationMaps) {
        Gson gson = new Gson();
        Map<List<String>, String> hashMap = new HashMap<>();
        for (Map.Entry<String, String> entry : skewedColValueLocationMaps.entrySet()) {
            ArrayList arrayList = gson.fromJson(entry.getKey(), ArrayList.class);
            hashMap.put(arrayList, entry.getValue());
        }
        return hashMap;
    }

    public static List<Order> toHiveSortCols(List<com.aliyun.datalake20200710.models.Order> sortCols) {
        if (null == sortCols) {
            return null;
        }
        List<Order> catalogOrders = new ArrayList<>();
        sortCols.forEach(order -> catalogOrders.add(toHiveOrder(order)));
        return catalogOrders;
    }

    public static Order toHiveOrder(com.aliyun.datalake20200710.models.Order catalogOrder) {
        if (null == catalogOrder) {
            return null;
        }
        Order order = new Order();
        order.setOrder(catalogOrder.order);
        order.setCol(catalogOrder.col);
        return order;
    }

    public static Partition toHivePartition(com.aliyun.datalake20200710.models.Partition catalogPartition) {
        Partition hivePartition = new Partition();

        hivePartition.setDbName(catalogPartition.databaseName);
        hivePartition.setTableName(catalogPartition.tableName);
        hivePartition.setCreateTime(nullOrZero(catalogPartition.createTime));
        hivePartition.setLastAccessTime(nullOrZero(catalogPartition.lastAccessTime));
        hivePartition.setSd(toHiveStorageDescriptor(catalogPartition.sd));
        hivePartition.setPrivileges(toHivePrivilegeSet(catalogPartition.privileges));
        hivePartition.setParameters(new HashMap<>(catalogPartition.parameters));
        hivePartition.setValues(new ArrayList<>(catalogPartition.values));

        return hivePartition;
    }

    private static int nullOrZero(Integer input) {
        return Objects.isNull(input) ? 0 : input;
    }

    public static PrincipalPrivilegeSet toHivePrivilegeSet(
            com.aliyun.datalake20200710.models.PrincipalPrivilegeSet privilegeSet) {
        if (privilegeSet == null) {
            return null;
        }

        PrincipalPrivilegeSet set = new PrincipalPrivilegeSet();
        set.setRolePrivileges(toHivePrivilegeMap(privilegeSet.rolePrivileges));
        set.setGroupPrivileges(toHivePrivilegeMap(privilegeSet.groupPrivileges));
        set.setUserPrivileges(toHivePrivilegeMap(privilegeSet.userPrivileges));

        return set;
    }

    private static Map<String, List<PrivilegeGrantInfo>> toHivePrivilegeMap(
            Map<String, List<com.aliyun.datalake20200710.models.PrivilegeGrantInfo>> rolePrivileges) {
        if (rolePrivileges == null) {
            return null;
        }

        Map<String, List<PrivilegeGrantInfo>> map = new HashMap<>(rolePrivileges.size());
        rolePrivileges.forEach((k, v) -> map.put(k, toHiveGrantInfoList(v)));

        return map;
    }

    private static List<PrivilegeGrantInfo> toHiveGrantInfoList(
            List<com.aliyun.datalake20200710.models.PrivilegeGrantInfo> catalogGrantInfos) {
        if (catalogGrantInfos == null) {
            return null;
        }

        List<PrivilegeGrantInfo> infoList = new ArrayList<>(catalogGrantInfos.size());
        catalogGrantInfos.forEach(t -> infoList.add(toHiveGrantInfo(t)));

        return infoList;
    }

    private static PrivilegeGrantInfo toHiveGrantInfo(com.aliyun.datalake20200710.models.PrivilegeGrantInfo catalogGrantInfo) {
        if (catalogGrantInfo == null) {
            return null;
        }

        PrivilegeGrantInfo hiveInfo = new PrivilegeGrantInfo();
        hiveInfo.setGrantOption(catalogGrantInfo.grantOption);
        hiveInfo.setGrantor(catalogGrantInfo.grantor);
        hiveInfo.setGrantorType(toHivePrincipalType(catalogGrantInfo.grantorType));
        hiveInfo.setPrivilege(catalogGrantInfo.privilege);
        hiveInfo.setCreateTime(catalogGrantInfo.createTime);

        return hiveInfo;
    }

    public static PrincipalType toHivePrincipalType(String ownerType) {
        if (ownerType == null) {
            return null;
        }
        return PrincipalType.valueOf(ownerType);
    }

    public static PrincipalType toHivePrincipalTypeString(String ownerType) {
        if (ownerType == null) {
            return null;
        }
        return PrincipalType.valueOf(ownerType);
    }

    public static Function toHiveFunction(com.aliyun.datalake20200710.models.Function function) {
        if (function == null) {
            return null;
        }
        Function hiveFunction = new Function();
        hiveFunction.setCreateTime(function.createTime);
        hiveFunction.setDbName(function.databaseName);
        hiveFunction.setClassName(function.className);
        hiveFunction.setFunctionName(function.functionName);
        FunctionType functionType = toHiveFunctionType(function.functionType);
        hiveFunction.setFunctionType(functionType);
        hiveFunction.setOwnerName(function.ownerName);
        PrincipalType ownerType = toHivePrincipalType(function.ownerType);
        hiveFunction.setOwnerType(ownerType);
        hiveFunction.setResourceUris(toHiveResourceUriList(function.resourceUri));
        return hiveFunction;
    }

    public static FunctionType toHiveFunctionType(String functionType) {
        if (functionType == null) {
            return null;
        }
        return FunctionType.valueOf(functionType);
    }

    public static ResourceType toHiveResourceType(String resourceType) {
        if (resourceType == null) {
            return null;
        }
        return ResourceType.valueOf(resourceType);
    }

    public static List<ResourceUri> toHiveResourceUriList(
            List<com.aliyun.datalake20200710.models.ResourceUri> resourceUris) {
        if (resourceUris == null) {
            return null;
        }
        List<ResourceUri> uris = new ArrayList<>(resourceUris.size());
        resourceUris.forEach(t -> uris.add(toHiveResourceUri(t)));
        return uris;
    }

    public static ResourceUri toHiveResourceUri(com.aliyun.datalake20200710.models.ResourceUri catalogResourceUri) {
        if (catalogResourceUri == null) {
            return null;
        }
        ResourceType type = toHiveResourceType(catalogResourceUri.resourceType);
        ResourceUri hiveUri = new ResourceUri(type, catalogResourceUri.uri);
        return hiveUri;
    }

    public static Index ToHiveIndex(com.aliyun.datalake20200710.models.Table catalogTable) {
        Index hiveIndex = new Index();
        Map<String, String> parameters = catalogTable.parameters;
        hiveIndex.setIndexName(catalogTable.tableName);
        hiveIndex.setCreateTime(catalogTable.createTime);
        hiveIndex.setLastAccessTime(catalogTable.lastAccessTime);
        hiveIndex.setSd(toHiveStorageDescriptor(catalogTable.sd));
        hiveIndex.setParameters(catalogTable.parameters);

        hiveIndex.setDeferredRebuild(parameters.get(DataLakeConfig.INDEX_DEFERRED_REBUILD).equals("TRUE"));
        hiveIndex.setIndexHandlerClass(parameters.get(DataLakeConfig.INDEX_HANDLER_CLASS));
        hiveIndex.setDbName(parameters.get(DataLakeConfig.INDEX_DB_NAME));
        hiveIndex.setOrigTableName(parameters.get(DataLakeConfig.INDEX_ORIGIN_TABLE_NAME));
        hiveIndex.setIndexTableName(parameters.get(DataLakeConfig.INDEX_TABLE_NAME));

        return hiveIndex;
    }

    public static ColumnStatistics toHiveColumnStats(String catalogId, String dbName, String tblName, com.aliyun.datalake20200710.models.ColumnStatistics columnStatistics, boolean isTableLevel, boolean enableBitVector, IHiveShims hiveShims) throws IOException {
        ColumnStatistics hiveColumnStats = new ColumnStatistics();
        hiveColumnStats.setStatsDesc(toHiveColumnStatsDesc(catalogId, dbName, tblName, isTableLevel, columnStatistics.getColumnStatisticsDesc(), hiveShims));
        hiveColumnStats.setStatsObj(toHiveColumnStatsObjs(columnStatistics.getColumnStatisticsObjList(), enableBitVector, hiveShims));
        return hiveColumnStats;
    }

    public static ColumnStatisticsObj toHiveColumnStatsObj(com.aliyun.datalake20200710.models.ColumnStatisticsObj columnStatisticsObj, boolean enableBitVector, IHiveShims hiveShims) throws IOException {
        ColumnStatisticsObj hiveColumnStats = new ColumnStatisticsObj();
        hiveColumnStats.setColName(columnStatisticsObj.getColumnName());
        hiveColumnStats.setColType(columnStatisticsObj.getColumnType());
        String type = columnStatisticsObj.getColumnStatisticsData().getStatisticsType();
        ColumnStatisticsData hiveColumnStatsData = new ColumnStatisticsData();
        ColumnStatisticsData._Fields field = hiveColumnStatsData.fieldForId(Integer.parseInt(type));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(columnStatisticsObj.getColumnStatisticsData().getStatisticsData());
        switch (field) {
            case BOOLEAN_STATS:
                hiveColumnStatsData.setBooleanStats(hiveShims.getBooleanColumnStatsData(root.get("numFalses").asLong(), root.get("numTrues").asLong(), root.get("numNulls").asLong()));
                break;
            case LONG_STATS:
                hiveColumnStatsData.setLongStats(hiveShims.getLongColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), root.get("lowValue").asLong(), root.get("highValue").asLong(), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case DOUBLE_STATS:
                hiveColumnStatsData.setDoubleStats(hiveShims.getDoubleColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), root.get("lowValue").asDouble(), root.get("highValue").asDouble(), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case STRING_STATS:
                hiveColumnStatsData.setStringStats(hiveShims.getStringColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), root.get("avgColLen").asDouble(), root.get("maxColLen").asLong(), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case BINARY_STATS:
                hiveColumnStatsData.setBinaryStats(hiveShims.getBinaryColumnStatsData(root.get("numNulls").asLong(), root.get("avgColLen").asDouble(), root.get("maxColLen").asLong()));
                break;
            case DECIMAL_STATS:
                hiveColumnStatsData.setDecimalStats(hiveShims.getDecimalColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), hiveShims.createThriftDecimal(root.get("lowValue").textValue()), hiveShims.createThriftDecimal(root.get("highValue").textValue()), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case DATE_STATS:
                hiveColumnStatsData.setDateStats(hiveShims.getDateColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), new Date(root.get("lowValue").asLong()), new Date(root.get("highValue").asLong()), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            default:
                break;
        }
        hiveColumnStats.setStatsData(hiveColumnStatsData);
        return hiveColumnStats;
    }

    public static List<ColumnStatisticsObj> toHiveColumnStatsObjs(List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> columnStatisticsObj, boolean enableBitVector, IHiveShims hiveShims) throws IOException {
        List<ColumnStatisticsObj> hiveColumnStatsObjs = new ArrayList<>();
        for (com.aliyun.datalake20200710.models.ColumnStatisticsObj obj : columnStatisticsObj) {
            hiveColumnStatsObjs.add(toHiveColumnStatsObj(obj, enableBitVector, hiveShims));
        }
        return hiveColumnStatsObjs;
    }

    public static Map<String, List<ColumnStatisticsObj>> toHiveColumnStatsObjMaps(Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> columnStatisticsObjMap, boolean enableBitVector, IHiveShims hiveShims) throws IOException {
        Map<String, List<ColumnStatisticsObj>> hiveStatsMap = new HashMap<>();
        Iterator<Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = columnStatisticsObjMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> iter = iterator.next();
            hiveStatsMap.put(iter.getKey(), toHiveColumnStatsObjs(iter.getValue(), enableBitVector, hiveShims));
        }
        return hiveStatsMap;
    }

    public static Map<String, ColumnStatistics> toHiveColumnStatsMaps(String catalogId, String dbName, String tblName, Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> columnStatisticsObjMap, boolean isTableLevel, boolean enableBitVector, IHiveShims hiveShims) throws IOException {
        Map<String, ColumnStatistics> hiveStatsMap = new HashMap<>();
        Iterator<Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = columnStatisticsObjMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> iter = iterator.next();
            List<ColumnStatisticsObj> objs = toHiveColumnStatsObjs(iter.getValue(), enableBitVector, hiveShims);
            ColumnStatistics stats = new ColumnStatistics();
            ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
            desc.setTableName(tblName);
            desc.setDbName(dbName);
            desc.setPartName(iter.getKey());
            desc.setIsTblLevel(isTableLevel);
            hiveShims.setColumnStatisticsDescCatalog(catalogId, desc);
            stats.setStatsDesc(desc);
            stats.setStatsObj(objs);
            hiveStatsMap.put(iter.getKey(), stats);
        }
        return hiveStatsMap;
    }

    public static ColumnStatisticsDesc toHiveColumnStatsDesc(String catalogId, String dbName, String tblName, boolean isTableLevel, com.aliyun.datalake20200710.models.ColumnStatisticsDesc columnStatistics, IHiveShims hiveShims) {
        ColumnStatisticsDesc hiveColumnStatsDesc = new ColumnStatisticsDesc();
        hiveColumnStatsDesc.setPartName(columnStatistics.getPartitionName());
        hiveColumnStatsDesc.setLastAnalyzed(columnStatistics.getLastAnalyzedTime());
        hiveColumnStatsDesc.setTableName(dbName);
        hiveColumnStatsDesc.setDbName(tblName);
        hiveShims.setColumnStatisticsDescCatalog(catalogId, hiveColumnStatsDesc);
        hiveColumnStatsDesc.setIsTblLevel(isTableLevel);
        return hiveColumnStatsDesc;
    }

    public static LockResponse toHiveLockResponse(com.aliyun.datalake20200710.models.LockStatus lockStatus) {
        LockResponse lockResponse = new LockResponse();
        if (lockStatus != null) {
            lockResponse.setLockid(lockStatus.getLockId());
            lockResponse.setState(toLockState(lockStatus.getLockState()));
        }
        return lockResponse;
    }

    public static LockState toLockState(String state) {
        if ("NOT_ACQUIRED".equals(state)) {
            return LockState.NOT_ACQUIRED;
        } else if ("ACQUIRED".equals(state)) {
            return LockState.ACQUIRED;
        } else {
            return LockState.NOT_ACQUIRED;
        }
    }

    interface MetaStoreException {
        TException get(String msg);
    }
}
