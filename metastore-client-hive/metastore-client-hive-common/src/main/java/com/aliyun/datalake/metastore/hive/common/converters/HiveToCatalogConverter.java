package com.aliyun.datalake.metastore.hive.common.converters;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.aliyun.datalake.metastore.hive.common.utils.Utils;
import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.aliyun.datalake20200710.models.FunctionInput;
import com.aliyun.datalake20200710.models.LockObj;
import com.aliyun.datalake20200710.models.TableInput;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.hive.metastore.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

public class HiveToCatalogConverter {

    public static com.aliyun.datalake20200710.models.Database toCatalogDatabase(Database hiveDatabase) {
        com.aliyun.datalake20200710.models.Database catalogDb = new com.aliyun.datalake20200710.models.Database();
        catalogDb.description = hiveDatabase.getDescription();
        catalogDb.locationUri = hiveDatabase.getLocationUri();
        catalogDb.name = hiveDatabase.getName();
        catalogDb.ownerName = hiveDatabase.getOwnerName();
        catalogDb.ownerType = toCatalogPrincipalTypeString(hiveDatabase.getOwnerType());
        catalogDb.parameters = firstNonNull(hiveDatabase.getParameters(), Maps.newHashMap());
        catalogDb.privileges = toCatalogPrivilegeSet(hiveDatabase.getPrivileges());
        return catalogDb;
    }

    public static TableInput toCatalogTableInput(Table table) {
        TableInput tableInput = new TableInput();
        tableInput.tableName = table.getTableName();
        tableInput.databaseName = table.getDbName();
        tableInput.owner = table.getOwner();
        tableInput.sd = toCatalogStorageDescriptor(table.getSd());
        tableInput.partitionKeys = toCatalogCols(table.getPartitionKeys());
        tableInput.parameters = table.getParameters();
        tableInput.viewOriginalText = table.getViewOriginalText();
        tableInput.viewExpandedText = table.getViewExpandedText();
        tableInput.tableType = table.getTableType();
        tableInput.privileges = toCatalogPrivilegeSet(table.getPrivileges());

        if (table.isSetLastAccessTime()) {
            tableInput.lastAccessTime = table.getLastAccessTime();
        }
        if (table.isSetRetention()) {
            tableInput.retention = table.getRetention();
        }
        if (table.isSetTemporary()) {
            tableInput.temporary = table.isTemporary();
        }
        if (Utils.supportRewrite()) {
            if (table.isSetRewriteEnabled()) {
                tableInput.rewriteEnabled = table.isRewriteEnabled();
            }
        }

        return tableInput;
    }

    public static com.aliyun.datalake20200710.models.Table toCatalogTable(Table table) {
        if (table == null) {
            return null;
        }

        com.aliyun.datalake20200710.models.Table catalogTable = new com.aliyun.datalake20200710.models.Table();

        catalogTable.tableName = table.getTableName();
        catalogTable.databaseName = table.getDbName();
        catalogTable.owner = table.getOwner();
        catalogTable.sd = toCatalogStorageDescriptor(table.getSd());
        catalogTable.partitionKeys = toCatalogCols(table.getPartitionKeys());
        catalogTable.parameters = table.getParameters();
        catalogTable.viewOriginalText = table.getViewOriginalText();
        catalogTable.viewExpandedText = table.getViewExpandedText();
        catalogTable.tableType = table.getTableType();
        catalogTable.privileges = toCatalogPrivilegeSet(table.getPrivileges());

        if (table.isSetCreateTime()) {
            catalogTable.createTime = table.getCreateTime();
        }
        if (table.isSetLastAccessTime()) {
            catalogTable.lastAccessTime = table.getLastAccessTime();
        }
        if (table.isSetRetention()) {
            catalogTable.retention = table.getRetention();
        }
        if (table.isSetTemporary()) {
            catalogTable.temporary = table.isTemporary();
        }
        if (Utils.supportRewrite()) {
            if (table.isSetRewriteEnabled()) {
                catalogTable.rewriteEnabled = table.isRewriteEnabled();
            }
        }
        return catalogTable;
    }

    public static com.aliyun.datalake20200710.models.StorageDescriptor toCatalogStorageDescriptor(StorageDescriptor sd) {
        if (null == sd) {
            return null;
        }

        com.aliyun.datalake20200710.models.StorageDescriptor storageDescriptor
                = new com.aliyun.datalake20200710.models.StorageDescriptor();

        storageDescriptor.location = sd.getLocation();
        storageDescriptor.bucketCols = sd.getBucketCols();
        storageDescriptor.cols = toCatalogCols(sd.getCols());
        storageDescriptor.inputFormat = sd.getInputFormat();
        storageDescriptor.outputFormat = sd.getOutputFormat();
        storageDescriptor.compressed = sd.isCompressed();
        storageDescriptor.numBuckets = sd.getNumBuckets();
        storageDescriptor.serDeInfo = toCatalogSerdeInfo(sd.getSerdeInfo());
        storageDescriptor.sortCols = toCatalogSortCols(sd.getSortCols());
        storageDescriptor.skewedInfo = toCatalogSkewedInfo(sd.getSkewedInfo());
        storageDescriptor.parameters = sd.getParameters();
        storageDescriptor.storedAsSubDirectories = sd.isStoredAsSubDirectories();

        return storageDescriptor;
    }

    public static List<com.aliyun.datalake20200710.models.FieldSchema> toCatalogCols(List<FieldSchema> cols) {
        if (cols == null) {
            return null;
        }
        List<com.aliyun.datalake20200710.models.FieldSchema> fieldSchemas = new ArrayList<>();
        cols.forEach(c -> fieldSchemas.add(toCatalogColSchema(c)));
        return fieldSchemas;
    }

    public static com.aliyun.datalake20200710.models.FieldSchema toCatalogColSchema(FieldSchema schema) {
        com.aliyun.datalake20200710.models.FieldSchema fieldSchema = new com.aliyun.datalake20200710.models.FieldSchema();
        fieldSchema.comment = schema.getComment();
        fieldSchema.name = schema.getName();
        fieldSchema.type = schema.getType();
        return fieldSchema;
    }

    public static com.aliyun.datalake20200710.models.SerDeInfo toCatalogSerdeInfo(SerDeInfo serdeInfo) {
        if (serdeInfo == null) {
            return null;
        }
        com.aliyun.datalake20200710.models.SerDeInfo catalogSerdeInfo = new com.aliyun.datalake20200710.models.SerDeInfo();
        catalogSerdeInfo.name = serdeInfo.getName();
        catalogSerdeInfo.parameters = serdeInfo.getParameters();
        catalogSerdeInfo.serializationLib = serdeInfo.getSerializationLib();
        return catalogSerdeInfo;
    }

    public static com.aliyun.datalake20200710.models.SkewedInfo toCatalogSkewedInfo(SkewedInfo skewedInfo) {
        if (skewedInfo == null) {
            return null;
        }
        com.aliyun.datalake20200710.models.SkewedInfo catalogSkewedInfo = new com.aliyun.datalake20200710.models.SkewedInfo();
        catalogSkewedInfo.skewedColNames = skewedInfo.getSkewedColNames();
        catalogSkewedInfo.skewedColValues = skewedInfo.getSkewedColValues();
        catalogSkewedInfo.setSkewedColValueLocationMaps(toCatalogSkewedColValueLocationMaps(skewedInfo.getSkewedColValueLocationMaps()));
        return catalogSkewedInfo;
    }

    public static Map<String, String> toCatalogSkewedColValueLocationMaps(Map<List<String>, String> skewedColValueLocationMaps) {
        if (skewedColValueLocationMaps == null) {
            return null;
        }
        Map<String, String> transformedMap = new HashMap<>(skewedColValueLocationMaps.size());
        skewedColValueLocationMaps.forEach((k, v) -> {
            transformedMap.put(new Gson().toJson(k), v);
        });
        return transformedMap;
    }

    public static List<com.aliyun.datalake20200710.models.Order> toCatalogSortCols(List<Order> sortCols) {
        if (sortCols == null) {
            return null;
        }
        List<com.aliyun.datalake20200710.models.Order> catalogOrders = new ArrayList<>();
        sortCols.forEach(order -> catalogOrders.add(toCatalogOrder(order)));
        return catalogOrders;
    }

    public static com.aliyun.datalake20200710.models.Order toCatalogOrder(Order order) {
        if (order == null) {
            return null;
        }
        com.aliyun.datalake20200710.models.Order catalogOrder = new com.aliyun.datalake20200710.models.Order();
        catalogOrder.col = order.getCol();
        catalogOrder.order = order.getOrder();
        return catalogOrder;
    }

    public static com.aliyun.datalake20200710.models.Partition toCatalogPartition(
            Partition hivePartition) {
        if (hivePartition == null) {
            return null;
        }

        com.aliyun.datalake20200710.models.Partition catalogPartition = new com.aliyun.datalake20200710.models.Partition();

        catalogPartition.databaseName = hivePartition.getDbName();
        catalogPartition.tableName = hivePartition.getTableName();
        catalogPartition.createTime = hivePartition.getCreateTime();
        catalogPartition.lastAccessTime = hivePartition.getLastAccessTime();
        catalogPartition.sd = toCatalogStorageDescriptor(hivePartition.getSd());
        catalogPartition.privileges = toCatalogPrivilegeSet(hivePartition.getPrivileges());

        if (hivePartition.getParameters() != null) {
            catalogPartition.parameters = new HashMap<>(hivePartition.getParameters());
        }
        if (hivePartition.getValues() != null) {
            catalogPartition.values = new ArrayList<>(hivePartition.getValues());
        }

        return catalogPartition;
    }

    public static com.aliyun.datalake20200710.models.PartitionInput toCatalogPartitionInput(
            Partition hivePartition) {
        if (hivePartition == null) {
            return null;
        }

        com.aliyun.datalake20200710.models.PartitionInput partitionInput = new com.aliyun.datalake20200710.models.PartitionInput();
        partitionInput.databaseName = hivePartition.getDbName();
        partitionInput.tableName = hivePartition.getTableName();
        partitionInput.lastAccessTime = hivePartition.getLastAccessTime();
        partitionInput.sd = toCatalogStorageDescriptor(hivePartition.getSd());
        partitionInput.privileges = toCatalogPrivilegeSet(hivePartition.getPrivileges());

        if (hivePartition.getParameters() != null) {
            partitionInput.parameters = new HashMap<>(hivePartition.getParameters());
        }
        if (hivePartition.getValues() != null) {
            partitionInput.values = new ArrayList<>(hivePartition.getValues());
        }

        return partitionInput;
    }

    public static com.aliyun.datalake20200710.models.PrincipalPrivilegeSet toCatalogPrivilegeSet(
            PrincipalPrivilegeSet privileges) {
        if (privileges == null) {
            return null;
        }
        com.aliyun.datalake20200710.models.PrincipalPrivilegeSet catalogPrivilegeSet
                = new com.aliyun.datalake20200710.models.PrincipalPrivilegeSet();
        catalogPrivilegeSet.groupPrivileges = toCatalogPrivilegeMap(privileges.getGroupPrivileges());
        catalogPrivilegeSet.rolePrivileges = toCatalogPrivilegeMap(privileges.getRolePrivileges());
        catalogPrivilegeSet.userPrivileges = toCatalogPrivilegeMap(privileges.getUserPrivileges());
        return catalogPrivilegeSet;
    }

    private static Map<String, List<com.aliyun.datalake20200710.models.PrivilegeGrantInfo>> toCatalogPrivilegeMap(
            Map<String, List<PrivilegeGrantInfo>> groupPrivileges) {
        if (groupPrivileges == null) {
            return null;
        }

        Map<String, List<com.aliyun.datalake20200710.models.PrivilegeGrantInfo>> catalogPrivileges = new HashMap<>(
                groupPrivileges.size());
        groupPrivileges.forEach((k, v) -> catalogPrivileges.put(k, toCatalogGrantInfoList(v)));

        return catalogPrivileges;
    }

    private static List<com.aliyun.datalake20200710.models.PrivilegeGrantInfo> toCatalogGrantInfoList(
            List<PrivilegeGrantInfo> privilegeGrantInfos) {
        if (privilegeGrantInfos == null) {
            return null;
        }

        List<com.aliyun.datalake20200710.models.PrivilegeGrantInfo> infoList = new ArrayList<>(privilegeGrantInfos.size());
        privilegeGrantInfos.forEach(t -> infoList.add(toCatalogGrantInfo(t)));

        return infoList;
    }

    private static com.aliyun.datalake20200710.models.PrivilegeGrantInfo toCatalogGrantInfo(
            PrivilegeGrantInfo hiveInfo) {
        if (hiveInfo == null) {
            return null;
        }

        com.aliyun.datalake20200710.models.PrivilegeGrantInfo privilegeGrantInfo
                = new com.aliyun.datalake20200710.models.PrivilegeGrantInfo();
        privilegeGrantInfo.privilege = hiveInfo.getPrivilege();
        privilegeGrantInfo.grantor = hiveInfo.getGrantor();
        privilegeGrantInfo.grantOption = hiveInfo.isGrantOption();
        privilegeGrantInfo.grantorType = toCatalogPrincipalTypeString(hiveInfo.getGrantorType());
        privilegeGrantInfo.createTime = hiveInfo.getCreateTime();

        return privilegeGrantInfo;
    }

    public static String toCatalogPrincipalTypeString(PrincipalType ownerType) {
        if (ownerType == null) {
            return null;
        }
        return ownerType.name();
    }

    public static com.aliyun.datalake20200710.models.Function toCatalogFunction(String catalogName, Function function) {
        if (null == function) {
            return null;
        }
        com.aliyun.datalake20200710.models.Function catalogFunction = new com.aliyun.datalake20200710.models.Function();
        catalogFunction.catalogId = catalogName;
        catalogFunction.databaseName = function.getDbName();
        catalogFunction.className = function.getClassName();
        catalogFunction.functionName = function.getFunctionName();
        catalogFunction.functionType = toCatalogFunctionTypeString(function.getFunctionType());
        catalogFunction.ownerName = function.getOwnerName();
        catalogFunction.ownerType = toCatalogPrincipalTypeString(function.getOwnerType());
        catalogFunction.createTime = function.getCreateTime();
        catalogFunction.resourceUri = toCatalogResourceUriList(function.getResourceUris());
        return catalogFunction;
    }

    public static FunctionInput toCatalogFunctionInput(String catalogId, Function function) {
        if (null == function) {
            return null;
        }
        com.aliyun.datalake20200710.models.FunctionInput catalogFunction =
                new com.aliyun.datalake20200710.models.FunctionInput();
        catalogFunction.className = function.getClassName();
        catalogFunction.functionName = function.getFunctionName();
        catalogFunction.functionType = toCatalogFunctionTypeString(function.getFunctionType());
        catalogFunction.ownerName = function.getOwnerName();
        catalogFunction.ownerType = toCatalogPrincipalTypeString(function.getOwnerType());
        catalogFunction.resourceUri = toCatalogResourceUriList(function.getResourceUris());
        return catalogFunction;
    }

    public static String toCatalogFunctionTypeString(FunctionType functionType) {
        if (functionType == null) {
            return null;
        }
        return functionType.name();
    }

    public static String toCatalogResourceTypeString(ResourceType resourceType) {
        if (resourceType == null) {
            return null;
        }
        return resourceType.name();
    }

    public static List<com.aliyun.datalake20200710.models.ResourceUri> toCatalogResourceUriList(
            List<ResourceUri> resourceUris) {
        if (resourceUris == null) {
            return null;
        }
        List<com.aliyun.datalake20200710.models.ResourceUri> uris = new ArrayList<>();
        resourceUris.forEach(t -> uris.add(toCatalogResourceUri(t)));
        return uris;
    }

    public static com.aliyun.datalake20200710.models.ResourceUri toCatalogResourceUri(ResourceUri resourceUri) {
        if (resourceUri == null) {
            return null;
        }
        com.aliyun.datalake20200710.models.ResourceUri catalogUri
                = new com.aliyun.datalake20200710.models.ResourceUri();
        catalogUri.uri = resourceUri.getUri();
        catalogUri.resourceType = toCatalogResourceTypeString(resourceUri.getResourceType());
        return catalogUri;
    }

    public static com.aliyun.datalake20200710.models.Table convertIndexToCatalogTableObject(Index hiveIndex) {
        // convert index object to a table object
        com.aliyun.datalake20200710.models.Table catalogIndexTableObject = new com.aliyun.datalake20200710.models.Table();
        catalogIndexTableObject.tableName = hiveIndex.getIndexName();
        catalogIndexTableObject.createTime = hiveIndex.getCreateTime();
        catalogIndexTableObject.lastAccessTime = hiveIndex.getLastAccessTime();
        catalogIndexTableObject.sd = toCatalogStorageDescriptor(hiveIndex.getSd());
        catalogIndexTableObject.parameters = hiveIndex.getParameters() ==
                null ? new HashMap<>() : hiveIndex.getParameters();

        // store rest of fields in index to parameter map
        catalogIndexTableObject.parameters.put(DataLakeConfig.INDEX_DEFERRED_REBUILD,
                hiveIndex.isDeferredRebuild() ? "TRUE" : "FALSE");
        catalogIndexTableObject.parameters.put(DataLakeConfig.INDEX_TABLE_NAME, hiveIndex.getIndexTableName());
        catalogIndexTableObject.parameters.put(DataLakeConfig.INDEX_HANDLER_CLASS, hiveIndex.getIndexHandlerClass());
        catalogIndexTableObject.parameters.put(DataLakeConfig.INDEX_DB_NAME, hiveIndex.getDbName());
        catalogIndexTableObject.parameters.put(DataLakeConfig.INDEX_ORIGIN_TABLE_NAME, hiveIndex.getOrigTableName());

        return catalogIndexTableObject;
    }


    public static com.aliyun.datalake20200710.models.ColumnStatistics toCatalogColumnStats(IHiveShims hiveShims, ColumnStatistics columnStatistics) throws IOException {
        com.aliyun.datalake20200710.models.ColumnStatistics catalogStats = new com.aliyun.datalake20200710.models.ColumnStatistics();
        catalogStats.setColumnStatisticsDesc(toCatalogColumnStatsDesc(columnStatistics.getStatsDesc()));
        catalogStats.setColumnStatisticsObjList(toCatalogColumnStatsObjs(hiveShims, columnStatistics.getStatsObj()));
        return catalogStats;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatisticsObj toCatalogColumnStatsObj(IHiveShims hiveShims, ColumnStatisticsObj columnStatisticsObj) throws IOException {
        com.aliyun.datalake20200710.models.ColumnStatisticsObj catalogStatsObj = new com.aliyun.datalake20200710.models.ColumnStatisticsObj();
        if (columnStatisticsObj.getColName() != null) {
            catalogStatsObj.setColumnName(columnStatisticsObj.getColName());
        }
        if (columnStatisticsObj.getColType() != null) {
            catalogStatsObj.setColumnType(columnStatisticsObj.getColType());
        }
        catalogStatsObj.setColumnStatisticsData(toCatalogColumnStatsData(hiveShims, columnStatisticsObj.getStatsData()));
        return catalogStatsObj;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatisticsObj.ColumnStatisticsObjColumnStatisticsData toCatalogColumnStatsData(IHiveShims hiveShims, ColumnStatisticsData columnStatisticsData) throws IOException {
        com.aliyun.datalake20200710.models.ColumnStatisticsObj.ColumnStatisticsObjColumnStatisticsData catalogColumnStatisticsData = new com.aliyun.datalake20200710.models.ColumnStatisticsObj.ColumnStatisticsObjColumnStatisticsData();
        catalogColumnStatisticsData.setStatisticsData(toCatalogColumnStatsDataCore(hiveShims, columnStatisticsData));
        catalogColumnStatisticsData.setStatisticsType(String.valueOf(columnStatisticsData.getSetField().getThriftFieldId()));
        return catalogColumnStatisticsData;
    }

    public static String toCatalogColumnStatsDataCore(IHiveShims hiveShims, ColumnStatisticsData columnStatisticsData) throws IOException {
        String statsDataCore = "";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        switch (columnStatisticsData.getSetField()) {
            case BOOLEAN_STATS:
                BooleanColumnStatsData boolStats = columnStatisticsData.getBooleanStats();
                root.put("numNulls", boolStats.isSetNumNulls() ? boolStats.getNumNulls() : null);
                root.put("numTrues", boolStats.isSetNumTrues() ? boolStats.getNumTrues() : null);
                root.put("numFalses", boolStats.isSetNumFalses() ? boolStats.getNumFalses() : null);
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case LONG_STATS:
                LongColumnStatsData longStats = columnStatisticsData.getLongStats();
                root.put("numNulls", longStats.isSetNumNulls() ? longStats.getNumNulls() : null);
                root.put("numDVs", longStats.isSetNumDVs() ? longStats.getNumDVs() : null);
                root.put("lowValue", longStats.isSetLowValue() ? longStats.getLowValue() : null);
                root.put("highValue", longStats.isSetHighValue() ? longStats.getHighValue() : null);
                root.put("bitVectors", hiveShims.getLongBitVector(longStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case DOUBLE_STATS:
                DoubleColumnStatsData doubleStats = columnStatisticsData.getDoubleStats();
                root.put("numNulls", doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null);
                root.put("numDVs", doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null);
                root.put("lowValue", doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null);
                root.put("highValue", doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
                root.put("bitVectors", hiveShims.getDoubleBitVector(doubleStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case STRING_STATS:
                StringColumnStatsData stringStats = columnStatisticsData.getStringStats();
                root.put("numNulls", stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null);
                root.put("numDVs", stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null);
                root.put("maxColLen", stringStats.isSetMaxColLen() ? stringStats.getMaxColLen() : null);
                root.put("avgColLen", stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : null);
                root.put("bitVectors", hiveShims.getStringBitVector(stringStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case BINARY_STATS:
                BinaryColumnStatsData binaryStats = columnStatisticsData.getBinaryStats();
                root.put("numNulls", binaryStats.isSetNumNulls() ? binaryStats.getNumNulls() : null);
                root.put("maxColLen", binaryStats.isSetMaxColLen() ? binaryStats.getMaxColLen() : null);
                root.put("avgColLen", binaryStats.isSetAvgColLen() ? binaryStats.getAvgColLen() : null);
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case DECIMAL_STATS:
                DecimalColumnStatsData decimalStats = columnStatisticsData.getDecimalStats();
                root.put("numNulls", decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null);
                root.put("numDVs", decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null);
                root.put("lowValue", decimalStats.isSetLowValue() ? hiveShims.createJdoDecimalString(decimalStats.getLowValue()) : null);
                root.put("highValue", decimalStats.isSetHighValue() ? hiveShims.createJdoDecimalString(decimalStats.getHighValue()) : null);
                root.put("bitVectors", hiveShims.getDecimalBitVector(decimalStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case DATE_STATS:
                DateColumnStatsData dateStats = columnStatisticsData.getDateStats();
                root.put("numNulls", dateStats.isSetNumNulls() ? dateStats.getNumNulls() : null);
                root.put("numDVs", dateStats.isSetNumDVs() ? dateStats.getNumDVs() : null);
                root.put("lowValue", dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null);
                root.put("highValue", dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
                root.put("bitVectors", hiveShims.getDateBitVector(dateStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            default:
                break;
        }
        return statsDataCore;
    }

    public static List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> toCatalogColumnStatsObjs(IHiveShims hiveShims, List<ColumnStatisticsObj> columnStatisticsObjs) throws IOException {
        List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> catalogObjs = new ArrayList<>();
        for (ColumnStatisticsObj obj : columnStatisticsObjs) {
            catalogObjs.add(toCatalogColumnStatsObj(hiveShims, obj));
        }
        return catalogObjs;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatisticsDesc toCatalogColumnStatsDesc(ColumnStatisticsDesc columnStatisticsDesc) {
        com.aliyun.datalake20200710.models.ColumnStatisticsDesc catalogStatsDesc = new com.aliyun.datalake20200710.models.ColumnStatisticsDesc();
        if (columnStatisticsDesc.getLastAnalyzed() == 0) {
            long time = System.currentTimeMillis() / 1000;
            catalogStatsDesc.setLastAnalyzedTime(time);
        } else {
            catalogStatsDesc.setLastAnalyzedTime(columnStatisticsDesc.getLastAnalyzed());
        }
        if (columnStatisticsDesc.getPartName() != null) {
            catalogStatsDesc.setPartitionName(columnStatisticsDesc.getPartName());
        }
        return catalogStatsDesc;
    }

    public static LockObj toCatalogLockObj(String catalogId, LockComponent lockComponent) {
        LockObj lockObj = new LockObj();
        lockObj.setCatalogId(catalogId);
        lockObj.setDatabaseName(lockComponent.getDbname());
//        lockObj.setPartitionName(lockComponent.getPartitionname());
        lockObj.setTableName(lockComponent.getTablename());
        return lockObj;
    }
}
