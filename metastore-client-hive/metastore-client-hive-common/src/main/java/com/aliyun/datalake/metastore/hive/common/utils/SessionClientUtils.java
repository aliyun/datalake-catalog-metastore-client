package com.aliyun.datalake.metastore.hive.common.utils;

import com.aliyun.datalake.metastore.hive.shims.IHiveShims;
import com.aliyun.datalake.metastore.hive.shims.ShimsLoader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SessionClientUtils {
    /**
     * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#deepCopy(Table)
     */
    public static final IHiveShims HIVE_SHIMS = ShimsLoader.getHiveShims();
    public static final Logger LOG = LoggerFactory.getLogger(SessionClientUtils.class);

    public static Table deepCopy(Table table) {
        Table copy = null;
        if (table != null) {
            copy = new Table(table);
        }
        return copy;
    }

    /**
     * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#deepCopy(FieldSchema)
     */
    public static FieldSchema deepCopy(FieldSchema schema) {
        FieldSchema copy = null;
        if (schema != null) {
            copy = new FieldSchema(schema);
        }
        return copy;
    }

    /**
     * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#deepCopyFieldSchemas(List)
     */
    public static List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
        List<FieldSchema> copy = null;
        if (schemas != null) {
            copy = new ArrayList<FieldSchema>();
            for (FieldSchema schema : schemas) {
                copy.add(deepCopy(schema));
            }
        }
        return copy;
    }

    /**
     * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#deepCopy(PrincipalPrivilegeSet)
     */
    public static PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
        PrincipalPrivilegeSet copy = null;
        if (pps != null) {
            copy = new PrincipalPrivilegeSet(pps);
        }
        return copy;
    }

    public static List<String> mergeSortList(List<String> tableNames, Set<String> tempTableNames) {
        Set<String> allTableNames = new HashSet<String>(tableNames.size() + tempTableNames.size());
        allTableNames.addAll(tableNames);
        allTableNames.addAll(tempTableNames);
        tableNames = new ArrayList<String>(allTableNames);
        Collections.sort(tableNames);
        return tableNames;
    }

    public static List<String> mergeSortListWithPattern(List<String> tableNames, Set<String> tempTableNames, String tablePattern) {
        tablePattern = tablePattern.replaceAll("\\*", ".*");
        Pattern pattern = Pattern.compile(tablePattern);
        Matcher matcher = pattern.matcher("");
        Set<String> combinedTableNames = new HashSet<String>();
        for (String tableName : tempTableNames) {
            matcher.reset(tableName);
            if (matcher.matches()) {
                combinedTableNames.add(tableName);
            }
        }

        // Combine/sort temp and normal table results
        combinedTableNames.addAll(tableNames);
        tableNames = new ArrayList<String>(combinedTableNames);
        Collections.sort(tableNames);
        return tableNames;
    }

    public static void mergeTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes, List<TableMeta> tableMetas, Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> tmpTables) {
        List<Matcher> dbPatternList = new ArrayList<>();
        for (String element : dbPatterns.split("\\|")) {
            dbPatternList.add(Pattern.compile(element.replaceAll("\\*", ".*")).matcher(""));
        }
        List<Matcher> tblPatternList = new ArrayList<>();
        for (String element : tablePatterns.split("\\|")) {
            tblPatternList.add(Pattern.compile(element.replaceAll("\\*", ".*")).matcher(""));
        }
        for (Map.Entry<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> outer : tmpTables.entrySet()) {
            if (!matchesAny(outer.getKey(), dbPatternList)) {
                continue;
            }
            for (Map.Entry<String, org.apache.hadoop.hive.ql.metadata.Table> inner : outer.getValue().entrySet()) {
                org.apache.hadoop.hive.ql.metadata.Table table = inner.getValue();
                String tableName = table.getTableName();
                String typeString = table.getTableType().name();
                if (tableTypes != null && !tableTypes.contains(typeString)) {
                    continue;
                }
                if (!matchesAny(inner.getKey(), tblPatternList)) {
                    continue;
                }
                TableMeta tableMeta = new TableMeta(table.getDbName(), tableName, typeString);
                tableMeta.setComments(table.getProperty("comment"));
                tableMetas.add(tableMeta);
            }
        }
    }

    private static boolean matchesAny(String string, List<Matcher> matchers) {
        for (Matcher matcher : matchers) {
            if (matcher.reset(string).matches()) {
                return true;
            }
        }
        return matchers.isEmpty();
    }

    public static void createTempTableDir(SessionState ss, Warehouse wh, Table tbl, String dbName, String tblName, Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables, Boolean enableFsOperation)
            throws MetaException {
        Path tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
        if (tblPath == null) {
            throw new MetaException("Temp table path not set for " + tbl.getTableName());
        } else {
            if (!wh.isDir(tblPath)) {
                if (!HIVE_SHIMS.mkdirs(wh, tblPath, true, enableFsOperation)) {
                    throw new MetaException(tblPath
                            + " is not a directory or unable to create one");
                }
            }
            // Make sure location string is in proper format
            tbl.getSd().setLocation(tblPath.toString());
        }

        // Add temp table info to current session
        org.apache.hadoop.hive.ql.metadata.Table tTable = new org.apache.hadoop.hive.ql.metadata.Table(tbl);
        if (tables == null) {
            tables = new HashMap<String, org.apache.hadoop.hive.ql.metadata.Table>();
            ss.getTempTables().put(dbName, tables);
        }
        tables.put(tblName, tTable);
    }

    public static Path isExistsTempLocation(Table table, Warehouse wh, HiveConf conf) throws MetaException {
        Path tablePath = null;
        String pathStr = table.getSd().getLocation();
        if (pathStr != null) {
            try {
                tablePath = new Path(table.getSd().getLocation());
                if (!wh.isWritable(tablePath.getParent())) {
                    throw new MetaException("Table metadata not deleted since " + tablePath.getParent() +
                            " is not writable by " + conf.getUser());
                }
            } catch (IOException err) {
                MetaException metaException =
                        new MetaException("Error checking temp table path for " + table.getTableName());
                metaException.initCause(err);
                throw metaException;
            }
        }
        return tablePath;
    }

    public static void dropTempDir(Table table, Path tablePath, Warehouse wh, boolean deleteData,
                                   EnvironmentContext envContext, Boolean enableFsOperation) {
        // Delete table data
        if (deleteData && !MetaStoreUtils.isExternalTable(table)) {
            try {
                boolean ifPurge = false;
                if (envContext != null) {
                    ifPurge = Boolean.parseBoolean(envContext.getProperties().get("ifPurge"));
                }
                HIVE_SHIMS.deleteDir(wh, tablePath, true, ifPurge, false, enableFsOperation);
            } catch (Exception err) {
                LOG.error("Failed to delete temp table directory: " + tablePath, err);
                // Forgive error
            }
        }
    }
}