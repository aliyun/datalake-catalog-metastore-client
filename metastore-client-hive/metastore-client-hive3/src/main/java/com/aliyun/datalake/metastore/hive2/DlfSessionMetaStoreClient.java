package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.hive.common.utils.SessionClientUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.aliyun.datalake.metastore.hive.common.utils.SessionClientUtils.*;

/**
 * @see org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient
 */
public class DlfSessionMetaStoreClient extends DlfMetaStoreClient implements IMetaStoreClient {

    private static final Logger LOG = LoggerFactory.getLogger(DlfSessionMetaStoreClient.class);
    private Warehouse wh = null;

    //hive3的RetryingMetaStoreClient中的getProxy支持的construct要求的conf的类型是configuration，而不是hiveconf，详见 RetryingMetaStoreClient中的getProxy函数
    public DlfSessionMetaStoreClient(
            Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    public DlfSessionMetaStoreClient(Configuration hiveConf, HiveMetaHookLoader hiveMetaHookLoader) throws MetaException {
        this(hiveConf, hiveMetaHookLoader, null);
    }

    private static boolean haveTableColumnsChanged(Table oldt,
                                                   Table newt) {
        List<FieldSchema> oldCols = oldt.getSd().getCols();
        List<FieldSchema> newCols = newt.getSd().getCols();
        if (oldCols.size() != newCols.size()) {
            return true;
        }
        Iterator<FieldSchema> oldColsIter = oldCols.iterator();
        Iterator<FieldSchema> newColsIter = newCols.iterator();
        while (oldColsIter.hasNext()) {
            // Don't use FieldSchema.equals() since it also compares comments,
            // which is unnecessary for this method.
            if (!fieldSchemaEqualsIgnoreComment(oldColsIter.next(), newColsIter.next())) {
                return true;
            }
        }
        return false;
    }

    private static boolean fieldSchemaEqualsIgnoreComment(FieldSchema left, FieldSchema right) {
        // Just check name/type for equality, don't compare comment
        if (!left.getName().equals(right.getName())) {
            return true;
        }
        if (!left.getType().equals(right.getType())) {
            return true;
        }
        return false;
    }

    public static Map<String, org.apache.hadoop.hive.ql.metadata.Table> getTempTablesForDatabase(String dbName) {
        return getTempTables().get(dbName);
    }

    /**
     * @param dbName  actual database name
     * @param tblName actual table name or search pattern (for error message)
     */
    public static Map<String, org.apache.hadoop.hive.ql.metadata.Table> getTempTablesForDatabase(String dbName,
                                                                                                 String tblName) {
        return getTempTables(Warehouse.getQualifiedName(dbName, tblName)).
                get(dbName);
    }

    private static Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> getTempTables(String msg) {
        SessionState ss = SessionState.get();
        if (ss == null) {
            LOG.debug("No current SessionState, skipping temp tables for " + msg);
            return Collections.emptyMap();
        }
        return ss.getTempTables();
    }

    public static Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> getTempTables() {
        SessionState ss = SessionState.get();
        if (ss == null) {
            LOG.debug("No current SessionState, skipping temp tables");
            return Collections.emptyMap();
        }
        return ss.getTempTables();
    }

    private static void mergeColumnStats(Map<String, ColumnStatisticsObj> oldStats,
                                         ColumnStatistics newStats) {
        List<ColumnStatisticsObj> newColList = newStats.getStatsObj();
        if (newColList != null) {
            for (ColumnStatisticsObj colStat : newColList) {
                // This is admittedly a bit simple, StatsObjectConverter seems to allow
                // old stats attributes to be kept if the new values do not overwrite them.
                oldStats.put(colStat.getColName().toLowerCase(), colStat);
            }
        }
    }

    private Warehouse getWh() throws MetaException {
        if (wh == null) {
            wh = new Warehouse(getConf());
        }
        return wh;
    }

    @Override
    protected void create_table_with_environment_context(
            Table tbl, EnvironmentContext envContext)
            throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {

        if (tbl.isTemporary()) {
            createTempTable(tbl, envContext);
            return;
        }
        // non-temp tables should use underlying client.
        super.create_table_with_environment_context(tbl, envContext);
    }

    @Override
    protected void drop_table_with_environment_context(String dbname, String name,
                                                       boolean deleteData, boolean ignoreUnknownTable, EnvironmentContext envContext) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {
        // First try temp table
        Table table = getTempTable(dbname, name);
        if (table != null) {
            try {
                deleteTempTableColumnStatsForTable(dbname, name);
            } catch (NoSuchObjectException err) {
                // No stats to delete, forgivable error.
                LOG.info("Object not found in metastore", err);
            }
            dropTempTable(table, deleteData, envContext);
            return;
        }

        // Try underlying client
        super.drop_table_with_environment_context(dbname, name, deleteData, ignoreUnknownTable, envContext);
    }

    @Override
    public Table getTable(String dbname, String name) throws MetaException,
            TException, NoSuchObjectException {
        // First check temp tables
        Table table = getTempTable(dbname, name);
        if (table != null) {
            return SessionClientUtils.deepCopy(table);  // Original method used deepCopy(), do the same here.
        }

        // Try underlying client
        return super.getTable(dbname, name);
    }

    @Override
    public List<String> getAllTables(String dbName) throws TException {
        List<String> tableNames = super.getAllTables(dbName);

        // May need to merge with list of temp tables
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName);
        if (tables == null || tables.size() == 0) {
            return tableNames;
        }

        // Merge and sort result
        return mergeSortList(tableNames, tables.keySet());
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws TException {
        List<String> tableNames = super.getTables(dbName, tablePattern);

        // May need to merge with list of temp tables
        dbName = dbName.toLowerCase();
        tablePattern = tablePattern.toLowerCase();
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName, tablePattern);
        if (tables == null || tables.size() == 0) {
            return tableNames;
        }
        return mergeSortListWithPattern(tableNames, tables.keySet(), tablePattern);
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
            throws MetaException, TException {
        List<TableMeta> tableMetas = super.getTableMeta(dbPatterns, tablePatterns, tableTypes);
        Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> tmpTables = getTempTables();
        if (tmpTables.isEmpty()) {
            return tableMetas;
        }

        mergeTableMeta(dbPatterns, tablePatterns, tableTypes, tableMetas, tmpTables);
        return tableMetas;
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName,
                                             List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {

        dbName = dbName.toLowerCase();
        if (SessionState.get() == null || SessionState.get().getTempTables().size() == 0) {
            // No temp tables, just call underlying client
            return super.getTableObjectsByName(dbName, tableNames);
        }

        List<Table> tables =
                new ArrayList<Table>();
        for (String tableName : tableNames) {
            try {
                Table table = getTable(dbName, tableName);
                if (table != null) {
                    tables.add(table);
                }
            } catch (NoSuchObjectException err) {
                // Ignore error, just return the valid tables that are found.
            }
        }
        return tables;
    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException,
            TException, UnknownDBException {
        // First check temp tables
        Table table = getTempTable(databaseName, tableName);
        if (table != null) {
            return true;
        }

        // Try underlying client
        return super.tableExists(databaseName, tableName);
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tableName)
            throws MetaException, TException, UnknownTableException,
            UnknownDBException {
        // First check temp tables
        Table table = getTempTable(dbName, tableName);
        if (table != null) {
            return SessionClientUtils.deepCopyFieldSchemas(table.getSd().getCols());
        }

        // Try underlying client
        return super.getSchema(dbName, tableName);
    }

    @Deprecated
    @Override
    public void alter_table(String dbname, String tbl_name, Table new_tbl,
                            boolean cascade) throws InvalidOperationException, MetaException, TException {
        Table old_tbl = getTempTable(dbname, tbl_name);
        if (old_tbl != null) {
            //actually temp table does not support partitions, cascade is not applicable here
            alterTempTable(dbname, tbl_name, old_tbl, new_tbl, null);
            return;
        }
        super.alter_table(dbname, tbl_name, new_tbl, cascade);
    }

    @Override
    public void alter_table(String dbname, String tbl_name,
                            Table new_tbl) throws InvalidOperationException,
            MetaException, TException {
        Table old_tbl = getTempTable(dbname, tbl_name);
        if (old_tbl != null) {
            // actually temp table does not support partitions, cascade is not
            // applicable here
            alterTempTable(dbname, tbl_name, old_tbl, new_tbl, null);
            return;
        }
        super.alter_table(dbname, tbl_name, new_tbl);
    }

    @Override
    public void alter_table_with_environmentContext(String dbname, String tbl_name,
                                                    Table new_tbl, EnvironmentContext envContext)
            throws InvalidOperationException, MetaException, TException {
        // First try temp table
        Table old_tbl = getTempTable(dbname, tbl_name);
        if (old_tbl != null) {
            alterTempTable(dbname, tbl_name, old_tbl, new_tbl, envContext);
            return;
        }

        // Try underlying client
        super.alter_table_with_environmentContext(dbname, tbl_name, new_tbl, envContext);
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
                                                   String userName, List<String> groupNames) throws MetaException,
            TException {
        // If caller is looking for temp table, handle here. Otherwise pass on to underlying client.
        if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
            Table table =
                    getTempTable(hiveObject.getDbName(), hiveObject.getObjectName());
            if (table != null) {
                return SessionClientUtils.deepCopy(table.getPrivileges());
            }
        }

        return super.get_privilege_set(hiveObject, userName, groupNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            InvalidInputException {
        if (request.getColStatsSize() == 1) {
            ColumnStatistics colStats = request.getColStatsIterator().next();
            ColumnStatisticsDesc desc = colStats.getStatsDesc();
            String dbName = desc.getDbName().toLowerCase();
            String tableName = desc.getTableName().toLowerCase();
            if (getTempTable(dbName, tableName) != null) {
                return updateTempTableColumnStats(dbName, tableName, colStats);
            }
        }
        return super.setPartitionColumnStatistics(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
                                                              List<String> colNames) throws NoSuchObjectException, MetaException, TException,
            InvalidInputException, InvalidObjectException {
        if (getTempTable(dbName, tableName) != null) {
            return getTempTableColumnStats(dbName, tableName, colNames);
        }
        return super.getTableColumnStatistics(dbName, tableName, colNames);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            InvalidInputException {
        if (getTempTable(dbName, tableName) != null) {
            return deleteTempTableColumnStats(dbName, tableName, colName);
        }
        return super.deleteTableColumnStatistics(dbName, tableName, colName);
    }

    private void createTempTable(Table tbl,
                                 EnvironmentContext envContext) throws AlreadyExistsException, InvalidObjectException,
            MetaException, NoSuchObjectException, TException {

        SessionState ss = SessionState.get();
        if (ss == null) {
            throw new MetaException("No current SessionState, cannot create temporary table"
                    + tbl.getDbName() + "." + tbl.getTableName());
        }

        // We may not own the table object, create a copy
        tbl = deepCopyAndLowerCaseTable(tbl);

        String dbName = tbl.getDbName();
        String tblName = tbl.getTableName();
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName, tblName);
        if (tables != null && tables.containsKey(tblName)) {
            throw new MetaException("Temporary table " + dbName + "." + tblName + " already exists");
        }

        // Create temp table directory
        createTempTableDir(ss, getWh(), tbl, dbName, tblName, tables, getEnableFsOperation());

    }

    private Table getTempTable(String dbName, String tableName) {
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName.toLowerCase(), tableName.toLowerCase());
        if (tables != null) {
            org.apache.hadoop.hive.ql.metadata.Table table = tables.get(tableName.toLowerCase());
            if (table != null) {
                return table.getTTable();
            }
        }
        return null;
    }

    private void alterTempTable(String dbname, String tbl_name,
                                Table oldt,
                                Table newt,
                                EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
        dbname = dbname.toLowerCase();
        tbl_name = tbl_name.toLowerCase();
        boolean shouldDeleteColStats = false;

        // Disallow changing temp table location
        if (!newt.getSd().getLocation().equals(oldt.getSd().getLocation())) {
            throw new MetaException("Temp table location cannot be changed");
        }

        Table newtCopy = deepCopyAndLowerCaseTable(newt);
        /**
         * updateTableStatsFast has been removed by hive3
         */
//        MetaStoreUtils.updateTableStatsFast(newtCopy,
//                getWh().getFileStatusesForSD(newtCopy.getSd()), false, true, envContext);
        org.apache.hadoop.hive.ql.metadata.Table newTable = new org.apache.hadoop.hive.ql.metadata.Table(newtCopy);
        String newDbName = newTable.getDbName();
        String newTableName = newTable.getTableName();
        if (!newDbName.equals(oldt.getDbName()) || !newTableName.equals(oldt.getTableName())) {
            // Table was renamed.

            // Do not allow temp table rename if the new name already exists as a temp table
            if (getTempTable(newDbName, newTableName) != null) {
                throw new MetaException("Cannot rename temporary table to " + newTableName
                        + " - temporary table already exists with the same name");
            }

            // Remove old temp table entry, and add new entry to list of temp tables.
            // Note that for temp tables there is no need to rename directories
            Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbname, tbl_name);
            if (tables == null || tables.remove(tbl_name) == null) {
                throw new MetaException("Could not find temp table entry for " + dbname + "." + tbl_name);
            }
            shouldDeleteColStats = true;

            tables = getTempTablesForDatabase(newDbName, tbl_name);
            if (tables == null) {
                tables = new HashMap<String, org.apache.hadoop.hive.ql.metadata.Table>();
                SessionState.get().getTempTables().put(newDbName, tables);
            }
            tables.put(newTableName, newTable);
        } else {
            if (haveTableColumnsChanged(oldt, newt)) {
                shouldDeleteColStats = true;
            }
            getTempTablesForDatabase(dbname, tbl_name).put(tbl_name, newTable);
        }

        if (shouldDeleteColStats) {
            try {
                deleteTempTableColumnStatsForTable(dbname, tbl_name);
            } catch (NoSuchObjectException err) {
                // No stats to delete, forgivable error.
                LOG.info("Object not found in metastore", err);
            }
        }
    }

    private void dropTempTable(Table table, boolean deleteData,
                               EnvironmentContext envContext) throws MetaException, TException,
            NoSuchObjectException, UnsupportedOperationException {

        String dbName = table.getDbName().toLowerCase();
        String tableName = table.getTableName().toLowerCase();

        // Determine the temp table path
        Path tablePath = isExistsTempLocation(table, getWh(), getConf());

        // Remove table entry from SessionState
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName, tableName);
        if (tables == null || tables.remove(tableName) == null) {
            throw new MetaException("Could not find temp table entry for " + dbName + "." + tableName);
        }

        //delete dir
        dropTempDir(table, tablePath, getWh(), deleteData, envContext, getEnableFsOperation());
    }

    public Path isExistsTempLocation(Table table, Warehouse wh, Configuration conf) throws MetaException {
        Path tablePath = null;
        String pathStr = table.getSd().getLocation();
        if (pathStr != null) {
            try {
                tablePath = new Path(table.getSd().getLocation());
                if (!wh.isWritable(tablePath.getParent())) {
                    throw new MetaException("Table metadata not deleted since " + tablePath.getParent() +
                            " is not writable by " + SecurityUtils.getUser());
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

    private Table deepCopyAndLowerCaseTable(
            Table tbl) {
        Table newCopy = SessionClientUtils.deepCopy(tbl);
        newCopy.setDbName(newCopy.getDbName().toLowerCase());
        newCopy.setTableName(newCopy.getTableName().toLowerCase());
        return newCopy;
    }

    private Map<String, ColumnStatisticsObj> getTempTableColumnStatsForTable(String dbName,
                                                                             String tableName) {
        SessionState ss = SessionState.get();
        if (ss == null) {
            LOG.debug("No current SessionState, skipping temp tables");
            return null;
        }
        String lookupName = StatsUtils.getFullyQualifiedTableName(dbName.toLowerCase(),
                tableName.toLowerCase());
        return ss.getTempTableColStats().get(lookupName);
    }

    private List<ColumnStatisticsObj> getTempTableColumnStats(String dbName, String tableName,
                                                              List<String> colNames) {
        Map<String, ColumnStatisticsObj> tableColStats =
                getTempTableColumnStatsForTable(dbName, tableName);
        List<ColumnStatisticsObj> retval = new ArrayList<ColumnStatisticsObj>();

        if (tableColStats != null) {
            for (String colName : colNames) {
                colName = colName.toLowerCase();
                if (tableColStats.containsKey(colName)) {
                    retval.add(new ColumnStatisticsObj(tableColStats.get(colName)));
                }
            }
        }
        return retval;
    }

    private boolean updateTempTableColumnStats(String dbName, String tableName,
                                               ColumnStatistics colStats) throws MetaException {
        SessionState ss = SessionState.get();
        if (ss == null) {
            throw new MetaException("No current SessionState, cannot update temporary table stats for "
                    + dbName + "." + tableName);
        }
        Map<String, ColumnStatisticsObj> ssTableColStats =
                getTempTableColumnStatsForTable(dbName, tableName);
        if (ssTableColStats == null) {
            // Add new entry for this table
            ssTableColStats = new HashMap<String, ColumnStatisticsObj>();
            ss.getTempTableColStats().put(
                    StatsUtils.getFullyQualifiedTableName(dbName, tableName),
                    ssTableColStats);
        }
        mergeColumnStats(ssTableColStats, colStats);
        return true;
    }

    private boolean deleteTempTableColumnStatsForTable(String dbName, String tableName)
            throws NoSuchObjectException {
        Map<String, ColumnStatisticsObj> deletedEntry =
                getTempTableColumnStatsForTable(dbName, tableName);
        if (deletedEntry != null) {
            SessionState.get().getTempTableColStats().remove(
                    StatsUtils.getFullyQualifiedTableName(dbName, tableName));
        } else {
            throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
                    " temp table=" + tableName);
        }
        return true;
    }

    private boolean deleteTempTableColumnStats(String dbName, String tableName, String columnName)
            throws NoSuchObjectException {
        ColumnStatisticsObj deletedEntry = null;
        Map<String, ColumnStatisticsObj> ssTableColStats =
                getTempTableColumnStatsForTable(dbName, tableName);
        if (ssTableColStats != null) {
            deletedEntry = ssTableColStats.remove(columnName.toLowerCase());
        }
        if (deletedEntry == null) {
            throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
                    " temp table=" + tableName);
        }
        return true;
    }
}