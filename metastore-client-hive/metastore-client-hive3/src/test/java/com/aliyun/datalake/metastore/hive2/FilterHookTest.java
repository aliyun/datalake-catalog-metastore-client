package com.aliyun.datalake.metastore.hive2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FilterHookTest {
    private static final Logger LOG = LoggerFactory.getLogger(FilterHookTest.class);

    public static class DummyMetaStoreFilterHookImpl extends DefaultMetaStoreFilterHookImpl {
        private static boolean blockResults = false;

        public DummyMetaStoreFilterHookImpl(Configuration conf) {
            super(conf);
        }

        @Override
        public List<String> filterDatabases(List<String> dbList) throws MetaException {
            if (blockResults) {
                return new ArrayList<>();
            }
            return super.filterDatabases(dbList);
        }

        @Override
        public Database filterDatabase(Database dataBase) throws NoSuchObjectException {
            if (blockResults) {
                throw new NoSuchObjectException("Blocked access");
            }
            return super.filterDatabase(dataBase);
        }

        @Override
        public List<String> filterTableNames(String catName, String dbName, List<String> tableList)
                throws MetaException {
            if (blockResults) {
                return new ArrayList<>();
            }
            return super.filterTableNames(catName, dbName, tableList);
        }

        @Override
        public Table filterTable(Table table) throws NoSuchObjectException {
            if (blockResults) {
                throw new NoSuchObjectException("Blocked access");
            }
            return super.filterTable(table);
        }

        @Override
        public List<Table> filterTables(List<Table> tableList) throws MetaException {
            if (blockResults) {
                return new ArrayList<>();
            }
            return super.filterTables(tableList);
        }

        @Override
        public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
            if (blockResults) {
                return new ArrayList<>();
            }
            return super.filterPartitions(partitionList);
        }

        @Override
        public List<PartitionSpec> filterPartitionSpecs(
                List<PartitionSpec> partitionSpecList) throws MetaException {
            if (blockResults) {
                return new ArrayList<>();
            }
            return super.filterPartitionSpecs(partitionSpecList);
        }

        @Override
        public Partition filterPartition(Partition partition) throws NoSuchObjectException {
            if (blockResults) {
                throw new NoSuchObjectException("Blocked access");
            }
            return super.filterPartition(partition);
        }

        @Override
        public List<String> filterPartitionNames(String catName, String dbName, String tblName,
                                                 List<String> partitionNames) throws MetaException {
            if (blockResults) {
                return new ArrayList<>();
            }
            return super.filterPartitionNames(catName, dbName, tblName, partitionNames);
        }

    }

    private static final String DBNAME1 = "filterHook_testDB1";
    private static final String DBNAME2 = "filterHook_testDB2";
    private static final String TAB1 = "tab1";
    private static final String TAB2 = "tab2";
    private static IMetaStoreClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        DummyMetaStoreFilterHookImpl.blockResults = false;

        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetastoreConf.ConfVars.FILTER_HOOK.getVarname(),
                DummyMetaStoreFilterHookImpl.class.getName(), MetaStoreFilterHook.class.getName());

        client = new ProxyMetaStoreClient(hiveConf);

        client.dropDatabase(DBNAME1, true, true, true);
        client.dropDatabase(DBNAME2, true, true, true);

        Database db1 = TestUtil.getDatabase(DBNAME1);
        client.createDatabase(db1);
        Database db2 = TestUtil.getDatabase(DBNAME2);
        client.createDatabase(db2);

        Table tab1 = TestUtil.getTable(DBNAME1, TAB1, ImmutableMap.of("id", "int", "name", "string"));
        client.createTable(tab1);
        Table tab2 = TestUtil.getPartitionTable(DBNAME1, TAB2, ImmutableMap.of("id", "int"), ImmutableMap.of("name", "string"));
        client.createTable(tab2);

        Partition p1 = TestUtil.getPartition(DBNAME1, TAB2, ImmutableMap.of("id", "int"),
                ImmutableMap.of("name", "string"), Lists.newArrayList("value1"));
        Partition p2 = TestUtil.getPartition(DBNAME1, TAB2, ImmutableMap.of("id", "int"),
                ImmutableMap.of("name", "string"), Lists.newArrayList("value2"));
        client.add_partition(p1);
        client.add_partition(p2);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void testDefaultFilter() throws Exception {
        assertNotNull(client.getTable(DBNAME1, TAB1));
        assertEquals(2, client.getTables(DBNAME1, "*").size());
        assertEquals(2, client.getAllTables(DBNAME1).size());
        assertEquals(1, client.getTables(DBNAME1, TAB2).size());
        assertEquals(0, client.getAllTables(DBNAME2).size());

        assertNotNull(client.getDatabase(DBNAME1));
        assertEquals(1, client.getDatabases(DBNAME1).size());

        assertNotNull(client.getPartition(DBNAME1, TAB2, "name=value1"));
        assertEquals(1, client.getPartitionsByNames(DBNAME1, TAB2, Lists.newArrayList("name=value1")).size());
    }

    @Test
    public void testDummyFilterForTables() throws Exception {
        DummyMetaStoreFilterHookImpl.blockResults = true;
        try {
            client.getTable(DBNAME1, TAB1);
            fail("getTable() should fail with blocking mode");
        } catch (NoSuchObjectException e) {
            // Excepted
        }
        assertEquals(0, client.getTables(DBNAME1, "*").size());
        assertEquals(0, client.getAllTables(DBNAME1).size());
        assertEquals(0, client.getTables(DBNAME1, TAB2).size());
    }

    @Test
    public void testDummyFilterForDb() throws Exception {
        DummyMetaStoreFilterHookImpl.blockResults = true;
        try {
            assertNotNull(client.getDatabase(DBNAME1));
            fail("getDatabase() should fail with blocking mode");
        } catch (NoSuchObjectException e) {
            // Excepted
        }
        assertEquals(0, client.getDatabases("*").size());
        assertEquals(0, client.getAllDatabases().size());
        assertEquals(0, client.getDatabases(DBNAME1).size());
    }

    @Test
    public void testDummyFilterForPartition() throws Exception {
        DummyMetaStoreFilterHookImpl.blockResults = true;
        try {
            assertNotNull(client.getPartition(DBNAME1, TAB2, "name=value1"));
            fail("getPartition() should fail with blocking mode");
        } catch (NoSuchObjectException e) {
            // Excepted
        }
        assertEquals(0, client.getPartitionsByNames(DBNAME1, TAB2,
                Lists.newArrayList("name=value1")).size());
    }

}