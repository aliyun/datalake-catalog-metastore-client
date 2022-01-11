package com.aliyun.datalake.metastore.hive2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableMetaTest {
    private static final String NAME_PREFIX = "metastore_ut_";
    private static final String DB_NAME = NAME_PREFIX + "testpartdb";
    private static final String TABLE_NAME = NAME_PREFIX + "testparttable";
    private static HiveConf conf;
    private IMetaStoreClient client;
    private List<String> dbNames = new ArrayList<>();
    private List<TableMeta> expectedMetas = null;

    @Before
    public void setUp() throws Exception {
        // Get new client
        conf = new HiveConf();
        client = new ProxyMetaStoreClient(conf);
        dbNames.clear();
        // Clean up
        client.dropDatabase(DB_NAME + "_one", true, true, true);
        client.dropDatabase(DB_NAME + "_two", true, true, true);

        //Create test dbs and tables
        expectedMetas = new ArrayList<>();
        String dbName = DB_NAME + "_one";
        dbNames.add(dbName);
        createDB(dbName);
        expectedMetas.add(createTestTable(dbName, TABLE_NAME + "_one", TableType.EXTERNAL_TABLE));
        expectedMetas.add(createTestTable(dbName, TABLE_NAME + "", TableType.MANAGED_TABLE, "cmT"));
        expectedMetas.add(createTestTable(dbName, "v" + TABLE_NAME, TableType.VIRTUAL_VIEW));

        dbName = DB_NAME + "_two";
        dbNames.add(dbName);
        createDB(dbName);
        expectedMetas.add(createTestTable(dbName, TABLE_NAME + "_one", TableType.MANAGED_TABLE));
        expectedMetas.add(createTestTable(dbName, "v" + TABLE_NAME, TableType.MATERIALIZED_VIEW, ""));
    }

    @After
    public void tearDown() throws Exception {
        try {
            for (String dbName : dbNames) {
                List<String> allTables = client.getTables(dbName, "*");
                allTables.forEach(t -> {
                    try {
                        client.dropTable(dbName, t, true, true);
                    } catch (UnsupportedOperationException e) {
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                client.dropDatabase(dbName,
                        true,   // Delete data.
                        true,   // Ignore unknownDB.
                        true    // Cascade.
                );
            }
            if (client != null) {
                client.close();
            }
        } finally {
            client = null;
        }
    }


    private void createDB(String dbName) throws TException {
        Database testDb = TestUtil.getDatabase(dbName);
        client.createDatabase(testDb);
    }


    private Table createTable(String dbName, String tableName, TableType type)
            throws Exception {
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(dbName, tableName, type, columns);

        if (type == TableType.MATERIALIZED_VIEW) {
            CreationMetadata cm = new CreationMetadata("", dbName, tableName, ImmutableSet.of());
            table.setCreationMetadata(cm);
        }

        if (type == TableType.EXTERNAL_TABLE) {
            table.getParameters().put("EXTERNAL", "true");
        }

        return table;
    }

    private TableMeta createTestTable(String dbName, String tableName, TableType type, String comment)
            throws Exception {
        Table table = createTable(dbName, tableName, type);
        table.getParameters().put("comment", comment);
        client.createTable(table);
        TableMeta tableMeta = new TableMeta(dbName, tableName, type.name());
        tableMeta.setComments(comment);
        return tableMeta;
    }

    private TableMeta createTestTable(String dbName, String tableName, TableType type)
            throws Exception {
        Table table = createTable(dbName, tableName, type);
        client.createTable(table);
        return new TableMeta(dbName, tableName, type.name());
    }

    private void assertTableMetas(int[] expected, List<TableMeta> actualTableMetas) {
        assertTableMetas(expectedMetas, actualTableMetas, expected);
    }

    private void assertTableMetas(List<TableMeta> actual, int... expected) {
        assertTableMetas(expectedMetas, actual, expected);
    }

    private void assertTableMetas(List<TableMeta> fullExpected, List<TableMeta> actual, int... expected) {
        assertEquals("Expected " + expected.length + " but have " + actual.size() +
                " tableMeta(s)", expected.length, actual.size());

        Set<TableMeta> metas = new HashSet<>(actual);
        for (int i : expected) {
            assertTrue("Missing " + fullExpected.get(i), metas.remove(fullExpected.get(i)));
        }

        assertTrue("Unexpected tableMeta(s): " + metas, metas.isEmpty());

    }

    public List<TableMeta> filterTableMeta(List<TableMeta> tableMetas) {
        return tableMetas.stream().filter(t -> dbNames.contains(t.getDbName())).collect(Collectors.toList());
    }

    /**
     * Testing getTableMeta(String,String,List(String)) ->
     * get_table_meta(String,String,List(String)).
     */
    @Test
    public void testGetTableMeta() throws Exception {
        List<TableMeta> tableMetas = client.getTableMeta("asdf", "qwerty", Lists.newArrayList("zxcv"));
        assertTableMetas(new int[]{}, tableMetas);

        tableMetas = client.getTableMeta(DB_NAME + "_two", "v" + TABLE_NAME, Lists.newArrayList());
        assertTableMetas(new int[]{4}, tableMetas);

        tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("***", "**", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*one", "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*one*", "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta(DB_NAME + "_two", "*", Lists.newArrayList());
        assertTableMetas(new int[]{3, 4}, tableMetas);

        tableMetas = client.getTableMeta(DB_NAME + "_two*", "*", Lists.newArrayList());
        assertTableMetas(new int[]{3, 4}, tableMetas);

        tableMetas = client.getTableMeta(DB_NAME + "*", "*", Lists.newArrayList(
                TableType.EXTERNAL_TABLE.name()));
        assertTableMetas(new int[]{0}, tableMetas);

        tableMetas = client.getTableMeta(DB_NAME + "*", "*", Lists.newArrayList(
                TableType.EXTERNAL_TABLE.name(), TableType.MATERIALIZED_VIEW.name()));
        assertTableMetas(new int[]{0, 4}, tableMetas);

        tableMetas = client.getTableMeta("*one", "*", Lists.newArrayList("*TABLE"));
        assertTableMetas(new int[]{}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*one", "*", Lists.newArrayList("*"));
        assertTableMetas(new int[]{}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*one|*.Wo", "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));
        tableMetas = client.getTableMeta("*one|*ssXXXdd.wo", "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2}, filterTableMeta(tableMetas));
        tableMetas = client.getTableMeta("*one|*ssXXXdd.wo", "*_one|xxx", Lists.newArrayList());
        assertTableMetas(new int[]{0}, filterTableMeta(tableMetas));
        tableMetas = client.getTableMeta("*one|*ssXXXdd.wo", "*_one|v*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 2}, filterTableMeta(tableMetas));
        tableMetas = client.getTableMeta("*one|*.wo", "*_one|v*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 2, 3, 4}, filterTableMeta(tableMetas));
    }

    @Test
    public void testGetTableMetaCaseSensitive() throws Exception {
        List<TableMeta> tableMetas = client.getTableMeta("*tWo", NAME_PREFIX + "tEsT*", Lists.newArrayList());
        assertTableMetas(new int[]{3}, tableMetas);

        tableMetas = client.getTableMeta("*", "*", Lists.newArrayList("mAnAGeD_tABlE"));
        assertTableMetas(new int[]{}, tableMetas);
    }

    @Ignore
    @Test
    public void testGetTableMetaNullOrEmptyDb() throws Exception {
        List<TableMeta> tableMetas = client.getTableMeta(null, "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("", "*", Lists.newArrayList());
        assertTableMetas(new int[]{}, filterTableMeta(tableMetas));
    }

    @Ignore
    @Test
    public void testGetTableMetaNullOrEmptyTbl() throws Exception {
        List<TableMeta> tableMetas = client.getTableMeta("*", null, Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*", "", Lists.newArrayList());
        assertTableMetas(new int[]{}, filterTableMeta(tableMetas));
    }

    @Test
    public void testGetTableMetaNullOrEmptyTypes() throws Exception {
        List<TableMeta> tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*", "*", Lists.newArrayList(""));
        assertTableMetas(new int[]{}, filterTableMeta(tableMetas));

        tableMetas = client.getTableMeta("*", "*", null);
        assertTableMetas(new int[]{0, 1, 2, 3, 4}, filterTableMeta(tableMetas));
    }

    @Test
    public void testGetTableMetaNullNoDbNoTbl() throws Exception {
        client.dropDatabase(DB_NAME + "_one", true, true, true);
        client.dropDatabase(DB_NAME + "_two", true, true, true);
        List<TableMeta> tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
        assertTableMetas(new int[]{}, filterTableMeta(tableMetas));
    }

//    @Test
//    public void tablesInDifferentCatalog() throws TException {
//        String catName = "get_table_meta_catalog";
//        Catalog cat = new CatalogBuilder()
//                .setName(catName)
//                .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
//                .build();
//        client.createCatalog(cat);
//
//        String dbName = "db9";
//        // For this one don't specify a location to make sure it gets put in the catalog directory
//        Database db = new DatabaseBuilder()
//                .setName(dbName)
//                .setCatalogName(catName)
//                .create(client, metaStore.getConf());
//
//        String[] tableNames = {"table_in_other_catalog_1", "table_in_other_catalog_2", "random_name"};
//        List<TableMeta> expected = new ArrayList<>(tableNames.length);
//        for (int i = 0; i < tableNames.length; i++) {
//            client.createTable(new TableBuilder()
//                    .inDb(db)
//                    .setTableName(tableNames[i])
//                    .addCol("id", "int")
//                    .addCol("name", "string")
//                    .build(metaStore.getConf()));
//            expected.add(new TableMeta(dbName, tableNames[i], TableType.MANAGED_TABLE.name()));
//        }
//
//        List<String> types = Collections.singletonList(TableType.MANAGED_TABLE.name());
//        List<TableMeta> actual = client.getTableMeta(dbName, "*", types);
//        assertTableMetas(expected, actual, 0, 1, 2);
//
//        actual = client.getTableMeta("*", "table_*", types);
//        assertTableMetas(expected, actual, 0, 1);
//
//        actual = client.getTableMeta(dbName, "table_in_other_catalog_*", types);
//        assertTableMetas(expected, actual);
//    }

//    @Test
//    public void noSuchCatalog() throws TException {
//        List<TableMeta> tableMetas = client.getTableMeta("*", "*", Lists.newArrayList());
//        Assert.assertEquals(0, tableMetas.size());
//    }
//
//    @Test
//    public void catalogPatternsDontWork() throws TException {
//List<TableMeta> tableMetas = client.getTableMeta("h*", "*", "*", Lists.newArrayList());
//    Assert.assertEquals(0, tableMetas.size());
//    }

}
