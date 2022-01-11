package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStoreConfig;
import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TableCacheTest extends BaseTest {
    private static final String TEST_DB = TestUtil.TEST_DB;
    private static final String catalogId = "";
    private static Warehouse wh;
    private static Database testDb;
    private static FileSystem fs;
    private static ProxyMetaStoreClient client;
    private static boolean tbCacheEnable;
    private static CacheDataLakeMetaStore cacheDataLakeMetaStore;
    private static int tbCacheSize = 150;
    private static int tbCacheTTL = 1;
    private static int tbCacheSizeConf;
    private static int tbCacheTTLConf;
    private static IDataLakeMetaStore dataLakeMetaStore;

    @BeforeClass
    public static void setUp() throws TException, IOException {
        HiveConf conf = new HiveConf();
        client = new ProxyMetaStoreClient(conf);

        wh = new Warehouse(conf);
        fs = FileSystem.get(new Configuration());

        testDb = TestUtil.getDatabase(TEST_DB);
        try {
            client.dropDatabase(testDb.getName(), true, true, true);
        } catch (NoSuchObjectException e) {
        }
        client.createDatabase(testDb);

        tbCacheEnable = Boolean.valueOf(conf.get(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_ENABLE, "false"));
        //System.out.println("tbCacheEnable:" + tbCacheEnable);

        dataLakeMetaStore = ((DlfSessionMetaStoreClient) (client.getDlfSessionMetaStoreClient())).getClientDelegate().getDataLakeMetaStore();


        tbCacheSizeConf = Integer.valueOf(conf.get(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_SIZE, "0"));
        //System.out.println("dbCacheSizeConf:" + tbCacheSizeConf);

        tbCacheTTLConf = Integer.valueOf(conf.get(CacheDataLakeMetaStoreConfig.DATA_LAKE_TB_CACHE_TTL_MINS, "0"));
        //System.out.println("tbCacheTTLConf:" + tbCacheTTLConf);
    }

    @AfterClass
    public static void cleanUp() throws TException, IOException {
        fs.close();
        try {
            client.dropDatabase(testDb.getName(), true, true, true);
        } catch (NoSuchObjectException e) {

        }
    }

    @After
    public void cleanUpCase() throws TException {
        List<String> allTables = client.getTables(TEST_DB, "*");
        allTables.forEach(t -> {
            try {
                client.dropTable(TEST_DB, t, true, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testTbCacheEnable() {
        if (tbCacheEnable) {
            Assert.assertTrue(dataLakeMetaStore instanceof CacheDataLakeMetaStore);
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            Assert.assertEquals(tbCacheSize, tbCacheSizeConf);
            Assert.assertEquals(tbCacheTTL, tbCacheTTLConf);
            Assert.assertTrue(cacheDataLakeMetaStore.getTableCache() != null);
        }
    }

    @Test
    public void testCreateTableCache() throws Exception {
        if (tbCacheEnable) {
            String tblName = TestUtil.TEST_TABLE;
            createDefaultTable(tblName);

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Table table = client.getTable(TEST_DB, tblName);
            //控制台输出not hit；then init
            //System.out.println("size:" + cacheDataLakeMetaStore.getTableCache().size());
            Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().size() >= 1);

            CacheDataLakeMetaStore.TableIdentifier tbIdentifier = new CacheDataLakeMetaStore.TableIdentifier(catalogId, TEST_DB, tblName);
            //System.out.println("table:" + cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getTableName());
            Assert.assertEquals(tbIdentifier.getTableName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getTableName());
            Assert.assertEquals(tbIdentifier.getDbName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getDatabaseName());

            //hit
            Table table2 = client.getTable(TEST_DB, tblName);
            //控制台输出hit

            //drop
            client.dropTable(TEST_DB, tblName);

            //invalidate
            //System.out.println("finalSize:" + cacheDataLakeMetaStore.getTableCache().size());
            Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);

        }
    }

    @Test
    public void testCreateTableCacheTTL() throws Exception {
        if (tbCacheEnable) {
            String tblName = TestUtil.TEST_TABLE;
            createDefaultTable(tblName);

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Table table = client.getTable(TEST_DB, tblName);
            //控制台输出not hit；then init
            //System.out.println("size:" + cacheDataLakeMetaStore.getTableCache().size());
            Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().size() >= 1);

            CacheDataLakeMetaStore.TableIdentifier tbIdentifier = new CacheDataLakeMetaStore.TableIdentifier(catalogId, TEST_DB, tblName);
            //System.out.println("table:" + cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getTableName());
            Assert.assertEquals(table.getTableName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getTableName());
            Assert.assertEquals(table.getDbName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getDatabaseName());

            //hit
            Table table2 = client.getTable(TEST_DB, tblName);

            //sleep 1/2 TTL ,cache still validate
            Thread.sleep((tbCacheTTLConf / 2) * 60 * 1000);
            Assert.assertTrue("now sleeped 1/2TTL,cache now is valided", cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) != null);
            Table table3 = client.getTable(TEST_DB, tblName);

            // sleep 1 ttl，cache now invalidate
            Thread.sleep((tbCacheTTLConf) * 60 * 1000);
            Assert.assertTrue("now sleeped 1TTL,cache now is invalided", cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);
            Table table4 = client.getTable(TEST_DB, tblName);
            //Table drop
            client.dropTable(TEST_DB, tblName);

            //cache invalidate
            Assert.assertTrue("now sleeped 1TTL,cache now is invalided", cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);

        }
    }

    @Test
    public void testCreateTableCacheSize() throws Exception {
        if (tbCacheEnable) {
            String tblName = TestUtil.TEST_TABLE;
            createDefaultTable(tblName);

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }

            for (int i = 0; i < 100; i++) {
                createDefaultTable(tblName + i);
                //client.createTable(TestUtil.getTable(TEST_DB, tblName+i, columns));
                client.getTable(TEST_DB, tblName + i);
                //System.out.println(tblName + i + "size:" + cacheDataLakeMetaStore.getTableCache().size() + "loadcount:" + cacheDataLakeMetaStore.getTableCache().stats().loadCount() + "hitrate:" + cacheDataLakeMetaStore.getTableCache().stats().hitRate() + "hitcount:" + cacheDataLakeMetaStore.getTableCache().stats().hitCount());

            }
            Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() <= Math.min(tbCacheSize, 100));

            for (int i = 0; i < 100; i++) {
                client.dropTable(TEST_DB, tblName + i);
                //System.out.println(tblName + i + "size:" + cacheDataLakeMetaStore.getTableCache().size() + "loadcount:" + cacheDataLakeMetaStore.getTableCache().stats().loadCount() + "hitrate:" + cacheDataLakeMetaStore.getTableCache().stats().hitRate() + "hitcount:" + cacheDataLakeMetaStore.getTableCache().stats().hitCount());
            }
            Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().size() == 0);
        }
    }

    @Test
    public void testAlterTableWithCache() throws Exception {
        String tblName = TestUtil.TEST_TABLE;
        createDefaultTable(tblName);

        if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
        }

        //now cache has no db
        CacheDataLakeMetaStore.TableIdentifier tbIdentifier = new CacheDataLakeMetaStore.TableIdentifier(catalogId, TEST_DB, tblName);
        Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);

        //not hit/init
        Table table = client.getTable(TEST_DB, tblName);
        Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) != null);

        //alter then invalid
        //new table(name, "new table", "file:/tmp/new_db", null)
        String newTblName = TestUtil.TEST_TABLE + "new";
        Table renamed = getDefaultTable(newTblName);
        renamed.getSd().setLocation(table.getSd().getLocation());
        client.alter_table(TEST_DB, tblName, renamed);
        Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);

        //drop
        client.dropTable(TEST_DB, newTblName);
    }

    //    @Test
//    public void getAllTables() throws TException {
//        String tblName = "test_tbl";
//        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
//        client.createTable(TestUtil.getTable(TEST_DB, tblName, columns));
//        client.createTable(TestUtil.getTable(TEST_DB, tblName+"1", columns));
//        client.createTable(TestUtil.getTable(TEST_DB, tblName+"2", columns));
//        List<String> allTables = client.getTables(TEST_DB, ".*");
//        //System.out.println(allTables);
//    }
    public Table getDefaultTable(String tblName) {
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table0 = TestUtil.getTable(TEST_DB, tblName, columns);
        return table0;
    }

    public void createDefaultTable(String tblName) throws TException {
        client.createTable(getDefaultTable(tblName));
    }
}
