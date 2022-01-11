package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStoreConfig;
import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class DatabaseCacheTest {
    private static final String catalogId = "";
    private static final String TEST_DB = TestUtil.TEST_DB;
    private static ProxyMetaStoreClient client;
    private static boolean dbCacheEnable;
    private static CacheDataLakeMetaStore cacheDataLakeMetaStore;
    private static int dbCacheSize = 100;
    private static int dbCacheTTL = 1;
    private static int dbCacheSizeConf;
    private static int dbCacheTTLConf;
    private static IDataLakeMetaStore dataLakeMetaStore;

    @BeforeClass
    public static void setUp() throws TException, IOException {
        HiveConf conf = new HiveConf();
        client = new ProxyMetaStoreClient(conf);

        dbCacheEnable = Boolean.valueOf(conf.get(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_ENABLE, "false"));
        //System.out.println("dbCacheEnable:" + dbCacheEnable);

        dataLakeMetaStore = ((DlfSessionMetaStoreClient) (client.getDlfSessionMetaStoreClient())).getClientDelegate().getDataLakeMetaStore();


        dbCacheSizeConf = Integer.valueOf(conf.get(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_SIZE, "0"));
        //System.out.println("dbCacheSizeConf:" + dbCacheSizeConf);

        dbCacheTTLConf = Integer.valueOf(conf.get(CacheDataLakeMetaStoreConfig.DATA_LAKE_DB_CACHE_TTL_MINS, "0"));
        //System.out.println("dbCacheTTLConf:" + dbCacheTTLConf);
    }

    @AfterClass
    public static void cleanUp() throws TException {
        try {
            client.dropDatabase(TEST_DB, true, true, true);
        } catch (NoSuchObjectException e) {

        }
    }

    @Test
    public void testCacheEnable() {
        if (dbCacheEnable) {
            Assert.assertTrue(dataLakeMetaStore instanceof CacheDataLakeMetaStore);
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            Assert.assertEquals(dbCacheSize, dbCacheSizeConf);
            Assert.assertEquals(dbCacheTTL, dbCacheTTLConf);
            Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache() != null);
        }
    }

    @Test
    public void testCreateDatabaseCache() throws Exception {
        if (dbCacheEnable) {
            String name = TEST_DB;
            String d = "Default Hive database";
            String path = TestUtil.WAREHOUSE_PATH + name;
            client.dropDatabase(name, true, true, true);
            client.createDatabase(new Database(name, d, path, null));

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Database database = client.getDatabase(name);
            //控制台输出not hit；then init
            //System.out.println("size:" + cacheDataLakeMetaStore.getDatabaseCache().size());
            Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() >= 1);

            CacheDataLakeMetaStore.DbIdentifier dbIdentifier = new CacheDataLakeMetaStore.DbIdentifier(catalogId, name);
            //System.out.println("database:" + cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getName());
            Assert.assertEquals(database.getName(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getName());
            Assert.assertEquals(database.getLocationUri(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getLocationUri());

            //hit
            Database database2 = client.getDatabase(name);
            //控制台输出hit
            String tblName = TestUtil.TEST_TABLE;
            Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
            Table table = TestUtil.getTable(name, tblName, columns);

            // create a table
            client.createTable(table);
            client.getTable(name, tblName);
            if (cacheDataLakeMetaStore.getTableCache() != null) {
                Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().size() == 1);
            }

            //drop
            client.dropDatabase(name, true, true, true);

            //invalidate
            //System.out.println("finalSize:" + cacheDataLakeMetaStore.getDatabaseCache().size());
            Assert.assertTrue(database.getName(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null);
            if (cacheDataLakeMetaStore.getTableCache() != null) {
                Assert.assertTrue(cacheDataLakeMetaStore.getTableCache().size() == 0);
            }
        }
    }

    @Test
    public void testCreateDatabaseCacheTTL() throws Exception {
        if (dbCacheEnable) {
            String name = TEST_DB;
            String d = "Default Hive database";
            String path = TestUtil.WAREHOUSE_PATH + name;
            client.dropDatabase(name, true, true, true);
            client.createDatabase(new Database(name, d, path, null));

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Database database = client.getDatabase(name);
            //控制台输出not hit；then init
            //System.out.println("size:" + cacheDataLakeMetaStore.getDatabaseCache().size());
            Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() >= 1);

            CacheDataLakeMetaStore.DbIdentifier dbIdentifier = new CacheDataLakeMetaStore.DbIdentifier(catalogId, name);
            //System.out.println("database:" + cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getName());
            Assert.assertEquals(database.getName(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getName());
            Assert.assertEquals(database.getLocationUri(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getLocationUri());

            //hit
            Database database2 = client.getDatabase(name);

            //sleep 1/2 TTL ,cache still validate
            Thread.sleep((dbCacheTTLConf / 2) * 60 * 1000);
            Assert.assertTrue("now sleeped 1/2TTL,cache now is valided", cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) != null);
            Database database3 = client.getDatabase(name);

            // sleep 1 ttl，cache now invalidate
            Thread.sleep((dbCacheTTLConf) * 60 * 1000);
            Assert.assertTrue("now sleeped 1TTL,cache now is invalided", cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null);
            Database database4 = client.getDatabase(name);
            //database drop
            client.dropDatabase(name);

            //cache invalidate
            Assert.assertTrue("now sleeped 1TTL,cache now is invalided", cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null);

        }
    }

    @Test
    public void testCreateDatabaseCacheSize() throws Exception {
        if (dbCacheEnable) {
            String name = TEST_DB;
            String d = "Default Hive database";
            String path = TestUtil.WAREHOUSE_PATH;
            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            for (int i = 0; i < 100; i++) {
                client.dropDatabase(name + i, true, true, true);
                //System.out.println(name + i + "size:" + cacheDataLakeMetaStore.getDatabaseCache().size() + "loadcount:" + cacheDataLakeMetaStore.getDatabaseCache().stats().loadCount() + "hitrate:" + cacheDataLakeMetaStore.getDatabaseCache().stats().hitRate() + "hitcount:" + cacheDataLakeMetaStore.getDatabaseCache().stats().hitCount());
            }
            for (int i = 0; i < 100; i++) {
                client.createDatabase(new Database(name + i, d, path + name + i, null));
                client.getDatabase(name + i);
            }
            Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() <= Math.min(dbCacheSize, 100));
            for (int i = 0; i < 100; i++) {
                client.dropDatabase(name + i, true, true, true);
                //System.out.println(name + i + "size:" + cacheDataLakeMetaStore.getDatabaseCache().size() + "loadcount:" + cacheDataLakeMetaStore.getDatabaseCache().stats().loadCount() + "hitrate:" + cacheDataLakeMetaStore.getDatabaseCache().stats().hitRate() + "hitcount:" + cacheDataLakeMetaStore.getDatabaseCache().stats().hitCount());
            }
            //存在default database的缓存
            Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() <= 1);
        }
    }

    @Test
    public void testAlterDatabaseWithCache() throws Exception {
        String name = TEST_DB;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + name;
        if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
        }
        client.dropDatabase(name, true, true, true);
        client.createDatabase(new Database(name, d, path, null));

        //now cache has no db
        CacheDataLakeMetaStore.DbIdentifier dbIdentifier = new CacheDataLakeMetaStore.DbIdentifier(catalogId, name);
        Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null);

        //not hit/init
        Database database = client.getDatabase(name);
        Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) != null);

        //alter then invalid
        client.alterDatabase(name, new Database(name, "new database", TestUtil.WAREHOUSE_PATH + "new_db", null));
        Assert.assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null);

        //alter partition can't delete database

        //drop
        client.dropDatabase(name, true, true, true);
        //now alter db location which don't create new dir and move data;so we should delete by ourself
        Files.delete(Paths.get(path.substring(5)));
    }


}
