package com.aliyun.datalake.metastore.hive2;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatabaseTest extends BaseTest {
    private static final String TEST_DB1_NAME = TestUtil.TEST_DB + "1";
    private static final String TEST_DB2_NAME = TestUtil.TEST_DB + "2";
    private static final String TEST_DB3_NAME = TestUtil.TEST_DB + "3";
    private static IMetaStoreClient client;
    private static FileSystem fs;
    private static Warehouse warehouse;
    private static HiveConf hiveConf;

    @BeforeClass
    public static void setUp() throws TException, IOException {
        hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.metrics.enabled", "true");
        hiveConf.set("hive.key1", "value1");
        hiveConf.set("hive.key2", "http://www.example.com");
        hiveConf.set("hive.key3", "");
        hiveConf.set("hive.key4", "0");
        client = new ProxyMetaStoreClient(hiveConf);
        warehouse = new Warehouse(hiveConf);
        fs = FileSystem.get(new Configuration());
    }

    @AfterClass
    public static void cleanUp() throws TException {
        try {
            client.dropDatabase(TEST_DB1_NAME, true, true, true);
            client.dropDatabase(TEST_DB2_NAME, true, true, true);
            client.dropDatabase(TEST_DB3_NAME, true, true, true);
            client.dropDatabase("db-2", true, true, true);
        } catch (NoSuchObjectException e) {

        }
    }

    private static void silentDropDatabase(String dbName) throws MetaException, TException {
        try {
            for (String tableName : client.getTables(dbName, "*")) {
                client.dropTable(dbName, tableName);
            }
            client.dropDatabase(dbName);
        } catch (NoSuchObjectException e) {
        } catch (InvalidOperationException e) {
        }
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String name = TEST_DB1_NAME;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + name;
        client.dropDatabase(name, true, true, true);
        client.createDatabase(new Database(name, d, path, null));

        // validate path is correct
        ValidatorUtils.validateExist(fs, new Path(path), true);
        Assert.assertEquals(path.replace("//", ""), client.getDatabase(name).getLocationUri());

        // create database which already exists
        TestUtil.run(client, client -> client.createDatabase(new Database(name, d, path, null)),
                new AlreadyExistsException("already exists"));

        // drop database
        client.dropDatabase(name, true, true, true);

        // check fs is not exist
        ValidatorUtils.validateExist(fs, new Path(path), false);

//        # TODO bug, dlf allow create dbname with hyphen(-)
//        TestUtil.run(client, client -> {
//            client.dropDatabase("db-2", true, true, true);
//            client.createDatabase(new Database("db-2", d, path, null));
//        }, new InvalidObjectException("not a valid database name"));
//
//        client.dropDatabase("db-2", true, true, true);
    }

    @Test
    public void testAlterDatabase() throws Exception {
        String name = TEST_DB1_NAME;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + name;
        String newPath = TestUtil.WAREHOUSE_PATH + "new_db";
        String newDescription = "new database";

        client.dropDatabase(name, true, true, true);

        client.createDatabase(new Database(name, d, path, null));
        ValidatorUtils.validateExist(fs, new Path(path), true);

        Database newDatabase = new Database(name, newDescription, newPath, null);
        client.alterDatabase(name, newDatabase);

        ValidatorUtils.validateExist(fs, new Path(path), true);
        ValidatorUtils.validateExist(fs, new Path(newDatabase.getLocationUri()), false);
        ValidatorUtils.validateDatabases(newDatabase, client.getDatabase(name));

        try {
            client.alterDatabase("default", TestUtil.buildDatabase(name));
            Assert.assertTrue("Rename database not allowed", false);
        } catch (InvalidOperationException e) {
            Assert.assertTrue(e.getMessage().contains("Rename database is unsupported"));
        }

        try {
            client.alterDatabase("db_not_exist", TestUtil.buildDatabase("db_not_exist"));
            Assert.assertTrue("Alter non-existing database should failed", false);
        } catch (NoSuchObjectException e) {
            // pass
        }

        //alter location will only changes the default parent-directory where new tables will be added
        client.dropDatabase(name, true, true, true);

        // delete the directory that because database location changed
        ValidatorUtils.validateExist(fs, new Path(path), true);
        fs.delete(new Path(path), true);
    }

    @Test
    public void testDropDatabase() throws Exception {
        String name1 = TEST_DB1_NAME;
        String name2 = TEST_DB2_NAME;
        String name3 = TEST_DB3_NAME;
        String d = "Default Hive database";
        String path1 = TestUtil.WAREHOUSE_PATH + name1 + ".db";
        String path2 = TestUtil.WAREHOUSE_PATH + name2 + ".db";
        String path3 = TestUtil.WAREHOUSE_PATH + name3 + ".db";

        // drop and recreate database name1
        client.dropDatabase(name1, true, true, true);
        ValidatorUtils.validateExist(fs, new Path(path1), false);

        client.createDatabase(new Database(name1, d, path1, null));
        ValidatorUtils.validateExist(fs, new Path(path1), true);

        client.dropDatabase(name1, true, true);
        ValidatorUtils.validateExist(fs, new Path(path1), false, "Database data should be deleted");

        // create database name2
        client.dropDatabase(name2, true, true, true);
        client.createDatabase(new Database(name2, d, path2, null));
        ValidatorUtils.validateExist(fs, new Path(path2), true);

        // drop database, don't delete data, don't use cascade
        client.dropDatabase(name2, false, true);
        ValidatorUtils.validateExist(fs, new Path(path2), true, "Database data should not be deleted");
        fs.delete(new Path(path2), true);

        // ignoreUnknownDb
        client.dropDatabase("db_not_exist", true, true);
        try {
            client.dropDatabase("db_not_exist", true, false);
            Assert.assertTrue("Drop non-existing database should failed", false);
        } catch (NoSuchObjectException e) {
            // pass
        }

        // cascade
        client.dropDatabase(name3, true, true, true);
        client.createDatabase(new Database(name3, d, path3, null));
        client.createTable(TestUtil.buildTable(name3, TestUtil.TEST_TABLE));

        // create external table
        String eTableName = TestUtil.TEST_TABLE + "_1";
        String eTableName2 = TestUtil.TEST_TABLE + "_2";
        String eTableName3 = TestUtil.TEST_TABLE + "_3";
        client.createTable(TestUtil.buildTable(name3, eTableName, TableType.EXTERNAL_TABLE.name()));

        // create external table with location which not subdirectory of the database
        Table table2 = TestUtil.buildTable(name3, eTableName2, TableType.EXTERNAL_TABLE.name());
        table2.getSd().setLocation(table2.getSd().getLocation().replace(name3, name2));
        client.createTable(table2);

        Table table3 = TestUtil.buildTable(name3, eTableName3, TableType.MATERIALIZED_VIEW.name());
        table3.getSd().setLocation(table3.getSd().getLocation().replace(name3, name2));
        client.createTable(table3);

        Table eTable = client.getTable(name3, eTableName);
        ValidatorUtils.validateExist(fs, new Path(eTable.getSd().getLocation()), true);

        Table eTable2 = client.getTable(name3, eTableName2);
        ValidatorUtils.validateExist(fs, new Path(eTable2.getSd().getLocation()), true);

        Table eTable3 = client.getTable(name3, eTableName3);
        ValidatorUtils.validateExist(fs, new Path(eTable3.getSd().getLocation()), true);

        try {
            client.dropDatabase(name3, true, true, false);
            Assert.assertTrue("Drop database should failed", false);
        } catch (InvalidOperationException e) {
            // pass
        }

        Table table = client.getTable(name3, TestUtil.TEST_TABLE);
        ValidatorUtils.validateExist(fs, new Path(table.getSd().getLocation()), true);

        // should delete all table data/database data cascade(include managed table/external table)
        client.dropDatabase(name3, true, true, true);
        ValidatorUtils.validateExist(fs, new Path(path3), false);
        ValidatorUtils.validateExist(fs, new Path(table.getSd().getLocation()), false);
        ValidatorUtils.validateExist(fs, new Path(eTable.getSd().getLocation()), false);
        ValidatorUtils.validateExist(fs, new Path(eTable2.getSd().getLocation()), true);
        ValidatorUtils.validateExist(fs, new Path(eTable3.getSd().getLocation()), false);

        fs.delete(new Path(eTable2.getSd().getLocation()), true);
        fs.delete(new Path(eTable3.getSd().getLocation()), true);
    }

    @Test
    public void testGetDatabase() throws Exception {
        // GetDatabase
        Assert.assertEquals("default", client.getDatabase("default").getName());
        try {
            client.getDatabase("db_not_exists");
            Assert.assertTrue("Get non-existing database should failed", false);
        } catch (NoSuchObjectException e) {
            Assert.assertTrue(e.getMessage().contains("Database not found"));
        }

        // GetDatabases
        String db1Name = TEST_DB1_NAME;
        client.dropDatabase(db1Name, true, true, true);
        String db2Name = TEST_DB2_NAME;
        client.dropDatabase(db2Name, true, true, true);
        int size = client.getDatabases("*db*").size();
        int patternAllSize = client.getDatabases(".*.").size();
        int allSize = client.getAllDatabases().size();
        Assert.assertEquals(patternAllSize, allSize);
        // check create/get
        Database createDatabase = TestUtil.buildDatabase(db1Name);
        client.createDatabase(createDatabase);
        Database createdDatabase = client.getDatabase(createDatabase.getName());
        ValidatorUtils.validateDatabases(createDatabase, createdDatabase);
        client.createDatabase(TestUtil.buildDatabase(db2Name));
        Assert.assertEquals(Lists.newArrayList(db1Name, db2Name), client.getDatabases("metastore_ut_test_db."));

        // check getDatabase filter
        int newSize = client.getDatabases("*db*").size();
        // check sub pattern
        Assert.assertEquals(Lists.newArrayList(db1Name, db2Name), client.getDatabases("metastore_ut_test_db1" +
                "|metastore_ut_test.db2"));

        // check Upper case pattern
        Assert.assertEquals(newSize, client.getDatabases("*DB*").size());
        // check other pattern
        Assert.assertEquals(0, client.getDatabases("sdf*0_SAD9.").size());
        // check size
        Assert.assertEquals(size + 2, newSize);

        // check all database
        int allNewSize = client.getAllDatabases().size();
        Assert.assertEquals(allSize + 2, allNewSize);
        client.dropDatabase(db1Name, true, true, true);
        client.dropDatabase(db2Name, true, true, true);

    }

    @Test
    public void testDatabase() throws Throwable {
        try {
            // clear up any existing databases
            silentDropDatabase(TEST_DB1_NAME);
            silentDropDatabase(TEST_DB2_NAME);

            Database db = new Database();
            db.setLocationUri(TestUtil.WAREHOUSE_PATH + TEST_DB1_NAME);
            db.setName(TEST_DB1_NAME);
            db.setOwnerName("root");
            db.setOwnerType(PrincipalType.USER);
            client.createDatabase(db);

            db = client.getDatabase(TEST_DB1_NAME);

            assertEquals("name of returned db is different from that of inserted db",
                    TEST_DB1_NAME, db.getName());
            assertEquals("location of the returned db is different from that of inserted db",
                    TestUtil.WAREHOUSE_PATH.replace("///", "/") + TEST_DB1_NAME, db.getLocationUri());
            assertEquals(db.getOwnerName(), "root");
            assertEquals(db.getOwnerType(), PrincipalType.USER);
            Database db2 = new Database();
            db2.setName(TEST_DB2_NAME);
            db2.setLocationUri(TestUtil.WAREHOUSE_PATH + TEST_DB2_NAME);
            client.createDatabase(db2);

            db2 = client.getDatabase(TEST_DB2_NAME);

            assertEquals("name of returned db is different from that of inserted db",
                    TEST_DB2_NAME, db2.getName());
            assertEquals("location of the returned db is different from that of inserted db",
                    TestUtil.WAREHOUSE_PATH.replace("///", "/") + TEST_DB2_NAME, db2.getLocationUri());

            List<String> dbs = client.getDatabases(".*");

            assertTrue("first database is not " + TEST_DB1_NAME, dbs.contains(TEST_DB1_NAME));
            assertTrue("second database is not " + TEST_DB2_NAME, dbs.contains(TEST_DB2_NAME));

            client.dropDatabase(TEST_DB1_NAME);
            client.dropDatabase(TEST_DB2_NAME);
            silentDropDatabase(TEST_DB1_NAME);
            silentDropDatabase(TEST_DB2_NAME);
        } catch (Throwable e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testDatabase() failed.");
            throw e;
        }
    }

    @Test
    public void testDatabaseLocationWithPermissionProblems() throws Exception {

        // Note: The following test will fail if you are running this test as root. Setting
        // permission to '0' on the database folder will not preclude root from being able
        // to create the necessary files.

        if (System.getProperty("user.name").equals("root")) {
            System.err.println("Skipping test because you are running as root!");
            return;
        }

        silentDropDatabase(TEST_DB1_NAME);

        Database db = new Database();
        db.setName(TEST_DB1_NAME);
        String dbLocation =
                TestUtil.WAREHOUSE_PATH + "test/_testDB_create_";
        FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), hiveConf);
        fs.mkdirs(
                new Path(TestUtil.WAREHOUSE_PATH + "test"),
                new FsPermission((short) 0));
        db.setLocationUri(dbLocation);


        boolean createFailed = false;
        try {
            client.createDatabase(db);
        } catch (MetaException cantCreateDB) {
            createFailed = true;
        } finally {
            // Cleanup
            if (!createFailed) {
                try {
                    client.dropDatabase(TEST_DB1_NAME);
                } catch (Exception e) {
                    System.err.println("Failed to remove database in cleanup: " + e.getMessage());
                }
            }

            fs.setPermission(new Path(TestUtil.WAREHOUSE_PATH + "test"),
                    new FsPermission((short) 755));
            fs.delete(new Path(TestUtil.WAREHOUSE_PATH + "test"), true);
        }

        assertTrue("Database creation succeeded even with permission problem", createFailed);
    }

    @Test
    public void testDatabaseLocation() throws Throwable {
        try {
            // clear up any existing databases
            silentDropDatabase(TEST_DB1_NAME);

            Database db = new Database();
            db.setName(TEST_DB1_NAME);
            String dbLocation =
                    TestUtil.WAREHOUSE_PATH + "_testDB_create_";
            db.setLocationUri(dbLocation);
            client.createDatabase(db);

            db = client.getDatabase(TEST_DB1_NAME);

            assertEquals("name of returned db is different from that of inserted db",
                    TEST_DB1_NAME, db.getName());
            assertEquals("location of the returned db is different from that of inserted db",
                    dbLocation.replace("///", "/"), db.getLocationUri());

            client.dropDatabase(TEST_DB1_NAME);
            silentDropDatabase(TEST_DB1_NAME);

            boolean objectNotExist = false;
            try {
                client.getDatabase(TEST_DB1_NAME);
            } catch (NoSuchObjectException e) {
                objectNotExist = true;
            }
            assertTrue("Database " + TEST_DB1_NAME + " exists ", objectNotExist);

            db = new Database();
            db.setName(TEST_DB1_NAME);
            dbLocation =
                    TestUtil.WAREHOUSE_PATH + "_testDB_file_";
            FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), hiveConf);
            fs.createNewFile(new Path(dbLocation));
            fs.deleteOnExit(new Path(dbLocation));
            db.setLocationUri(dbLocation);

            boolean createFailed = false;
            try {
                client.createDatabase(db);
            } catch (MetaException cantCreateDB) {
                System.err.println(cantCreateDB.getMessage());
                createFailed = true;
            }
            assertTrue("Database creation succeeded even location exists and is a file", createFailed);

            objectNotExist = false;
            try {
                client.getDatabase(TEST_DB1_NAME);
            } catch (NoSuchObjectException e) {
                objectNotExist = true;
            }
            assertTrue("Database " + TEST_DB1_NAME + " exists when location is specified and is a file",
                    objectNotExist);
            silentDropDatabase(TEST_DB1_NAME);

        } catch (Throwable e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testDatabaseLocation() failed.");
            throw e;
        }
    }

    @Test
    public void testGetConfigValue() {

        String val = "value";

        //if (!isThriftClient) {
        try {
            assertEquals(client.getConfigValue("hive.key1", val), "value1");
            assertEquals(client.getConfigValue("hive.key2", val), "http://www.example.com");
            assertEquals(client.getConfigValue("hive.key3", val), "");
            assertEquals(client.getConfigValue("hive.key4", val), "0");
            assertEquals(client.getConfigValue("hive.key5", val), val);
            assertEquals(client.getConfigValue(null, val), val);
        } catch (ConfigValSecurityException e) {
            e.printStackTrace();
            assert (false);
        } catch (TException e) {
            e.printStackTrace();
            assert (false);
        }
        //}

        boolean threwException = false;
        try {
            // Attempting to get the password should throw an exception
            client.getConfigValue("javax.jdo.option.ConnectionPassword", "password");
        } catch (ConfigValSecurityException e) {
            threwException = true;
        } catch (TException e) {
            e.printStackTrace();
            assert (false);
        }
        assert (threwException);
    }

    /**
     * Test changing owner and owner type of a database
     *
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    @Test
    public void testDBOwnerChange() throws NoSuchObjectException, MetaException, TException {
        final String dbName = "alterDbOwner";
        final String user1 = "user1";
        final String user2 = "user2";
        final String role1 = "role1";

        silentDropDatabase(dbName);
        Database db = new Database();
        db.setLocationUri(TestUtil.WAREHOUSE_PATH + dbName);
        db.setName(dbName);
        db.setOwnerName(user1);
        db.setOwnerType(PrincipalType.USER);

        client.createDatabase(db);
        checkDbOwnerType(dbName, user1, PrincipalType.USER);

        db.setOwnerName(user2);
        client.alterDatabase(dbName, db);
        checkDbOwnerType(dbName, user2, PrincipalType.USER);

        db.setOwnerName(role1);
        db.setOwnerType(PrincipalType.ROLE);
        client.alterDatabase(dbName, db);
        checkDbOwnerType(dbName, role1, PrincipalType.ROLE);
        silentDropDatabase(dbName);

    }

    private void checkDbOwnerType(String dbName, String ownerName, PrincipalType ownerType)
            throws NoSuchObjectException, MetaException, TException {
        Database db = client.getDatabase(dbName);
        assertEquals("Owner name", ownerName, db.getOwnerName());
        assertEquals("Owner type", ownerType, db.getOwnerType());
    }

    @Test
    public void testRetriableClientWithConnLifetime() throws Exception {

        HiveConf conf = new HiveConf(hiveConf);
        conf.setLong(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME.name(), 60);
        long timeout = 65 * 1000; // Lets use a timeout more than the socket lifetime to simulate a reconnect

        // Test a normal retriable client
        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(conf, getHookLoader(), DlfSessionMetaStoreClient.class.getName());
        client.getAllDatabases();
        client.close();

        // Connect after the lifetime, there should not be any failures
        client = RetryingMetaStoreClient.getProxy(conf, getHookLoader(), DlfSessionMetaStoreClient.class.getName());
        Thread.sleep(timeout);
        client.getAllDatabases();
        client.close();
    }

    private HiveMetaHookLoader getHookLoader() {
        HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
            @Override
            public HiveMetaHook getHook(
                    org.apache.hadoop.hive.metastore.api.Table tbl)
                    throws MetaException {
                return null;
            }
        };
        return hookLoader;
    }
}