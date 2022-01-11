package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.aliyun.datalake20200710.models.PartitionInput;
import com.aliyun.datalake20200710.models.PrincipalPrivilegeSet;
import com.aliyun.datalake20200710.models.TableInput;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileOperationSwitchTest {
    private static final Logger logger = LoggerFactory.getLogger(FileOperationSwitchTest.class);
    @BeforeClass
    public static void setUp() throws TException, IOException {

    }

    @AfterClass
    public static void cleanUp() throws TException {
        HiveConf hiveConf = new HiveConf();
        DlfMetaStoreClient client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(TestUtil.TEST_DB, true, true, true);
    }

    @Test
    //gray test
    public void testFileOperationGray() throws MetaException {
        HiveConf hiveConf = new HiveConf();
        List<Double> grayRate = new ArrayList<Double>() {{add(0.3d); }{add(0.8d); }{add(1.2d); }{add(-0.3d);}};
        int totalCont = 100;
        int grayTrues = 0;
        for (Double gray : grayRate) {
            grayTrues = 0;
            for (int i = 0; i < totalCont; i++) {
                hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
                hiveConf.set("dlf.catalog.enable.file.operation", "true");
                hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", gray.toString());
                DlfMetaStoreClient dlfMetaStoreClient = new DlfMetaStoreClient(hiveConf);
                if (dlfMetaStoreClient.getEnableFsOperation()) {
                    grayTrues++;
                }
            }
            System.out.println("gray:" + grayTrues * 1.0 / totalCont);
            if (gray > 1.0) {
                gray = 1.0;
            }
            if (gray < 0.0) {
                gray = 0.0;
            }
            Assert.assertTrue("gray rate" + gray + "not right:" + grayTrues * 1.0 / totalCont + ":" + gray, Math.abs(grayTrues * 1.0 / totalCont - gray) < 0.15);
        }

        grayTrues = 0;
        for (int i = 0; i < totalCont; i++) {
            hiveConf.set("dlf.catalog.proxyMode", "DLF_ONLY");
            hiveConf.set("dlf.catalog.enable.file.operation", "true");
            hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "0.9");
            DlfMetaStoreClient dlfMetaStoreClient = new DlfMetaStoreClient(hiveConf);
            if (dlfMetaStoreClient.getEnableFsOperation()) {
                grayTrues++;
            }
        }
        Assert.assertEquals(grayTrues, totalCont);

        grayTrues = 0;
        for (int i = 0; i < totalCont; i++) {
            hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
            hiveConf.set("dlf.catalog.enable.file.operation", "false");
            hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "0.9");
            DlfMetaStoreClient dlfMetaStoreClient = new DlfMetaStoreClient(hiveConf);
            if (dlfMetaStoreClient.getEnableFsOperation()) {
                grayTrues++;
            }
        }
        Assert.assertEquals(grayTrues, 0);
    }

    //create database /drop database
    @Test
    public void testDatabaseOperation() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + dbName;
        HiveConf hiveConf = new HiveConf();
        FileSystem fs = FileSystem.get(hiveConf);
        //enable fsoperation = false(enable=false,grayrate=0), mkdir = false
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        DlfMetaStoreClient client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(dbName, true, true, true);
        fs.delete(new Path(path), true);
        Database database = new Database(dbName, d, path, null);
        client.createDatabase(database);
        ValidatorUtils.validateDatabases(database, client.getDatabase(dbName));
        ValidatorUtils.validateExist(fs, new Path(path), false);

        //enable fsoperation = false((enable=true,grayrate=0)), mkdir = false
        client.dropDatabase(dbName, true, true, true);
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        client = new DlfMetaStoreClient(hiveConf);
        client.createDatabase(database);
        ValidatorUtils.validateDatabases(database, client.getDatabase(dbName));
        ValidatorUtils.validateExist(fs, new Path(path), false);

        //enable fsoperation = true(enable=true,grayrate=1), mkdir = true
        client.dropDatabase(dbName, true, true, true);
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createDatabase(database);
        ValidatorUtils.validateDatabases(database, client.getDatabase(dbName));
        ValidatorUtils.validateExist(fs, new Path(path), true);

        //enable fsoperation = false,  delete meta = true， deletedir= false
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "false");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(dbName, true, true, true);
        ValidatorUtils.validateExist(fs, new Path(path), true);
        fs.delete(new Path(path), true);

        //enable fsoperation = true, mkdir=true,deletdir = true
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createDatabase(database);
        ValidatorUtils.validateDatabases(database, client.getDatabase(dbName));
        ValidatorUtils.validateExist(fs, new Path(path), true);
        client.dropDatabase(dbName, true, true, true);
        ValidatorUtils.validateExist(fs, new Path(path), false);

        //create database exception,then mkdir = true, rollback mkdir = true
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        IDataLakeMetaStore dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        IDataLakeMetaStore spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).createDatabase(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap(), Mockito.anyString(), Mockito.anyString(), Mockito.any(PrincipalPrivilegeSet.class));
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        final IMetaStoreClient client1 = client;
        Assert.assertThrows(MetaException.class, () -> client1.createDatabase(database));
        Assert.assertThrows(NoSuchObjectException.class, () -> client1.getDatabase(dbName));
        ValidatorUtils.validateExist(fs, new Path(path), false);

        //create database exception, then mkdir = false, rollback mkdir = false;
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "false");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).createDatabase(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap(), Mockito.anyString(), Mockito.anyString(), Mockito.any(PrincipalPrivilegeSet.class));
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        final IMetaStoreClient client2 = client;
        Assert.assertThrows(MetaException.class, () -> client2.createDatabase(database));
        Assert.assertThrows(NoSuchObjectException.class, () -> client2.getDatabase(dbName));
        ValidatorUtils.validateExist(fs, new Path(path), false);

        //grayrate = mkdir rate = deletedir rate
        int totalCount = 100;
        hiveConf = new HiveConf();
        Double gray = 0.7d;
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", gray.toString());
        int mkdirCount = 0;
        int deleteDirCount = 0;
        for (int i = 0; i < totalCount; i++) {
            client = new DlfMetaStoreClient(hiveConf);
            client.createDatabase(database);
            boolean isMkdir = fs.exists(new Path(path));
            if (isMkdir) {
                mkdirCount++;
            }
            client.dropDatabase(dbName, true, true, true);
            if (isMkdir && !fs.exists(new Path(path))) {
                deleteDirCount++;
            }
        }
        logger.info("gray rate 1:" + mkdirCount * 1.0 / totalCount);
        logger.info("gray rate 2:" + deleteDirCount * 1.0 / totalCount);
        Assert.assertTrue("gray rate" + gray + "not right:" + mkdirCount * 1.0 / totalCount + ":" + gray, Math.abs(mkdirCount * 1.0 / totalCount - gray) < 0.15);
        Assert.assertTrue("gray rate" + gray + "not right:" + deleteDirCount * 1.0 / totalCount + ":" + gray, Math.abs(deleteDirCount * 1.0 / totalCount - gray) < 0.15);
    }

    //create table /drop table
    @Test
    public void testTableFileOperation() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + dbName;
        HiveConf hiveConf = new HiveConf();
        DlfMetaStoreClient client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(dbName, true, true, true);
        Database database = new Database(dbName, d, path, null);
        client.createDatabase(database);
        FileSystem fs = FileSystem.get(hiveConf);
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(dbName, tblName, columns);
        Path tablePath = new Path(table.getSd().getLocation());

        //enable fsoperation = false(enable=false,grayrate=0), mkdir = false
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        ValidatorUtils.validateExist(fs, tablePath, false);

        //enable fsoperation = false((enable=true,grayrate=0)), mkdir = false
        client.dropTable(dbName, tblName, true, true, true);
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        ValidatorUtils.validateExist(fs, tablePath, false);

        //enable fsoperation = true(enable=true,grayrate=1), mkdir = true
        client.dropTable(dbName, tblName, true, true, true);
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        ValidatorUtils.validateExist(fs, tablePath, true);

        //enable fsoperation = false,  delete meta = true， deletedir= false
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "false");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.dropTable(dbName, tblName, true, true, true);
        ValidatorUtils.validateExist(fs, tablePath, true);
        fs.delete(tablePath, true);

        //enable fsoperation = true, mkdir=true,deletdir = true
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        ValidatorUtils.validateExist(fs, tablePath, true);
        client.dropTable(dbName, tblName, true, true, true);
        ValidatorUtils.validateExist(fs, tablePath, false);

        //create database exception,then mkdir = true, rollback mkdir = true
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        IDataLakeMetaStore dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        IDataLakeMetaStore spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).createTable(Mockito.anyString(), Mockito.any(TableInput.class));
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        final IMetaStoreClient client1 = client;
        Assert.assertThrows(MetaException.class, () -> client1.createTable(table));
        Assert.assertThrows(NoSuchObjectException.class, () -> client1.getTable(dbName, tblName));
        ValidatorUtils.validateExist(fs, tablePath, false);

        //create database exception, then mkdir = false, rollback mkdir = false;
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "false");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).createTable(Mockito.anyString(), Mockito.any(TableInput.class));
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        final IMetaStoreClient client2 = client;
        Assert.assertThrows(MetaException.class, () -> client1.createTable(table));
        Assert.assertThrows(NoSuchObjectException.class, () -> client1.getTable(dbName, tblName));
        ValidatorUtils.validateExist(fs, tablePath, false);

        //grayrate = mkdir rate = deletedir rate
        int totalCount = 100;
        hiveConf = new HiveConf();
        Double gray = 0.7d;
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", gray.toString());
        int mkdirCount = 0;
        int deleteDirCount = 0;
        for (int i = 0; i < totalCount; i++) {
            client = new DlfMetaStoreClient(hiveConf);
            client.createTable(table);
            boolean ismkdir = fs.exists(tablePath);
            if (ismkdir) {
                mkdirCount++;
            }
            client.dropTable(dbName, tblName, true, true, true);
            if (ismkdir && !fs.exists(tablePath)) {
                deleteDirCount++;
            }
        }
        logger.info("gray rate 1:" + mkdirCount * 1.0 / totalCount);
        logger.info("gray rate 2:" + deleteDirCount * 1.0 / totalCount);

        Assert.assertTrue("gray rate" + gray + "not right:" + mkdirCount * 1.0 / totalCount + ":" + gray, Math.abs(mkdirCount * 1.0 / totalCount - gray) <= 0.15);
        Assert.assertTrue("gray rate" + gray + "not right:" + deleteDirCount * 1.0 / totalCount + ":" + gray, Math.abs(deleteDirCount * 1.0 / totalCount - gray) <= 0.15);
    }

    //rename table/rename partition
    @Test
    public void testRenameTableFileOperation() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + dbName + ".db";
        HiveConf hiveConf = new HiveConf();
        DlfMetaStoreClient client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(dbName, true, true, true);
        Database database = new Database(dbName, d, path, null);
        client.createDatabase(database);
        FileSystem fs = FileSystem.get(hiveConf);
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Table table = TestUtil.getTable(dbName, tblName, columns);
        Path tablePath = new Path(table.getSd().getLocation());

        //enable fsoperation = false(enable=false,grayrate=0), rename operation=false
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        Table getTable = client.getTable(dbName, tblName);
        String newTableName = tblName + "_renamed";
        getTable.setTableName(newTableName);
        client.alter_table(dbName, tblName, getTable);
        Table renamedTable = client.getTable(dbName, newTableName);
        ValidatorUtils.validateExist(fs, new Path(renamedTable.getSd().getLocation()), false);

        //enable fsoperation = true(enable=true,grayrate=1.0), rename operation=false
        client.dropTable(dbName, newTableName);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        getTable = client.getTable(dbName, tblName);
        getTable.setTableName(newTableName);
        client.alter_table(dbName, tblName, getTable);
        renamedTable = client.getTable(dbName, newTableName);
        ValidatorUtils.validateExist(fs, new Path(renamedTable.getSd().getLocation()), false);

        //enable fsoperation = true(dlf_only), rename operation=true
        client.dropTable(dbName, newTableName);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_ONLY");
        hiveConf.set("dlf.catalog.enable.file.operation", "flase");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        getTable = client.getTable(dbName, tblName);
        getTable.setTableName(newTableName);
        client.alter_table(dbName, tblName, getTable);
        renamedTable = client.getTable(dbName, newTableName);
        ValidatorUtils.validateExist(fs, new Path(renamedTable.getSd().getLocation()), true);

        //enable fsoperation = true(dlf_only), rename operation=true rename exception then rollback
        client.dropTable(dbName, newTableName);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_ONLY");
        hiveConf.set("dlf.catalog.enable.file.operation", "flase");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        getTable = client.getTable(dbName, tblName);
        getTable.setTableName(newTableName);
        IDataLakeMetaStore dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        IDataLakeMetaStore spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).doRenameTableInMs(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(TableInput.class), Mockito.anyBoolean() );
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        IMetaStoreClient client1 = client;
        Table getTable1 = getTable;
        Assert.assertThrows(MetaException.class, () -> client1.alter_table(dbName, tblName, getTable1));
        Assert.assertThrows(NoSuchObjectException.class, () -> client1.getTable(dbName, newTableName));
        ValidatorUtils.validateExist(fs, new Path(getTable1.getSd().getLocation().replace(tblName, newTableName)), false);

        //enable fsoperation = true(DLF_METASTORE_FAILURE), rename operation=true
        client.dropTable(dbName, tblName);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_FAILURE");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        getTable = client.getTable(dbName, tblName);
        getTable.setTableName(newTableName);
        client.alter_table(dbName, tblName, getTable);
        renamedTable = client.getTable(dbName, newTableName);
        ValidatorUtils.validateExist(fs, new Path(renamedTable.getSd().getLocation()), true);

        //enable fsoperation = true(dlf_only), rename operation=true rename exception then rollback
        client.dropTable(dbName, newTableName);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_FAILURE");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.createTable(table);
        getTable = client.getTable(dbName, tblName);
        getTable.setTableName(newTableName);
        dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).doRenameTableInMs(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(TableInput.class), Mockito.anyBoolean() );
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        IMetaStoreClient client2 = client;
        Table getTable2 = getTable;
        Assert.assertThrows(MetaException.class, () -> client2.alter_table(dbName, tblName, getTable2));
        Assert.assertThrows(NoSuchObjectException.class, () -> client2.getTable(dbName, newTableName));
        ValidatorUtils.validateExist(fs, new Path(getTable1.getSd().getLocation().replace(tblName, newTableName)), false);
    }

    //create partition /drop partition
    @Test
    public void testPartitionFsOperation() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + dbName;
        HiveConf hiveConf = new HiveConf();
        DlfMetaStoreClient client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(dbName, true, true, true);
        Database database = new Database(dbName, d, path, null);
        client.createDatabase(database);
        FileSystem fs = FileSystem.get(hiveConf);
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partCols = ImmutableMap.of("string", "date");
        Table table = TestUtil.getPartitionTable(dbName, tblName, columns, partCols);
        client.createTable(table);
        Partition partition = TestUtil.buildPartition(dbName, tblName, table, 1, Lists.newArrayList("2020"));
        Path partitionPath = new Path(partition.getSd().getLocation());

        //enable fsoperation = false(enable=false,grayrate=0), mkdir = false
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        ValidatorUtils.validateExist(fs, partitionPath, false);

        //enable fsoperation = false((enable=true,grayrate=0)), mkdir = false
        client.dropPartition(dbName, tblName, partition.getValues(), true);
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        ValidatorUtils.validateExist(fs, partitionPath, false);

        //enable fsoperation = true(enable=true,grayrate=1), mkdir = true
        client.dropPartition(dbName, tblName, partition.getValues(), true);
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        ValidatorUtils.validateExist(fs, partitionPath, true);

        //enable fsoperation = false,  delete meta = true， deletedir= false
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "false");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.dropPartition(dbName, tblName, partition.getValues(), true);
        ValidatorUtils.validateExist(fs, partitionPath, true);
        fs.delete(partitionPath, true);

        //enable fsoperation = true, mkdir=true,deletdir = true
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        ValidatorUtils.validateExist(fs, partitionPath, true);
        client.dropPartition(dbName, tblName, partition.getValues(), true);
        ValidatorUtils.validateExist(fs, partitionPath, false);

        //create database exception,then mkdir = true, rollback mkdir = true
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        IDataLakeMetaStore dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        IDataLakeMetaStore spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).addPartitions(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean());
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        final IMetaStoreClient client2 = client;
        Assert.assertThrows(MetaException.class, () -> client2.add_partition(partition));
        Assert.assertThrows(NoSuchObjectException.class, () -> client2.getPartition(dbName, tblName, partition.getValues()));
        ValidatorUtils.validateExist(fs, partitionPath, false);

        //create database exception, then mkdir = false, rollback mkdir = false;
        hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "false");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).addPartitions(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean());
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);
        final IMetaStoreClient client1 = client;
        Assert.assertThrows(MetaException.class, () -> client1.add_partition(partition));
        Assert.assertThrows(NoSuchObjectException.class, () -> client1.getPartition(dbName, tblName, partition.getValues()));
        ValidatorUtils.validateExist(fs, partitionPath, false);

        //grayrate = mkdir rate = deletedir rate
        int totalCount = 100;
        hiveConf = new HiveConf();
        Double gray = 0.7d;
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", gray.toString());
        int mkdirCount = 0;
        int deleteDirCount = 0;
        for (int i = 0; i < totalCount; i++) {
            client = new DlfMetaStoreClient(hiveConf);
            client.add_partition(partition);
            boolean isMkdir = fs.exists(partitionPath);
            if (isMkdir) {
                mkdirCount++;
            }
            client.dropPartition(dbName, tblName, partition.getValues(), true);
            if (isMkdir && !fs.exists(partitionPath)) {
                deleteDirCount++;
            }
        }
        logger.info("gray rate 1:" + mkdirCount * 1.0 / totalCount);
        logger.info("gray rate 2:" + deleteDirCount * 1.0 / totalCount);
        Assert.assertTrue("gray rate" + gray + "not right:" + mkdirCount * 1.0 / totalCount + ":" + gray, Math.abs(mkdirCount * 1.0 / totalCount - gray) <= 0.15);
        Assert.assertTrue("gray rate" + gray + "not right:" + deleteDirCount * 1.0 / totalCount + ":" + gray, Math.abs(deleteDirCount * 1.0 / totalCount - gray) <= 0.15);
    }

    @Test
    public void testRenamePartitionFileOperation() throws Exception {
        String dbName = TestUtil.TEST_DB;
        String d = "Default Hive database";
        String path = TestUtil.WAREHOUSE_PATH + dbName + ".db";
        HiveConf hiveConf = new HiveConf();
        DlfMetaStoreClient client = new DlfMetaStoreClient(hiveConf);
        client.dropDatabase(dbName, true, true, true);
        Database database = new Database(dbName, d, path, null);
        client.createDatabase(database);
        FileSystem fs = FileSystem.get(hiveConf);
        String tblName = TestUtil.TEST_TABLE;
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partCols = ImmutableMap.of("string", "date");
        Table table = TestUtil.getPartitionTable(dbName, tblName, columns, partCols);
        client.createTable(table);
        Partition partition = TestUtil.buildPartition(dbName, tblName, table, 1, Lists.newArrayList("2020"));
        Path partitionPath = new Path(partition.getSd().getLocation());

        //enable fsoperation = false(enable=false,grayrate=0), rename operation=false
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        Partition getPartition = client.getPartition(dbName, tblName, partition.getValues());
        getPartition.setValues(Lists.newArrayList("2021"));
        client.renamePartition(dbName, tblName, Lists.newArrayList("2020"), getPartition);
        Partition renamedPartition = client.getPartition(dbName, tblName, getPartition.getValues());
        ValidatorUtils.validateExist(fs, new Path(renamedPartition.getSd().getLocation()), false);

        //enable fsoperation = true(enable=true,grayrate=1.0), rename operation=false
        client.dropPartition(dbName, tblName, getPartition.getValues(), true);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_SUCCESS");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        getPartition = client.getPartition(dbName, tblName, partition.getValues());
        getPartition.setValues(Lists.newArrayList("2021"));
        client.renamePartition(dbName, tblName, Lists.newArrayList("2020"), getPartition);
        renamedPartition = client.getPartition(dbName, tblName, getPartition.getValues());
        ValidatorUtils.validateExist(fs, new Path(renamedPartition.getSd().getLocation()), false);

        //enable fsoperation = true(dlf_only), rename operation=true
        client.dropPartition(dbName, tblName, getPartition.getValues(), true);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_ONLY");
        hiveConf.set("dlf.catalog.enable.file.operation", "flase");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        getPartition = client.getPartition(dbName, tblName, partition.getValues());
        getPartition.setValues(Lists.newArrayList("2021"));
        client.renamePartition(dbName, tblName, Lists.newArrayList("2020"), getPartition);
        renamedPartition = client.getPartition(dbName, tblName, getPartition.getValues());
        ValidatorUtils.validateExist(fs, new Path(renamedPartition.getSd().getLocation()), true);


        //enable fsoperation = true(dlf_only), rename operation=true rename exception then rollback
        client.dropPartition(dbName, tblName, getPartition.getValues(), true);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_ONLY");
        hiveConf.set("dlf.catalog.enable.file.operation", "flase");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        getPartition = client.getPartition(dbName, tblName, partition.getValues());
        getPartition.setValues(Lists.newArrayList("2021"));

        IDataLakeMetaStore dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        IDataLakeMetaStore spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).renamePartitionInCatalog(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.any(PartitionInput.class));
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);

        IMetaStoreClient client1 = client;
        Partition getPartition1 = getPartition;
        Assert.assertThrows(MetaException.class, () -> client1.renamePartition(dbName, tblName, Lists.newArrayList("2020"), getPartition1));
        Assert.assertThrows(NoSuchObjectException.class, () -> client1.getPartition(dbName, tblName, getPartition1.getValues()));
        ValidatorUtils.validateExist(fs, new Path(getPartition.getSd().getLocation().replace("2020", "2021")), false);

        //enable fsoperation = true(DLF_METASTORE_FAILURE), rename operation=true
        client.dropPartition(dbName, tblName, partition.getValues(), true);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_FAILURE");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        getPartition = client.getPartition(dbName, tblName, partition.getValues());
        getPartition.setValues(Lists.newArrayList("2021"));
        client.renamePartition(dbName, tblName, Lists.newArrayList("2020"), getPartition);
        renamedPartition = client.getPartition(dbName, tblName, getPartition.getValues());
        ValidatorUtils.validateExist(fs, new Path(renamedPartition.getSd().getLocation()), true);


        //enable fsoperation = true(DLF_METASTORE_FAILURE), rename operation=true rename exception then rollback
        client.dropPartition(dbName, tblName, getPartition.getValues(), true);
        hiveConf.set("dlf.catalog.proxyMode", "DLF_METASTORE_FAILURE");
        hiveConf.set("dlf.catalog.enable.file.operation", "true");
        hiveConf.set("dlf.catalog.enable.file.operation.gray.rate", "1.0");
        client = new DlfMetaStoreClient(hiveConf);
        client.add_partition(partition);
        getPartition = client.getPartition(dbName, tblName, partition.getValues());
        getPartition.setValues(Lists.newArrayList("2021"));

        dataLakeMetaStore = client.getClientDelegate().getDataLakeMetaStore();
        spyDataLakeMetastore = Mockito.spy(dataLakeMetaStore);
        Mockito.doThrow(Exception.class).when(spyDataLakeMetastore).renamePartitionInCatalog(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.any(PartitionInput.class));
        client.getClientDelegate().setDataLakeMetaStore(spyDataLakeMetastore);

        IMetaStoreClient client2 = client;
        Partition getPartition2 = getPartition;
        Assert.assertThrows(MetaException.class, () -> client2.renamePartition(dbName, tblName, Lists.newArrayList("2020"), getPartition2));
        Assert.assertThrows(NoSuchObjectException.class, () -> client2.getPartition(dbName, tblName, getPartition2.getValues()));
        ValidatorUtils.validateExist(fs, new Path(getPartition.getSd().getLocation().replace("2020", "2021")), false);

    }

}
