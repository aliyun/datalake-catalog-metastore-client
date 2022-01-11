package com.aliyun.datalake.metastore.hive2;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
@Ignore
public class MetaClientPerformanceTest extends BaseTest {
    private static int ROUNDS = 1;
    private static int batchSize = 5;
    private static String TEST_DB = null;
    private static long startTime;
    private static long stopTime;
    private IMetaStoreClient client;

    public MetaClientPerformanceTest(IMetaStoreClient client) {
        this.client = client;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> clients() throws MetaException, FileNotFoundException {
        IMetaStoreClient hmsClient = TestUtil.getHMSClient();
        IMetaStoreClient dlfClient = TestUtil.getDlfClient();
        return Arrays.asList(new Object[][]{{hmsClient}, {dlfClient}});
    }

    @After
    public void cleanUpCase() throws TException {
        if (TEST_DB != null) {
            client.dropDatabase(TEST_DB, true, true, true);
            TEST_DB = null;
        }
    }

    @Test
    public void getDatabase() throws TException {
        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.getDatabase("default");
        }
        stopWatch("GetDatabase");
    }

    @Test
    public void createDatabase() throws TException {
        String name = TestUtil.randomNumStr("db");

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.createDatabase(TestUtil.buildDatabase(name + i));
        }
        stopWatch("CreateDatabase");

        // clean up
        for (int i = 0; i < ROUNDS; i++) {
            client.dropDatabase(name + i, true, true, true);
        }
    }

    @Test
    public void dropDatabase() throws TException {
        String name = TestUtil.randomNumStr("db");

        for (int i = 0; i < ROUNDS; i++) {
            client.createDatabase(TestUtil.buildDatabase(name + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.dropDatabase(name + i);
        }
        stopWatch("DropDatabase");
    }

    @Test
    public void alterDatabase() throws TException {
        String name = TestUtil.randomNumStr("db");
        String path = TestUtil.WAREHOUSE_PATH + "db_update";

        for (int i = 0; i < ROUNDS; i++) {
            client.createDatabase(TestUtil.buildDatabase(name + i, path));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.alterDatabase(name + i, TestUtil.buildDatabase(name + i, path + "_new"));
        }
        stopWatch("AlterDatabase");

        // clean up
        for (int i = 0; i < ROUNDS; i++) {
            client.dropDatabase(name + i, true, true, true);
        }
    }

    @Test
    public void createTable() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String name = TestUtil.randomNumStr("tbl");

        client.createDatabase(TestUtil.buildDatabase(TEST_DB));

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.createTable(TestUtil.buildTable(TEST_DB, name + i));
        }
        stopWatch("CreateTable");
    }

    @Test
    public void dropTable() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String name = TestUtil.randomNumStr("tbl");

        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        for (int i = 0; i < ROUNDS; i++) {
            client.createTable(TestUtil.buildTable(TEST_DB, name + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.dropTable(TEST_DB, name + i);
        }
        stopWatch("DropTable");
    }

    @Test
    public void getTable() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String name = TestUtil.randomNumStr("tbl");

        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        for (int i = 0; i < ROUNDS; i++) {
            client.createTable(TestUtil.buildTable(TEST_DB, name + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.getTable(TEST_DB, name + i);
        }
        stopWatch("GetTable");

    }

    @Test
    public void tableExists() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String name = TestUtil.randomNumStr("tbl");

        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        for (int i = 0; i < ROUNDS; i++) {
            client.createTable(TestUtil.buildTable(TEST_DB, name + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            Assert.assertTrue(client.tableExists(TEST_DB, name + i));
        }
        stopWatch("TableExists");

    }

    @Test
    public void alterTable() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String name = TestUtil.randomNumStr("tbl");

        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        for (int i = 0; i < ROUNDS; i++) {
            client.createTable(TestUtil.buildTable(TEST_DB, name + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            Table tbl = TestUtil.buildTable(TEST_DB, name + i);
            tbl.setOwner("owner_new");
            tbl.setCreateTime(1024);
            client.alter_table(TEST_DB, name + i, tbl);
        }
        stopWatch("AlterTable");
    }

    @Test
    public void renameTable() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String name = TestUtil.randomNumStr("tbl");

        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        for (int i = 0; i < ROUNDS; i++) {
            client.createTable(TestUtil.buildTable(TEST_DB, name + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.alter_table(TEST_DB, name + i, TestUtil.buildTable(TEST_DB, name + "_new" + i));
        }
        stopWatch("RenameTable");
    }

    @Test
    public void createFunction() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String functionName = TestUtil.randomNumStr("fun");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.createFunction(TestUtil.buildFunction(TEST_DB, functionName + i));
        }
        stopWatch("CreateFunction");
    }

    @Test
    public void dropFunction() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String functionName = TestUtil.randomNumStr("fun");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));

        for (int i = 0; i < ROUNDS; i++) {
            client.createFunction(TestUtil.buildFunction(TEST_DB, functionName + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.dropFunction(TEST_DB, functionName + i);
        }
        stopWatch("DropFunction");
    }

    @Test
    public void getFunction() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String functionName = TestUtil.randomNumStr("fun");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));

        for (int i = 0; i < ROUNDS; i++) {
            client.createFunction(TestUtil.buildFunction(TEST_DB, functionName + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.getFunction(TEST_DB, functionName + i);
        }
        stopWatch("GetFunction");

    }

    @Test
    public void alterFunction() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String functionName = TestUtil.randomNumStr("fun");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));

        for (int i = 0; i < ROUNDS; i++) {
            client.createFunction(TestUtil.buildFunction(TEST_DB, functionName + i));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            Function function = TestUtil.buildFunction(TEST_DB, functionName + i);
            function.setOwnerName("ower_new");
            client.alterFunction(TEST_DB, functionName + i, function);
        }
        stopWatch("AlterFunction");

    }

    @Test
    public void addPartition() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            Partition partition = TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i)));
            client.add_partition(partition);
        }
        stopWatch("AddPartition");
    }

    @Test
    public void addPartitions() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.add_partitions(parts.subList(i * batchSize, i * batchSize + batchSize));
        }
        stopWatch("AddPartitions_" + batchSize);
    }

    @Test
    public void getPartition() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        for (int i = 0; i < ROUNDS; i++) {
            Partition partition = TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i)));
            client.add_partition(partition);
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.getPartition(TEST_DB, tableName, "day=" + (2020 + i));
        }
        stopWatch("GetPartition");
    }

    @Test
    public void getPartitionsByNames() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        List<String> partNames = new ArrayList<>(ROUNDS * batchSize);
        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            partNames.add("day=" + (2020 + i));
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }
        client.add_partitions(parts);

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.getPartitionsByNames(TEST_DB, tableName, partNames.subList(i * batchSize, i * batchSize + batchSize));
        }
        stopWatch("GetPartitionsByNames_" + batchSize);
    }

    @Test
    public void listPartitions() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        // add partitions ( ROUNDS * batchSize )
        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }
        for (int i = 0; i < ROUNDS; i++) {
            client.add_partitions(parts.subList(i * batchSize, i * batchSize + batchSize));
        }

        // for each round list first `batchSize` partitions
        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.listPartitions(TEST_DB, tableName, (short) batchSize);
        }
        stopWatch("ListPartitions_" + batchSize);
    }

    @Test
    public void listPartitionNames() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        // add partitions ( ROUNDS * batchSize )
        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }
        for (int i = 0; i < ROUNDS; i++) {
            client.add_partitions(parts.subList(i * batchSize, i * batchSize + batchSize));
        }

        // for each round list first `batchSize` partitions
        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.listPartitionNames(TEST_DB, tableName, (short) batchSize);
        }
        stopWatch("ListPartitionNames_" + batchSize);
    }

    @Test
    public void listPartitionsByFilter() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        // add partitions ( ROUNDS * batchSize )
        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }
        for (int i = 0; i < ROUNDS; i++) {
            client.add_partitions(parts.subList(i * batchSize, i * batchSize + batchSize));
        }

        // for each round list first `batchSize` partitions
        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.listPartitionsByFilter(TEST_DB, tableName,
                    "day>=" + (2020 + i * batchSize), (short) batchSize);
        }
        stopWatch("ListPartitionsByFilter_" + batchSize);
    }

    @Test
    public void dropPartition() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        for (int i = 0; i < ROUNDS; i++) {
            Partition partition = TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i)));
            client.add_partition(partition);
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.dropPartition(TEST_DB, tableName, "day=" + (2020 + i), false);
        }
        stopWatch("DropPartition");
    }

    @Ignore
    @Test
    public void dropPartitions() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }

        for (int i = 0; i < ROUNDS; i++) {
            client.add_partitions(parts.subList(i * batchSize, i * batchSize + batchSize));
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            List<ObjectPair<Integer, byte[]>> partExprs = new ArrayList<>(batchSize);
            // TODO partExprs
            client.dropPartitions(TEST_DB, tableName, partExprs, false, false);
        }
        stopWatch("DropPartitions_" + batchSize);
    }

    @Test
    public void renamePartition() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        for (int i = 0; i < ROUNDS; i++) {
            Partition partition = TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i)));
            client.add_partition(partition);
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.renamePartition(TEST_DB, tableName, Lists.newArrayList(String.valueOf(2020 + i)),
                    TestUtil.buildPartition(TEST_DB, tableName, 1,
                            Lists.newArrayList(String.valueOf(1000000 + 2020 + i))));
        }
        stopWatch("RenamePartition");
    }

    @Test
    public void alterPartition() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        for (int i = 0; i < ROUNDS; i++) {
            Partition partition = TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i)));
            client.add_partition(partition);
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            Partition partition = TestUtil.buildPartition(TEST_DB, tableName, 100,
                    Lists.newArrayList(String.valueOf(2020 + i)));
            client.alter_partition(TEST_DB, tableName, partition);
        }
        stopWatch("AlterPartition");
    }

    @Test
    public void alterPartitions() throws TException {
        TEST_DB = TestUtil.randomNumStr("db");
        String tableName = TestUtil.randomNumStr("tbl");
        client.createDatabase(TestUtil.buildDatabase(TEST_DB));
        client.createTable(TestUtil.buildTable(TEST_DB, tableName));

        List<Partition> parts = new ArrayList<>(ROUNDS * batchSize);
        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.add(TestUtil.buildPartition(TEST_DB, tableName, 1,
                    Lists.newArrayList(String.valueOf(2020 + i))));
        }

        for (int i = 0; i < ROUNDS; i++) {
            client.add_partitions(parts.subList(i * batchSize, i * batchSize + batchSize));
        }

        for (int i = 0; i < ROUNDS * batchSize; i++) {
            parts.get(i).setCreateTime(100);
        }

        startWatch();
        for (int i = 0; i < ROUNDS; i++) {
            client.alter_partitions(TEST_DB, tableName, parts.subList(i * batchSize, i * batchSize + batchSize));
        }
        stopWatch("AlterPartitions_" + batchSize);
    }
    // END OF TESTS

    private void startWatch() {
        startTime = System.currentTimeMillis();
    }

    private void stopWatch(String msg) {
        stopTime = System.currentTimeMillis();
        System.out.println(client.getClass().getSimpleName() + "_" + msg + " time(ms): " + (stopTime - startTime));
    }
}