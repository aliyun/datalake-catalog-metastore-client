package com.aliyun.datalake.metastore.hive2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.CompositePartitionSpecProxy;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PartitionSpecsTest {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionSpecsTest.class);
    private static int msPort;
    private static Configuration conf;
    private static IMetaStoreClient client;
    private static String dbName = "metastore_ut_testpartitionspecs_db";
    private static String tableName = "metastore_ut_testpartitionspecs_table";
    private static int nDates = 10;
    private static String datePrefix = "2014010";

    @AfterClass
    public static void tearDown() throws Exception {
        LOG.info("Shutting down metastore.");

        try {
            client.dropDatabase(dbName, true, true, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void startMetaStoreServer() throws Exception {
        HiveConf hiveConf = new HiveConf();
        client = TestUtil.getDlfClient();
    }

    private static void createTable(boolean enablePartitionGrouping) throws Exception {


        List<FieldSchema> columns = new ArrayList<>();
        columns.add(new FieldSchema("foo", "string", ""));
        columns.add(new FieldSchema("bar", "string", ""));

        List<FieldSchema> partColumns = new ArrayList<>();
        partColumns.add(new FieldSchema("dt", "string", ""));
        partColumns.add(new FieldSchema("blurb", "string", ""));

        SerDeInfo serdeInfo = new SerDeInfo("LBCSerDe",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", new HashMap<>());

        StorageDescriptor storageDescriptor
                = new StorageDescriptor(columns, null,
                "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
                false, 0, serdeInfo, null, null, null);

        Map<String, String> tableParameters = new HashMap<>();
        tableParameters.put("hive.hcatalog.partition.spec.grouping.enabled", enablePartitionGrouping ? "true" : "false");
        Table table = new Table(tableName, dbName, "", 0, 0, 0, storageDescriptor, partColumns, tableParameters, "", "", "");

        client.createTable(table);
        Assert.assertTrue("Table " + dbName + "." + tableName + " does not exist",
                client.tableExists(dbName, tableName));

    }

    private static void clearAndRecreateDB() throws Exception {
        client.dropDatabase(dbName,
                true,   // Delete data.
                true,   // Ignore unknownDB.
                true    // Cascade.
        );

        Database database = TestUtil.buildDatabase(dbName);
        client.createDatabase(database);
    }

    // Get partition-path. For grid='XYZ', place the partition outside the table-path.
    private static String getPartitionPath(Table table, List<String> partValues) {

        return partValues.get(1).equalsIgnoreCase("isLocatedOutsideTablePath") ? // i.e. Is the partition outside the table-dir?
                table.getSd().getLocation().replace(table.getTableName(), "location_outside_" + table.getTableName())
                        + "_" + partValues.get(0) + "_" + partValues.get(1)
                : null; // Use defaults... Partitions are put in the table directory.

    }

    private static void populatePartitions(Table table, List<String> blurbs) throws Exception {
        for (int i = 0; i < nDates; ++i) {
            for (String blurb : blurbs) {
                StorageDescriptor sd = new StorageDescriptor(table.getSd());
                // Add partitions located in the table-directory (i.e. default).
                List<String> values = Arrays.asList(datePrefix + i, blurb);
                sd.setLocation(getPartitionPath(table, values));
                client.add_partition(new Partition(values, dbName, tableName, 0, 0, sd, null));
            }
        }
    }

    private void testGetPartitionSpecs(boolean enablePartitionGrouping) {
        try {
            clearAndRecreateDB();
            createTable(enablePartitionGrouping);
            Table table = client.getTable(dbName, tableName);
            populatePartitions(table, Arrays.asList("isLocatedInTablePath", "isLocatedOutsideTablePath"));

            PartitionSpecProxy partitionSpecProxy = client.listPartitionSpecs(dbName, tableName, -1);
            Assert.assertEquals("Unexpected number of partitions.", nDates * 2, partitionSpecProxy.size());

            Map<String, List<String>> locationToDateMap = new HashMap<>();
            locationToDateMap.put("isLocatedInTablePath", new ArrayList<>());
            locationToDateMap.put("isLocatedOutsideTablePath", new ArrayList<>());
            PartitionSpecProxy.PartitionIterator iterator = partitionSpecProxy.getPartitionIterator();

            while (iterator.hasNext()) {
                Partition partition = iterator.next();
                locationToDateMap.get(partition.getValues().get(1)).add(partition.getValues().get(0));
            }

            List<String> expectedDates = new ArrayList<>(nDates);
            for (int i = 0; i < nDates; ++i) {
                expectedDates.add(datePrefix + i);
            }

            Assert.assertArrayEquals("Unexpected date-values.", expectedDates.toArray(), locationToDateMap.get("isLocatedInTablePath").toArray());
            Assert.assertArrayEquals("Unexpected date-values.", expectedDates.toArray(), locationToDateMap.get("isLocatedOutsideTablePath").toArray());

            partitionSpecProxy = client.listPartitionSpecsByFilter(dbName, tableName, "blurb = \"isLocatedOutsideTablePath\"", -1);
            locationToDateMap.get("isLocatedInTablePath").clear();
            locationToDateMap.get("isLocatedOutsideTablePath").clear();
            iterator = partitionSpecProxy.getPartitionIterator();

            while (iterator.hasNext()) {
                Partition partition = iterator.next();
                locationToDateMap.get(partition.getValues().get(1)).add(partition.getValues().get(0));
            }

            Assert.assertEquals("Unexpected date-values.", 0, locationToDateMap.get("isLocatedInTablePath").size());
            Assert.assertArrayEquals("Unexpected date-values.", expectedDates.toArray(), locationToDateMap.get("isLocatedOutsideTablePath").toArray());


        } catch (Throwable t) {
            LOG.error("Unexpected Exception!", t);
            t.printStackTrace();
            Assert.assertTrue("Unexpected Exception!", false);
        }
    }

    /**
     * Test for HiveMetaStoreClient.listPartitionSpecs() and HiveMetaStoreClient.listPartitionSpecsByFilter().
     * Check behaviour with and without Partition-grouping enabled.
     */
    @Test
    public void testGetPartitionSpecs_WithAndWithoutPartitionGrouping() {
        testGetPartitionSpecs(true);
        testGetPartitionSpecs(false);
    }


    /**
     * Test to confirm that partitions can be added using PartitionSpecs.
     */
    @Test
    public void testAddPartitions() {
        try {
            // Create source table.
            clearAndRecreateDB();
            createTable(true);
            Table table = client.getTable(dbName, tableName);
            populatePartitions(table, Arrays.asList("isLocatedInTablePath", "isLocatedOutsideTablePath"));

            // Clone the table,
            String targetTableName = "cloned_" + tableName;
            Table targetTable = new Table(table);
            targetTable.setTableName(targetTableName);
            StorageDescriptor targetTableSd = new StorageDescriptor(targetTable.getSd());
            targetTableSd.setLocation(
                    targetTableSd.getLocation().replace(tableName, targetTableName));
            client.createTable(targetTable);

            // Get partition-list from source.
            PartitionSpecProxy partitionsForAddition
                    = client.listPartitionSpecsByFilter(dbName, tableName, "blurb = \"isLocatedInTablePath\"", -1);
            partitionsForAddition.setTableName(targetTableName);
            partitionsForAddition.setRootLocation(targetTableSd.getLocation());

            Assert.assertEquals("Unexpected number of partitions added. ",
                    partitionsForAddition.size(), client.add_partitions_pspec(partitionsForAddition));

            // Check that the added partitions are as expected.
            PartitionSpecProxy clonedPartitions = client.listPartitionSpecs(dbName, targetTableName, -1);
            Assert.assertEquals("Unexpected number of partitions returned. ",
                    partitionsForAddition.size(), clonedPartitions.size());

            PartitionSpecProxy.PartitionIterator sourceIterator = partitionsForAddition.getPartitionIterator(),
                    targetIterator = clonedPartitions.getPartitionIterator();

            while (targetIterator.hasNext()) {
                Partition sourcePartition = sourceIterator.next(),
                        targetPartition = targetIterator.next();
                Assert.assertEquals("Mismatched values.",
                        sourcePartition.getValues(), targetPartition.getValues());
                Assert.assertEquals("Mismatched locations.",
                        sourcePartition.getSd().getLocation(), targetPartition.getSd().getLocation());
            }
        } catch (Throwable t) {
            LOG.error("Unexpected Exception!", t);
            t.printStackTrace();
            Assert.assertTrue("Unexpected Exception!", false);
        }
    }

    /**
     * Test to confirm that Partition-grouping behaves correctly when Table-schemas evolve.
     * Partitions must be grouped by location and schema.
     */
    @Test
    public void testFetchingPartitionsWithDifferentSchemas() {
        try {
            // Create source table.
            clearAndRecreateDB();
            createTable(true);
            Table table = client.getTable(dbName, tableName);
            populatePartitions(
                    table,
                    Arrays.asList("isLocatedInTablePath", "isLocatedOutsideTablePath") // Blurb list.
            );

            // Modify table schema. Add columns.
            List<FieldSchema> fields = table.getSd().getCols();
            fields.add(new FieldSchema("goo", "string", "Entirely new column. Doesn't apply to older partitions."));
            table.getSd().setCols(fields);
            client.alter_table(dbName, tableName, table);
            // Check that the change stuck.
            table = client.getTable(dbName, tableName);
            Assert.assertEquals("Unexpected number of table columns.",
                    3, table.getSd().getColsSize());

            // Add partitions with new schema.
            // Mark Partitions with new schema with different blurb.
            populatePartitions(table, Arrays.asList("hasNewColumn"));

            // Retrieve *all* partitions from the table.
            PartitionSpecProxy partitionSpecProxy = client.listPartitionSpecs(dbName, tableName, -1);
            Assert.assertEquals("Unexpected number of partitions.", nDates * 3, partitionSpecProxy.size());

            // Confirm grouping.
            Assert.assertTrue("Unexpected type of PartitionSpecProxy.", partitionSpecProxy instanceof CompositePartitionSpecProxy);
            CompositePartitionSpecProxy compositePartitionSpecProxy = (CompositePartitionSpecProxy) partitionSpecProxy;
            List<PartitionSpec> partitionSpecs = compositePartitionSpecProxy.toPartitionSpec();
            Assert.assertTrue("PartitionSpec[0] should have been a SharedSDPartitionSpec.",
                    partitionSpecs.get(0).isSetSharedSDPartitionSpec());
            Assert.assertEquals("PartitionSpec[0] should use the table-path as the common root location. ",
                    table.getSd().getLocation(), partitionSpecs.get(0).getRootPath());
            Assert.assertTrue("PartitionSpec[1] should have been a SharedSDPartitionSpec.",
                    partitionSpecs.get(1).isSetSharedSDPartitionSpec());
            Assert.assertEquals("PartitionSpec[1] should use the table-path as the common root location. ",
                    table.getSd().getLocation(), partitionSpecs.get(1).getRootPath());
            Assert.assertTrue("PartitionSpec[2] should have been a ListComposingPartitionSpec.",
                    partitionSpecs.get(2).isSetPartitionList());

            // Categorize the partitions returned, and confirm that all partitions are accounted for.
            PartitionSpecProxy.PartitionIterator iterator = partitionSpecProxy.getPartitionIterator();
            Map<String, List<Partition>> blurbToPartitionList = new HashMap<>(3);
            while (iterator.hasNext()) {

                Partition partition = iterator.next();
                String blurb = partition.getValues().get(1);

                if (!blurbToPartitionList.containsKey(blurb)) {
                    blurbToPartitionList.put(blurb, new ArrayList<>(nDates));
                }

                blurbToPartitionList.get(blurb).add(partition);

            } // </Classification>

            // All partitions with blurb="isLocatedOutsideTablePath" should have 2 columns,
            // and must have locations outside the table directory.
            for (Partition partition : blurbToPartitionList.get("isLocatedOutsideTablePath")) {
                Assert.assertEquals("Unexpected number of columns.", 2, partition.getSd().getCols().size());
                Assert.assertEquals("Unexpected first column.", "foo", partition.getSd().getCols().get(0).getName());
                Assert.assertEquals("Unexpected second column.", "bar", partition.getSd().getCols().get(1).getName());
                String partitionLocation = partition.getSd().getLocation();
                String tableLocation = table.getSd().getLocation();
                Assert.assertTrue("Unexpected partition location: " + partitionLocation + ". " +
                                "Partition should have been outside table location: " + tableLocation,
                        !partitionLocation.startsWith(tableLocation));
            }

            // All partitions with blurb="isLocatedInTablePath" should have 2 columns,
            // and must have locations within the table directory.
            for (Partition partition : blurbToPartitionList.get("isLocatedInTablePath")) {
                Assert.assertEquals("Unexpected number of columns.", 2, partition.getSd().getCols().size());
                Assert.assertEquals("Unexpected first column.", "foo", partition.getSd().getCols().get(0).getName());
                Assert.assertEquals("Unexpected second column.", "bar", partition.getSd().getCols().get(1).getName());
                String partitionLocation = partition.getSd().getLocation();
                String tableLocation = table.getSd().getLocation();
                Assert.assertTrue("Unexpected partition location: " + partitionLocation + ". " +
                                "Partition should have been within table location: " + tableLocation,
                        partitionLocation.startsWith(tableLocation));
            }

            // All partitions with blurb="hasNewColumn" were added after the table schema changed,
            // and must have 3 columns. Also, the partition locations must lie within the table directory.
            for (Partition partition : blurbToPartitionList.get("hasNewColumn")) {
                Assert.assertEquals("Unexpected number of columns.", 3, partition.getSd().getCols().size());
                Assert.assertEquals("Unexpected first column.", "foo", partition.getSd().getCols().get(0).getName());
                Assert.assertEquals("Unexpected second column.", "bar", partition.getSd().getCols().get(1).getName());
                Assert.assertEquals("Unexpected third column.", "goo", partition.getSd().getCols().get(2).getName());
                String partitionLocation = partition.getSd().getLocation();
                String tableLocation = table.getSd().getLocation();
                Assert.assertTrue("Unexpected partition location: " + partitionLocation + ". " +
                                "Partition should have been within table location: " + tableLocation,
                        partitionLocation.startsWith(tableLocation));
            }

        } catch (Throwable t) {
            LOG.error("Unexpected Exception!", t);
            t.printStackTrace();
            Assert.assertTrue("Unexpected Exception!", false);
        }
    }

}
