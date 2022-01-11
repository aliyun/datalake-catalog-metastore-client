package com.aliyun.datalake.metastore.hive2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class MetaConfTest {
    private static final Logger LOG = LoggerFactory.getLogger(MetaConfTest.class);
    private static HiveConf conf;
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private ProxyMetaStoreClient hmsc;

    @AfterClass
    public static void tearDown() throws Exception {
        LOG.info("Shutting down metastore.");
    }

    @BeforeClass
    public static void startMetaStoreServer() throws Exception {

        conf = new HiveConf();
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL_DDL, false);
//        int msPort = MetaStoreUtils.startMetaStore(metastoreConf);
//        conf = MetastoreConf.newMetastoreConf();
//        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + msPort);
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.THRIFT_CONNECTION_RETRIES, 10);
    }

    @Before
    public void setup() throws MetaException {
        hmsc = new ProxyMetaStoreClient(conf, null,false);
    }

    @After
    public void closeClient() {
        if (hmsc != null) {
            hmsc.close();
        }
    }

    @Test
    public void testGetMetaConfDefault() throws TException {
        MetastoreConf.ConfVars metaConfVar = MetastoreConf.ConfVars.TRY_DIRECT_SQL;
        String expected = metaConfVar.getDefaultVal().toString();
        String actual = hmsc.getMetaConf(metaConfVar.toString());
        assertEquals(expected, actual);
    }

    @Test
    public void testGetMetaConfDefaultEmptyString() throws TException {
        MetastoreConf.ConfVars metaConfVar = MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN;
        String expected = "";
        String actual = hmsc.getMetaConf(metaConfVar.toString());
        assertEquals(expected, actual);
    }

    @Test
    public void testGetMetaConfOverridden() throws TException {
        MetastoreConf.ConfVars metaConfVar = MetastoreConf.ConfVars.TRY_DIRECT_SQL_DDL;
        String expected = "false";
        String actual = hmsc.getMetaConf(metaConfVar.toString());
        assertEquals(expected, actual);
    }

    @Test
    public void testGetMetaConfUnknownPreperty() throws TException {
        String unknownPropertyName = "hive.meta.foo.bar";
        Assert.assertThrows(MetaException.class, () -> hmsc.getMetaConf(unknownPropertyName));
    }
}
