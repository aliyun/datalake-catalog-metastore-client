package com.aliyun.datalake.metastore.hive2;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionTest extends BaseTest {
    public static String DEFAULT_DB = "default";
    public static String TEST_DB = TestUtil.TEST_DB;
    private static IMetaStoreClient client;
    private static Database testDb;

    @BeforeClass
    public static void setUp() throws TException, IOException {
        HiveConf conf = new HiveConf();
        client = TestUtil.getDlfClient();
        // client = TestUtil.getHMSClient();

        cleanUpDatabase(DEFAULT_DB);
        cleanUpDatabase(TEST_DB);
        testDb = TestUtil.getDatabase(TEST_DB);
        try {
            client.dropDatabase(testDb.getName(), true, true, true);
        } catch (NoSuchObjectException e) {
        }
        client.createDatabase(testDb);
    }

    @AfterClass
    public static void cleanUp() throws TException {
        try {
            client.dropDatabase(testDb.getName(), true, true, true);
        } catch (NoSuchObjectException e) {

        }
    }

    private static void cleanUpDatabase(String dbName) throws TException {
        List<String> allFunctions = client.getFunctions(dbName, "*");
        allFunctions.forEach(f -> {
            try {
                client.dropFunction(dbName, f);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @After
    public void cleanUpCase() throws TException {
        cleanUpDatabase(DEFAULT_DB);
        cleanUpDatabase(TEST_DB);
    }

    @Test
    public void testCreateFunction() throws TException {
        String funcName = "test_func";
        String className = "com.aliyun.datalake.TestFunc";
        Function function = TestUtil.getFunction(TEST_DB, funcName, className);

        // create the function
        client.createFunction(function);

        // check the created function
        Function check = client.getFunction(TEST_DB, funcName);
        Assert.assertEquals(check.getDbName(), TEST_DB);
        Assert.assertEquals(check.getFunctionName(), funcName);
        Assert.assertEquals(check.getClassName(), className);
        Assert.assertEquals(check.getFunctionType(), FunctionType.JAVA);
    }

    @Test
    public void testAlterFunction() throws TException {
        String funcName = "test_func";
        String className = "com.aliyun.datalake.TestFunc";
        Function function = TestUtil.getFunction(TEST_DB, funcName, className);

        // create the function
        client.createFunction(function);

        // alter function: change the classname
        String newClass = "com.aliyun.datalake.TestFuncNew";
        Function newFunc = TestUtil.getFunction(TEST_DB, funcName, newClass);
        client.alterFunction(TEST_DB, funcName, newFunc);

        // check the new function
        Function check = client.getFunction(TEST_DB, funcName);
        Assert.assertEquals(check.getDbName(), TEST_DB);
        Assert.assertEquals(check.getFunctionName(), funcName);
        Assert.assertEquals(check.getClassName(), newClass);
        Assert.assertEquals(check.getFunctionType(), FunctionType.JAVA);
    }

    @Test
    public void testAlterFunctionRename() throws TException {
        String funcName = "test_func";
        String className = "com.aliyun.datalake.TestFunc";
        Function function = TestUtil.getFunction(TEST_DB, funcName, className);

        // create the function
        client.createFunction(function);

        // alter function: rename
        String renamedFunc = "test_func_renamed";
        Function renamed = TestUtil.getFunction(TEST_DB, renamedFunc, className);
        client.alterFunction(TEST_DB, funcName, renamed);

        // check the renamed function
        Function check = client.getFunction(TEST_DB, renamedFunc);
        Assert.assertEquals(check.getClassName(), className);

        // original function cannot be got
        Assert.assertThrows("does not exist", NoSuchObjectException.class,
                () -> client.getFunction(TEST_DB, funcName));
    }

    @Test
    public void testAlterNonExistFunction() throws TException {
        String funcName = "test_func";
        String className = "com.aliyun.datalake.TestFunc";
        Function function = TestUtil.getFunction(TEST_DB, funcName, className);

        // alter a non exist func
        Assert.assertThrows("Function not found", NoSuchObjectException.class,
                () -> client.alterFunction("nonExistDb", funcName, function));
        Assert.assertThrows("Function not found", NoSuchObjectException.class,
                () -> client.alterFunction(TEST_DB, funcName, function));
    }

    @Test
    public void testGetFunction() throws TException {
        String funcName = "test_func";
        String className = "com.aliyun.datalake.TestFunc";
        Function function = TestUtil.getFunction(TEST_DB, funcName, className);

        // create the function
        client.createFunction(function);

        // get the created function
        Function check = client.getFunction(TEST_DB, funcName);
        Assert.assertEquals(check.getDbName(), TEST_DB);
        Assert.assertEquals(check.getFunctionName(), funcName);
    }

    @Test
    public void getNonExistFunction() throws TException {
        // get non exist function
        Assert.assertThrows("does not exist", NoSuchObjectException.class,
                () -> client.getFunction("non_exist_db", "non_exist_func"));
        Assert.assertThrows("does not exist", NoSuchObjectException.class,
                () -> client.getFunction(TEST_DB, "non_exist_func"));
    }

    @Test
    public void testGetFunctions() throws TException {
        String func1 = "test_func1";
        String func2 = "test_func2";
        String func3 = "func3";
        String className = "com.aliyun.datalake.TestFunc";
        Function function1 = TestUtil.getFunction(TEST_DB, func1, className);
        Function function2 = TestUtil.getFunction(TEST_DB, func2, className);
        Function function3 = TestUtil.getFunction(TEST_DB, func3, className);

        // create the functions
        client.createFunction(function1);
        client.createFunction(function2);
        client.createFunction(function3);

        // get the functions those match the pattern
        String pattern = "test_*";
        List<String> check = client.getFunctions(TEST_DB, pattern);
        check.sort(String::compareTo);
        List<String> expected = ImmutableList.of(func1, func2);
        Assert.assertEquals(expected, check);

        // test exact pattern
        String exactPattern = "test_.unc.";
        check = client.getFunctions(TEST_DB, exactPattern);
        check.sort(String::compareTo);
        expected = ImmutableList.of(func1, func2);
        Assert.assertEquals(expected, check);
        // test exact pattern
        exactPattern = "Func.";
        check = client.getFunctions(TEST_DB, exactPattern);
        check.sort(String::compareTo);
        expected = ImmutableList.of(func3);
        Assert.assertEquals(expected, check);
        // test exact pattern
        exactPattern = "Func3.*";
        check = client.getFunctions(TEST_DB, exactPattern);
        check.sort(String::compareTo);
        expected = ImmutableList.of(func3);
        Assert.assertEquals(expected, check);
        // test sub pattern
        check = client.getFunctions(TEST_DB, "test_.unc.|Func.|func3");
        check.sort(String::compareTo);
        expected = ImmutableList.of(func3, func1, func2);
        Assert.assertEquals(expected, check);
        //get other pattern
        List<String> otherPattern = client.getFunctions(TEST_DB, "_0sr*9sdf0_erDn.");
        Assert.assertTrue(otherPattern.size() == 0);
    }

    @Test
    public void testGetAllFunctionsResponse() throws TException {
        String func1 = "test_func1";
        String func2 = "test_func2";
        String func3 = "func3";
        String func4 = "test_func4";
        String className = "com.aliyun.datalake.TestFunc";
        Function function1 = TestUtil.getFunction(TEST_DB, func1, className);
        Function function2 = TestUtil.getFunction(TEST_DB, func2, className);
        Function function3 = TestUtil.getFunction(TEST_DB, func3, className);
        Function function4 = TestUtil.getFunction(DEFAULT_DB, func4, className);

        // create the functions
        client.createFunction(function1);
        client.createFunction(function2);
        client.createFunction(function3);
        client.createFunction(function4);
        // get all functions
        GetAllFunctionsResponse check = client.getAllFunctions();
        List<String> expected = ImmutableList.of(function1, function2, function3, function4).stream()
                .map(Function::getFunctionName)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
        List<String> result = check.getFunctions().stream()
                .map(Function::getFunctionName)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
        Assert.assertTrue(result.containsAll(expected));
    }

    @Test
    public void testDropFunctions() throws TException {
        String func1 = "testDeleteFunctions";
        String className = "com.aliyun.datalake.TestFunc";
        Function function1 = TestUtil.getFunction(TEST_DB, func1, className);
        client.createFunction(function1);
        Assert.assertTrue(client.getFunction(TEST_DB, func1) != null);
        client.dropFunction(TEST_DB, func1);
        Assert.assertThrows(NoSuchObjectException.class, () -> client.getFunction(TEST_DB, func1));
    }

    @Test
    public void testSimpleFunction() throws Exception {
        String dbName = TEST_DB;
        String funcName = "test_func";
        String className = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
        String owner = "test_owner";
        final int N_FUNCTIONS = 5;
        PrincipalType ownerType = PrincipalType.USER;
        int createTime = (int) (System.currentTimeMillis() / 1000);
        FunctionType funcType = FunctionType.JAVA;

        try {
            for (int i = 0; i < N_FUNCTIONS; i++) {
                createFunction(dbName, funcName + "_" + i, className, owner, ownerType, createTime, funcType, null);
            }

            // Try the different getters

            // getFunction()
            Function func = client.getFunction(dbName, funcName + "_0");
            Assert.assertEquals("function db name", dbName, func.getDbName());
            Assert.assertEquals("function name", funcName + "_0", func.getFunctionName());
            Assert.assertEquals("function class name", className, func.getClassName());
            Assert.assertEquals("function owner name", owner, func.getOwnerName());
            Assert.assertEquals("function owner type", PrincipalType.USER, func.getOwnerType());
            Assert.assertEquals("function type", funcType, func.getFunctionType());
            List<ResourceUri> resources = func.getResourceUris();
            Assert.assertTrue("function resources", resources == null || resources.size() == 0);

            boolean gotException = false;
            try {
                func = client.getFunction(dbName, "nonexistent_func");
            } catch (NoSuchObjectException e) {
                // expected failure
                gotException = true;
            }
            Assert.assertEquals(true, gotException);

            // getAllFunctions()
            GetAllFunctionsResponse response = client.getAllFunctions();
            List<Function> allFunctions = response.getFunctions().stream().filter(fuc -> fuc.getDbName().equals(dbName)).collect(Collectors.toList());
            Assert.assertEquals(N_FUNCTIONS, allFunctions.size());
            Assert.assertEquals(funcName + "_3", allFunctions.get(3).getFunctionName());

            // getFunctions()
            List<String> funcs = client.getFunctions(dbName, "*_func_*");
            Assert.assertEquals(N_FUNCTIONS, funcs.size());
            Assert.assertEquals(funcName + "_0", funcs.get(0));

            funcs = client.getFunctions(dbName, "nonexistent_func");
            Assert.assertEquals(0, funcs.size());

            // dropFunction()
            for (int i = 0; i < N_FUNCTIONS; i++) {
                client.dropFunction(dbName, funcName + "_" + i);
            }

            // Confirm that the function is now gone
            funcs = client.getFunctions(dbName, funcName);
            Assert.assertEquals(0, funcs.size());
            response = client.getAllFunctions();
            allFunctions = response.getFunctions().stream().filter(f -> f.getDbName().equals(dbName)).collect(Collectors.toList());
            Assert.assertEquals(0, allFunctions.size());
        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testConcurrentMetastores() failed.");
            throw e;
        }
    }

    @Test
    public void testFunctionWithResources() throws Exception {
        String funcName = "test_func";
        String className = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
        String owner = "test_owner";
        PrincipalType ownerType = PrincipalType.USER;
        int createTime = (int) (System.currentTimeMillis() / 1000);
        FunctionType funcType = FunctionType.JAVA;
        List<ResourceUri> resList = new ArrayList<ResourceUri>();
        resList.add(new ResourceUri(ResourceType.JAR, TestUtil.WAREHOUSE_PATH + "jar1.jar"));
        resList.add(new ResourceUri(ResourceType.FILE, TestUtil.WAREHOUSE_PATH + "file1.txt"));
        resList.add(new ResourceUri(ResourceType.ARCHIVE, TestUtil.WAREHOUSE_PATH + "archive1.tgz"));

        try {
            createFunction(TEST_DB, funcName, className, owner, ownerType, createTime, funcType, resList);

            // Try the different getters

            // getFunction()
            Function func = client.getFunction(TEST_DB, funcName);
            Assert.assertEquals("function db name", TEST_DB, func.getDbName());
            Assert.assertEquals("function name", funcName, func.getFunctionName());
            Assert.assertEquals("function class name", className, func.getClassName());
            Assert.assertEquals("function owner name", owner, func.getOwnerName());
            Assert.assertEquals("function owner type", PrincipalType.USER, func.getOwnerType());
            Assert.assertEquals("function type", funcType, func.getFunctionType());
            List<ResourceUri> resources = func.getResourceUris();
            Assert.assertEquals("Resource list size", resList.size(), resources.size());
            for (ResourceUri res : resources) {
                Assert.assertTrue("Matching resource " + res.getResourceType() + " " + res.getUri(),
                        resList.indexOf(res) >= 0);
            }
            client.dropFunction(TEST_DB, funcName);
        } catch (Exception e) {
            System.err.println(StringUtils.stringifyException(e));
            System.err.println("testConcurrentMetastores() failed.");
            throw e;
        }
    }

    private void createFunction(String dbName, String funcName, String className,
                                String ownerName, PrincipalType ownerType, int createTime,
                                org.apache.hadoop.hive.metastore.api.FunctionType functionType, List<ResourceUri> resources)
            throws Exception {
        Function func = new Function(funcName, dbName, className,
                ownerName, ownerType, createTime, functionType, resources);
        client.createFunction(func);
    }
}
