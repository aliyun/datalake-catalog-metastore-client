package com.aliyun.datalake.metastore.common;

import com.aliyun.datalake20200710.external.okhttp3.Call;
import com.aliyun.datalake20200710.external.okhttp3.OkHttpClient;
import com.aliyun.datalake20200710.external.okhttp3.Request;
import com.aliyun.datalake20200710.external.okhttp3.Response;
import com.aliyun.tea.TeaConverter;
import com.aliyun.tea.TeaPair;
import com.aliyun.tea.okhttp.ClientHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.aliyun.datalake.metastore.common.Constant.METASTORE_TOKEN_REFRESHER_NAME_FORMAT;

public class STSHelper {

    private final static Logger logger = LoggerFactory.getLogger(STSHelper.class);

    public static final String STS_REGION = "Region";
    public static final String STS_ACCESS_KEY_ID = "AccessKeyId";
    public static final String STS_ACCESS_KEY_SECRET = "AccessKeySecret";
    public static final String STS_TOKEN = "SecurityToken";
    public static final ConcurrentHashMap<String, OkHttpClient> clients = new ConcurrentHashMap();
    public static Map<String, Object> clientRuntimeOptions = TeaConverter.buildMap(new TeaPair[]{new TeaPair("timeouted", "retry"), new TeaPair("readTimeout", 30 * 1000), new TeaPair("connectTimeout", 30 * 1000), new TeaPair("retry", TeaConverter.buildMap(new TeaPair[]{new TeaPair("retryable", false), new TeaPair("maxAttempts", 3)})), new TeaPair("backoff", TeaConverter.buildMap(new TeaPair[]{new TeaPair("policy", "no"), new TeaPair("period", 1)})), new TeaPair("ignoreSSL", true)});
    private static volatile Long lastVersion = 0L;
    private static volatile Properties stsToken = new Properties();
    private static volatile boolean tokenRefresherInit = false;
    private static ScheduledExecutorService scheduledExecutorService = null;
    private static final int refreshStsTimes = 60 * 1000;

    public static void initSTSHelper(boolean isNewStsMode) {
        if (!tokenRefresherInit) {
            synchronized (STSHelper.class) {
                if (!tokenRefresherInit) {
                    scheduledExecutorService = Executors.newScheduledThreadPool(1,
                            new ThreadFactoryBuilder().setNameFormat(METASTORE_TOKEN_REFRESHER_NAME_FORMAT).setDaemon(true).build());

                    scheduledExecutorService.scheduleWithFixedDelay(() -> {
                        try {
                            getLatestSTSToken(true, isNewStsMode);
                        } catch (Exception exception) {
                            logger.info(exception.getMessage(), exception);
                        }
                    }, 0, refreshStsTimes, TimeUnit.MILLISECONDS);
                    tokenRefresherInit = true;
                }
            }
        }
    }

    public static Properties getLatestSTSToken(boolean force, boolean isNewStsMode) throws Exception {
        Long currentVersion = System.currentTimeMillis();
        if (!force && (currentVersion - lastVersion) <= refreshStsTimes) {
            return stsToken;
        } else {
            synchronized (STSHelper.class) {
                if (force || (currentVersion - lastVersion) > refreshStsTimes) {
                    stsToken = getEMRSTSToken(isNewStsMode);
                    lastVersion = currentVersion;
                }
            }
        }
        return stsToken;
    }

    public static Properties getEMRSTSToken(boolean isNewStsMode) throws Exception {
        logger.debug("dlf: use {} mode to fetch sts token", isNewStsMode ? "new" : "old");

        if (isNewStsMode) {
            return getEMRSTSTokenNew();
        } else {
            return getEMRSTSTokenOld();
        }
    }

    public static Call buildCall(OkHttpClient httpClient, String url) {
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        return httpClient.newCall(request);
    }

    public static Response getResponse(OkHttpClient httpClient, String url) throws Exception {
        Response response = null;
        long startTime = System.currentTimeMillis();
        try {
            response = retryGetResponse(httpClient, url, 0, 3);
        } finally {
            logger.info("sts request for url:{}, cost:{}ms", url, System.currentTimeMillis() - startTime);
        }

        return response;
    }

    public static Response retryGetResponse(OkHttpClient httpClient, String url, int curTimes, int maxTimes) throws Exception {
        Response response = null;
        try {
            Call call = buildCall(httpClient, url);
            response = call.execute();
            checkResponse(response);
        } catch (Exception e) {
            if (response != null) {
                response.close();
            }
            curTimes++;
            if (curTimes < maxTimes) {
                logger.info("retry get ststoken for times:{}", curTimes);
                return retryGetResponse(httpClient, url, curTimes, maxTimes);
            } else {
                logger.error("can't get ststoken afer retry:{} times, due to {}", maxTimes, e.getMessage(), e);
                throw e;
            }
        }
        return response;
    }

    public static void checkResponse(Response response) throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException(response.toString());
        }
    }

    public static Properties getEMRSTSTokenNew() throws Exception {
        Properties result = new Properties();

        OkHttpClient httpClient = getOkHttpClient("100.100.100.200", 80, clientRuntimeOptions); //new OkHttpClient();
        String role = "";

        Response response = getResponse(httpClient, "http://100.100.100.200/latest/meta-data/Ram/security-credentials/");
        role = response.body().string();

        response = getResponse(httpClient, "http://100.100.100.200/latest/meta-data/Ram/security-credentials/" + role);
        String tokens = response.body().string();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(tokens);
        result.put(STS_ACCESS_KEY_ID, root.get("AccessKeyId").asText());
        result.put(STS_ACCESS_KEY_SECRET, root.get("AccessKeySecret").asText());
        result.put(STS_TOKEN, root.get("SecurityToken").asText());

        response = getResponse(httpClient, "http://100.100.100.200/latest/meta-data/region-id");
        result.put(STS_REGION, response.body().string());

        return result;
    }

    public static Properties getEMRSTSTokenOld() throws Exception {
        Properties result = new Properties();
        OkHttpClient httpClient = getOkHttpClient("localhost", 10011, clientRuntimeOptions);

        Response response = getResponse(httpClient, "http://localhost:10011/cluster-region");
        result.put(STS_REGION, response.body().string());

        response = getResponse(httpClient, "http://localhost:10011/role-access-key-id");
        result.put(STS_ACCESS_KEY_ID, response.body().string());

        response = getResponse(httpClient, "http://localhost:10011/role-access-key-secret");
        result.put(STS_ACCESS_KEY_SECRET, response.body().string());

        response = getResponse(httpClient, "http://localhost:10011/role-security-token");
        result.put(STS_TOKEN, response.body().string());

        return result;
    }

    public static Properties getEMRSTSToken(String uid, String role) throws Exception {
        Properties result = new Properties();
        OkHttpClient httpClient = new OkHttpClient();

        Response response = getResponse(httpClient, "http://localhost:10011/cluster-region");
        result.put(STS_REGION, response.body().string());

        response = getResponse(httpClient, String.format("http://localhost:10011/dlf-ak-info?user_id=%s&role=%s", uid, role));
        String resp = response.body().string();
        ObjectMapper mapper = new ObjectMapper();
        Map kv = mapper.readValue(resp, Map.class);
        result.putAll(kv);

        return result;
    }

    public static OkHttpClient getOkHttpClient(String host, int port, Map<String, Object> map) throws Exception {
        String key;
        if (null == map.get("httpProxy") && null == map.get("httpsProxy")) {
            key = ClientHelper.getClientKey(host, port);
        } else {
            Object urlString = null == map.get("httpProxy") ? map.get("httpsProxy") : map.get("httpProxy");
            URL url = new URL(String.valueOf(urlString));
            key = ClientHelper.getClientKey(url.getHost(), url.getPort());
        }

        OkHttpClient client = clients.get(key);
        if (null == client) {
            synchronized (STSHelper.class) {
                client = clients.get(key);
                if (client == null) {
                    client = ClientHelper.creatClient(map);
                    clients.put(key, client);
                }
            }
        }

        return client;
    }
}
