package com.aliyun.datalake.metastore.common.util;

import com.aliyun.datalake20200710.external.okhttp3.Call;
import com.aliyun.datalake20200710.external.okhttp3.OkHttpClient;
import com.aliyun.datalake20200710.external.okhttp3.Request;
import com.aliyun.datalake20200710.external.okhttp3.Response;
import com.aliyun.datalake.metastore.common.entity.StsTokenInfo;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CupidAkUtils {
    private static final Logger logger = LoggerFactory.getLogger(CupidAkUtils.class);
    private static final int SC_OK = 200;
    private static final Gson GSON = new Gson();
    private static final String STS_TOKEN_REQ_URL_FORMAT =
            "http://localhost:10011/sts-token-info?user_id=%s&role=%s";

    private static String httpGet(String url) {
        OkHttpClient httpClient = new OkHttpClient();
        Request request;
        Response response;
        Call call;

        try {
            request = new Request.Builder()
                    .url(url)
                    .get()
                    .build();

            call = httpClient.newCall(request);
            response = call.execute();

            if (response.code() == SC_OK) {
                return response.body().string();
            }
        } catch (Exception ex) {
            logger.warn(String.format("httpGet url: %s failed.", url), ex);
        }

        return null;
    }

    public static StsTokenInfo fetchStsToken(String userId, String role) {
        String url = String.format(STS_TOKEN_REQ_URL_FORMAT, userId, role);
        String stsTokenRes = httpGet(url);
        assert stsTokenRes != null;

        Map<String, String> kvMap = GSON.fromJson(stsTokenRes, Map.class);
        String accessKeyId = kvMap.get("AccessKeyId");
        String accessKeySecret = kvMap.get("AccessKeySecret");
        String securityToken = kvMap.get("SecurityToken");
        assert accessKeyId != null &&
                accessKeySecret != null &&
                securityToken != null;

        return new StsTokenInfo(
                accessKeyId,
                accessKeySecret,
                securityToken);
    }
}
