package com.aliyun.datalake.metastore.common.util;

import com.aliyun.datalake.metastore.common.ProxyMode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;

public class ProxyLogUtils {
    private static final Logger logger = LoggerFactory.getLogger(ProxyLogUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static ProxyLogUtils proxyLogUtils;
    private static final Set<String> ignoreActionLogging = Sets.newHashSet("updateTableColumnStatistics", "isCompatibleWith");
    private static final int DEFAULT_LOG_SIZE = 1024;
    static {
        SimpleModule m = new SimpleModule();
        OBJECT_MAPPER.registerModule(m);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private String logStore;
    private boolean recordActionLog;
    private boolean recordLog;

    public ProxyLogUtils(String logStore, boolean recordActionLog, boolean recordLog) {
        this.logStore = logStore == null ? "default" : logStore;
        this.recordActionLog = recordActionLog;
        this.recordLog = recordLog;
    }

    /**
     * ignore to json exception
     *
     * @param object
     * @return
     */
    public static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            logger.debug(e.getMessage(), e);
            return e.getMessage();
        }
    }

    public static synchronized void initLogUtils(ProxyMode proxyMode, String logStore, boolean recordActionLog, boolean recordLog) {
        if (proxyLogUtils != null) {
            return;
        }

        logger.info("init LogUtils with proxyMode:{}", proxyMode);
        proxyLogUtils = new ProxyLogUtils(logStore, recordActionLog, recordLog);
    }

    public static boolean ignoreLogParameters(String actionName) {
        if (ignoreActionLogging.contains(actionName)) {
            return true;
        }
        return false;
    }

    public static String getActionParametersString(String actionName, Object... parameters) {
        ProxyLogUtils proxyLogUtils = getProxyLogUtils();

        if (proxyLogUtils == null || !proxyLogUtils.recordActionLog) {
            return "";
        }

        if (ignoreLogParameters(actionName)) {
            return "";
        } else {
            //截断处理
            String paramJson = ProxyLogUtils.toJson(parameters);
            return paramJson.length() > DEFAULT_LOG_SIZE ? paramJson.substring(0, DEFAULT_LOG_SIZE) : paramJson;
        }
    }

    public static void setProxyLogUtils(ProxyLogUtils proxyLogUtils) {
        ProxyLogUtils.proxyLogUtils = proxyLogUtils;
    }

    public static ProxyLogUtils getProxyLogUtils() {
        return proxyLogUtils;
    }
    public static void writeLog(Exception e, String actionName, Object... parameters) {
        try {
            ProxyLogUtils proxyLogUtils = getProxyLogUtils();

            if (proxyLogUtils == null) {
                return;
            }

            String message = e.getMessage();
            String stack = getStackTrace(e);

            proxyLogUtils.writeLog2Sls(actionName, message, stack, parameters);
        } catch (Exception e1) {
            logger.error(e1.getMessage(), e1);
        }
    }

    public static String getStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public void writeLog2Sls(String actionName, String message, String stack, Object... parameters) {
        String parameterString = toJson(parameters);
        parameterString = parameterString.length() > DEFAULT_LOG_SIZE ? parameterString.substring(0, DEFAULT_LOG_SIZE) : parameterString;
        logger.warn("dlf_error, uid: {}, actionName: {}, message: {}, stack:{}, parameters: {}", logStore, actionName, message,
                stack, parameterString);
    }

    public static void printLog(Runnable log) {
        ProxyLogUtils proxyUtils = getProxyLogUtils();
        if (proxyUtils.recordLog) {
            log.run();
        }
    }

    public boolean isRecordActionLog() {
        return recordActionLog;
    }

    public boolean isRecordLog() {
        return recordLog;
    }
}