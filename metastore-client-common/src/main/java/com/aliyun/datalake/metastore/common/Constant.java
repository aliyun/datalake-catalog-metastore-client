package com.aliyun.datalake.metastore.common;

import com.google.common.collect.Sets;

import java.util.Set;

public class Constant {
    public static final Set<String> RETRYABLE_ERROR_CODE = Sets.newHashSet(
            "InternalError",
            "Throttling",
            "Throttling.User",
            "InvalidApi.NotFound",
            "ServiceUnavailable",
            "Throttling.Api",
            "SignatureNonceUsed");

    public static final Set<String> TOKEN_RELATED_ERROR_CODE = Sets.newHashSet(
            "InvalidSecurityToken.Expired",
            "InvalidAccessKeyId.NotFound",
            "InvalidAccessKeyId.Inactive",
            "InvalidSecurityToken.MismatchWithAccessKey",
            "InvalidSecurityToken.Malformed",
            "MissingSecurityToken"
    );
    public static final String METASTORE_DELEGATE_THREAD_POOL_NAME_FORMAT = "dlf-metastore-delegate-%d";
    public static final String METASTORE_TOKEN_REFRESHER_NAME_FORMAT = "dlf-token-refresher-%d";
    public static final String EXECUTOR_FACTORY_CONF = "hive.metastore.executorservice.factory.class";

    static {
        RETRYABLE_ERROR_CODE.addAll(TOKEN_RELATED_ERROR_CODE);
    }
}
