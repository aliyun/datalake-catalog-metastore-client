package com.aliyun.datalake.metastore.common;

public class RetryableException extends Exception {

    private String errorCode;

    public RetryableException(String message) {
        super(message);
    }


    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(String errorCode, String message, String requestId) {
        super("ErrorCode: " + errorCode + ", Message:" + message + ", RequestId: " + requestId);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return this.errorCode;
    }
}
