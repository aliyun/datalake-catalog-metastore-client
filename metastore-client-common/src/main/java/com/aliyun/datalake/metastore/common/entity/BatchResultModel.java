package com.aliyun.datalake.metastore.common.entity;

/**
 * @param <T> for Data type
 */
public class BatchResultModel<T> extends ResultModel<T> {

    public BatchResultModel() {
    }

    public BatchResultModel(boolean success, String code, String message, String requestId, T data) {
        super(success, code, message, requestId, data);
    }
}
