package com.aliyun.datalake.metastore.common.entity;

import com.aliyun.tea.NameInMap;
import com.aliyun.tea.TeaModel;

public class ResultModel<T> extends TeaModel {
    @NameInMap("Success")
    public boolean success;

    @NameInMap("Code")
    public String code;

    @NameInMap("Message")
    public String message;

    @NameInMap("RequestId")
    public String requestId;

    @NameInMap("HttpStatusCode")
    public Integer httpStatusCode;

    public T data;

    public ResultModel() {
    }

    public ResultModel(boolean success, String code, String message, String requestId) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.requestId = requestId;
    }

    public ResultModel(boolean success, String code, String message, String requestId, T data) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.requestId = requestId;
        this.data = data;
    }

    public ResultModel(boolean success, String code, String message, String requestId, Integer httpStatusCode, T data) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.requestId = requestId;
        this.httpStatusCode = httpStatusCode;
        this.data = data;
    }

    public static <T> ResultModel<T> successModel(String requestId, T t) {
        return new ResultModel(true, "", "", requestId, t);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResultModel{");
        sb.append("success=").append(success);
        sb.append(", code='").append(code).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", requestId='").append(requestId).append('\'');
        sb.append(", httpStatusCode=").append(httpStatusCode);
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }
}