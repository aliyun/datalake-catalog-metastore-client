package com.aliyun.datalake.metastore.common.api;

import com.aliyun.datalake.metastore.common.Action;
import com.aliyun.datalake.metastore.common.entity.ResultModel;

public class DataLakeAPIException extends RuntimeException {
    private ResultModel<?> result = null;
    private Action action;

    public DataLakeAPIException(ResultModel<?> result, Action action) {
        this.result = result;
        this.action = action;
    }

    public DataLakeAPIException(String message, Throwable t) {
        super(message, t);
    }

    public DataLakeAPIException(String message) {
        super(message);
    }

    public DataLakeAPIException(Throwable t) {
        super(t);
    }

    public ResultModel<?> getResult() {
        return result;
    }

    public void setResult(ResultModel<?> result) {
        this.result = result;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }
}
