package com.aliyun.datalake.metastore.common.api;

import com.aliyun.datalake.metastore.common.Version;
import com.aliyun.datalake.metastore.common.entity.BatchResultModel;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake.metastore.common.functional.ThrowingBiFunction;
import com.aliyun.datalake20200710.Client;
import com.aliyun.tea.TeaException;
import com.aliyun.tea.TeaModel;
import com.aliyun.teautil.models.RuntimeOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class AbstractBaseApi {

    protected final Client client;

    public AbstractBaseApi(Client client) {
        this.client = client;
    }

    public <M, V extends ResultModel<M>> ResultModel<M> call(Callable<V> c) throws Exception {
        try {
            return c.call();
        } catch (TeaException e) {
            Map<String, Object> data = e.getData();
            if (data != null && data.get("Code") != null) {
                return TeaModel.toModel(data, new ResultModel<M>());
            } else {
                throw e;
            }
        }
    }

    public <M, V extends BatchResultModel<M>> V batchCall(Callable<V> c, V v) throws Exception {
        try {
            return c.call();
        } catch (TeaException e) {
            Map<String, Object> data = e.getData();
            if (data != null && data.get("Code") != null) {
                return TeaModel.toModel(data, v);
            } else {
                throw e;
            }
        }
    }

    public <R extends TeaModel, E extends Exception> TeaModel callWithOptions(ThrowingBiFunction<Map<String, String>, RuntimeOptions, R, E> calls) throws Exception{
        return callWithOptions(calls, null, null);
    }

    public <R extends TeaModel, E extends Exception> TeaModel callWithOptions(ThrowingBiFunction<Map<String, String>, RuntimeOptions, R, E> calls, Map<String, String> headers, RuntimeOptions runtime) throws Exception{
        if (runtime == null) {
            runtime = new RuntimeOptions();
        }
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put("datalake-metastore-client-version", Version.DATALAKE_METASTORE_CLIENT_VERSION);
        return calls.apply(headers, runtime);
    }
}
