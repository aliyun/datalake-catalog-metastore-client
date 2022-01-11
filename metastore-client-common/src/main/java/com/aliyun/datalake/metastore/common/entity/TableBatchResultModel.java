package com.aliyun.datalake.metastore.common.entity;

import com.aliyun.datalake20200710.models.TableError;
import com.aliyun.tea.NameInMap;

import java.util.List;

/**
 * @param <T> for Data type
 */
public class TableBatchResultModel<T> extends BatchResultModel<T> {

    @NameInMap("TableErrors")
    public List<TableError> errors;

    public TableBatchResultModel(boolean success, String code, String message, String requestId, T data, List<TableError> errors) {
        super(success, code, message, requestId, data);
        this.errors = errors;
    }
}
