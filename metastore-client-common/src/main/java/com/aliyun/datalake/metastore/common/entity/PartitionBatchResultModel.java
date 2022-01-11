package com.aliyun.datalake.metastore.common.entity;

import com.aliyun.datalake20200710.models.PartitionError;
import com.aliyun.tea.NameInMap;

import java.util.List;

/**
 * @param <T> for Data type
 */
public class PartitionBatchResultModel<T> extends BatchResultModel<T> {

    @NameInMap("PartitionErrors")
    public List<PartitionError> errors;

    public PartitionBatchResultModel() {
    }

    public PartitionBatchResultModel(boolean success, String code, String message, String requestId, T data, List<PartitionError> errors) {
        super(success, code, message, requestId, data);
        this.errors = errors;
    }
}
