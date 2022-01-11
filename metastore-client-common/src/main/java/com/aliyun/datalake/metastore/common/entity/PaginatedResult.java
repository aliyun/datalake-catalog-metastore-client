package com.aliyun.datalake.metastore.common.entity;

import java.util.List;

public class PaginatedResult<T> {

    private List<T> data;
    private String nextPageToken;

    public PaginatedResult(List<T> data, String nextPageToken) {
        this.data = data;
        this.nextPageToken = nextPageToken;
    }

    public List<T> getData() {
        return data;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
