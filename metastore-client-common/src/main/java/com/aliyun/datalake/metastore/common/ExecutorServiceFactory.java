package com.aliyun.datalake.metastore.common;

import java.util.concurrent.ExecutorService;

public interface ExecutorServiceFactory {
    ExecutorService getExecutorService(int threadNums);
}