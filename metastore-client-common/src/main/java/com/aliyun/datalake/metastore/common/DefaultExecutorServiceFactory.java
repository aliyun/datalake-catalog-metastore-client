package com.aliyun.datalake.metastore.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.aliyun.datalake.metastore.common.Constant.METASTORE_DELEGATE_THREAD_POOL_NAME_FORMAT;

public class DefaultExecutorServiceFactory implements ExecutorServiceFactory {
    private static volatile ExecutorService service;

    @Override
    public ExecutorService getExecutorService(int threadNums) {
        if (service != null) {
            return service;
        }

        synchronized (DefaultExecutorServiceFactory.class) {
            if (service == null) {
                service = Executors.newFixedThreadPool(threadNums, new ThreadFactoryBuilder()
                                .setNameFormat(METASTORE_DELEGATE_THREAD_POOL_NAME_FORMAT)
                                .setDaemon(true).build()
                );
            }
        }

        return service;
    }

}