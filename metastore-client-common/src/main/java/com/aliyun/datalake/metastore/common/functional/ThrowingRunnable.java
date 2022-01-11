package com.aliyun.datalake.metastore.common.functional;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {
    void run() throws E;
}