package com.aliyun.datalake.metastore.common.functional;

@FunctionalInterface
public interface ThrowingBiFunction<T1, T2, R, E extends Exception> {
    R apply(T1 t1, T2 t2) throws E;
}