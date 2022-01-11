package com.aliyun.datalake.metastore.common.functional;

import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FunctionalUtils {
    private static final Logger logger = LoggerFactory.getLogger(FunctionalUtils.class);

    public static <E extends Exception, T> void run(T client, Optional<T> optional,
                                                    boolean isAllowFailure,
                                                    ThrowingConsumer<T, E> consumer,
                                                    String clientType,
                                                    String actionName,
                                                    Object... parameters) throws E {
        consumerWrapper(client, consumer, clientType, actionName, parameters);
        if (optional.isPresent()) {
            run(() -> consumer.accept(optional.get()), isAllowFailure, clientType, actionName, parameters);
        }
    }

    public static <E extends Exception, T, R> R call(T client, Optional<T> optional,
                                                     boolean isAllowFailure,
                                                     ThrowingFunction<T, R, E> consumer,
                                                     String clientType,
                                                     String actionName,
                                                     Object... parameters) throws E {

        R result = functionWrapper(client, consumer, clientType, actionName, parameters);
        if (optional.isPresent()) {
            run(() -> consumer.apply(optional.get()), isAllowFailure, clientType, actionName, parameters);
        }

        return result;
    }

    public static <E extends Exception, T, R> R functionWrapper(T client, ThrowingFunction<T, R, E> consumer,
                                                                String clientType, String actionName, Object... parameters) throws E {
        long startTime = System.currentTimeMillis();
        try {
            return consumer.apply(client);
        } finally {
            long endTime = System.currentTimeMillis();
            ProxyLogUtils.printLog(() -> logger.info("{}.meta.{}, cost:{}ms, parameters: {}", clientType, actionName, endTime - startTime,
                                                     ProxyLogUtils.getActionParametersString(actionName, parameters)));
        }
    }

    public static <E extends Exception, T> void consumerWrapper(T client, ThrowingConsumer<T, E> consumer,
                                                                String clientType, String actionName, Object... parameters) throws E {
        long startTime = System.currentTimeMillis();
        try {
            consumer.accept(client);
        } finally {
            long endTime = System.currentTimeMillis();
            ProxyLogUtils.printLog(() -> logger.info("{}.meta.{}, cost:{}ms, parameters: {}", clientType, actionName, endTime - startTime,
                                                     ProxyLogUtils.getActionParametersString(actionName, parameters)));

        }
    }

    public static <E extends Exception> void run(ThrowingRunnable<E> r, boolean isAllowFailure, String firstClientType, String actionName, Object... parameters) throws E {
        String clientType = "dlf".equalsIgnoreCase(firstClientType) ? "hive" : "dlf";
        long startTime = System.currentTimeMillis();
        try {
            r.run();
        } catch (Exception e) {
            collectLogs(e, actionName, parameters);
            if (!isAllowFailure) {
                throw e;
            }
        } finally {
            long endTime = System.currentTimeMillis();
            ProxyLogUtils.printLog(() -> logger.info("{}.meta.{}, cost:{}ms", clientType, actionName, endTime - startTime));
        }

    }

    public static <E extends Exception, T> void batchedRunnable(List<T> list, int batchSize
            , ThrowingFunction<List<T>, Void, E> consumer, ExecutorService executorService) throws Exception {
        List<Future> partitionFutures = new ArrayList<>();
        for (int fromIndex = 0, toIndex = 0; toIndex < list.size(); fromIndex = toIndex) {
            toIndex = Math.min(fromIndex + batchSize, list.size());
            List<T> subBatch = list.subList(fromIndex, toIndex);
            partitionFutures.add(executorService.submit(() -> consumer.apply(subBatch)));
        }

        for (Future future : partitionFutures) {
            future.get();
        }
    }

    public static <E extends Exception, T, R extends List> void batchedCall(List<T> list, R resultList, int batchSize
            , ThrowingFunction<List<T>, R, E> function, ExecutorService executorService) throws Exception {
        List<Future<R>> partitionFutures = new ArrayList<>();
        for (int fromIndex = 0, toIndex = 0; toIndex < list.size(); fromIndex = toIndex) {
            toIndex = Math.min(fromIndex + batchSize, list.size());
            List<T> subBatch = list.subList(fromIndex, toIndex);
            partitionFutures.add(executorService.submit(() -> function.apply(subBatch)));
        }

        for (Future<R> future : partitionFutures) {
            R batchResult = future.get();
            if (batchResult != null) {
                if (resultList != null) {
                    resultList.addAll(batchResult);
                }
            }
        }
    }

    public static void collectLogs(Exception e, String actionName, Object... parameters) {
        ProxyLogUtils.writeLog(e, actionName, parameters);
    }
}