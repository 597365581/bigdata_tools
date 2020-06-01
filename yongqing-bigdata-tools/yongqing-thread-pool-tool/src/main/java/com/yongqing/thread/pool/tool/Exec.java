package com.yongqing.thread.pool.tool;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.List;

/**
 *
 */
public interface Exec {
    <O,D,S> ListenableFuture<List<O>> exec(ListeningExecutorService executorService, CallableFunction<O,S> function , List<D> listData,Integer slicesNum) throws Exception;
    ListenableFuture<?> exec(ListeningExecutorService executorService,List<? extends Runnable> threads);
}
