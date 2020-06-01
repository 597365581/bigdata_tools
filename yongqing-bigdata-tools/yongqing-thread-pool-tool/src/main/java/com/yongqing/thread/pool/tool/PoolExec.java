package com.yongqing.thread.pool.tool;


import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.yongqing.common.bigdata.tool.ListUtil;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 *
 */
public interface PoolExec extends Exec {
    void shutdown();
    void shutdownNow();
    default <O,D,S> ListenableFuture<List<O>> exec(ListeningExecutorService executorService, List<CallableFunction<O,S>> functionList ,D data) throws Exception {
       return exec(executorService,functionList,data, (listeningExecutorService, listenableFutures, callableFunctions, data1) -> callableFunctions.forEach(callableFunction->{
           listenableFutures.add(listeningExecutorService.submit(callableFunction.getCallableObject((S) data1)));
       }));
    }
    default <O,D,S> ListenableFuture<List<O>> exec(ListeningExecutorService executorService, List<CallableFunction<O,S>> functionList ,D data,FutureFunction<ListeningExecutorService,List<ListenableFuture<O>>,List<CallableFunction<O,S>> ,D> futureFunction) throws Exception{
        List<ListenableFuture<O>>  futures = Lists.newArrayList();
        futureFunction.futuresAdd(executorService,futures,functionList,data);
        final ListenableFuture<List<O>> resultsFuture = Futures.successfulAsList(futures);
        try {//所有都执行完毕
            resultsFuture.get();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return resultsFuture;
    }
    default <O,D,S> ListenableFuture<List<O>> exec(ListeningExecutorService executorService, CallableFunction<O,S> function , List<D> listData,Integer slicesNum) throws Exception {
        List<ListenableFuture<O>>  futures = Lists.newArrayList();
        if(null!= listData && listData.size()>0 ){
            if (null!=slicesNum && slicesNum>0) {
                for (List list : ListUtil.averageAssign(listData, slicesNum)) {
                    futures.add(executorService.submit(function.getCallableObject((S) list)));
                }
            }
            else {
                for (D data:listData){
                    futures.add(executorService.submit(function.getCallableObject((S) data)));
                }
            }
        }
        final ListenableFuture<List<O>> resultsFuture = Futures.successfulAsList(futures);
        try {//所有都执行完毕
            resultsFuture.get();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return resultsFuture;
    }
    default ListenableFuture<?> exec(ListeningExecutorService executorService,List<? extends Runnable> threads){
        List<ListenableFuture<?>>  futures = Lists.newArrayList();
        if(null!= threads && threads.size()>0){
            for (Runnable thread:threads){
                futures.add(executorService.submit(thread));
            }
        }
        final ListenableFuture<?> resultsFuture = Futures.successfulAsList(futures);
        try {//所有都执行完毕
            resultsFuture.get();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return resultsFuture;
    }
    default <O,D,S> ListenableFuture<List<O>> exec(ListeningExecutorService executorService, CallableFunction<O,S> function , List<Map<? extends Consumer<List<D>>,List<D>>> consumersList) throws Exception{
        return exec(executorService,function,consumersList, (Integer) null);
    }
    default <O,S> ListenableFuture<List<O>> exec(ListeningExecutorService executorService, Map<CallableFunction<O,S>,List<?>> map) throws Exception{
        List<ListenableFuture<O>>  futures = Lists.newArrayList();
        if(null!= map && map.size()>0 ){
            map.forEach((k,v)->{
                futures.add(executorService.submit(k.getCallableObject((S) v)));
            });
        }
        final ListenableFuture<List<O>> resultsFuture = Futures.successfulAsList(futures);
        try {//所有都执行完毕
            resultsFuture.get();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return resultsFuture;
    }
//    default <T>ListenableFuture<T> exec(ListeningExecutorService executorService,List<? extends Runnable> threads,T result){
//        List<ListenableFuture<T>>  futures = Lists.newArrayList();
//        if(null!= threads && threads.size()>0){
//            for (Runnable thread:threads){
//                futures.add(executorService.submit(thread,result));
//            }
//        }
//        final ListenableFuture<T> resultsFuture =(ListenableFuture<T>) Futures.successfulAsList(futures);
//        try {//所有都执行完毕
//            resultsFuture.get();
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
//        return resultsFuture;
//
//    }
}
