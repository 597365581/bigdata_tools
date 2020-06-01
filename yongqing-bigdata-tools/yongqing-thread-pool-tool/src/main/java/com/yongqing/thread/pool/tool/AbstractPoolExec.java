package com.yongqing.thread.pool.tool;


import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 *
 */
public class AbstractPoolExec implements PoolExec {
    protected ExecutorService executorService;
    private ListeningExecutorService listeningExecutorService;
    protected int poolSize=4;

    protected  ExecutorService initExecutorService(){
        synchronized (this){
            if(null ==executorService || executorService.isShutdown()){
                executorService = Executors.newFixedThreadPool(poolSize);
            }
        }
        return executorService;
    }
    protected ListeningExecutorService initListeningExecutorService(){
        synchronized (this){
            if(null == listeningExecutorService || listeningExecutorService.isShutdown()){
                listeningExecutorService = MoreExecutors.listeningDecorator(initExecutorService());
            }
        }
        return listeningExecutorService;
    }
    public <O,D>ListenableFuture<List<O>> exec(List<CallableFunction<O,D>> functionList ,D data) throws Exception{
        return PoolExec.super.exec(initListeningExecutorService(),functionList,data);
    }
    public <O,D,S> ListenableFuture<List<O>> exec(List<CallableFunction<O,S>> functionList ,D data,FutureFunction<ListeningExecutorService,List<ListenableFuture<O>>,List<CallableFunction<O,S>> ,D> futureFunction) throws Exception{
        return PoolExec.super.exec(initListeningExecutorService(),functionList,data,futureFunction);
    }
    public <O,D> ListenableFuture<List<O>> exec(CallableFunction<O,D> function , List<D> data) throws Exception {
        return PoolExec.super.exec(initListeningExecutorService(),function,data,(Integer)null);
    }
    public <O, D> ListenableFuture<List<O>> exec( CallableFunction<O, List<D>> function, List<D> listData, Integer i) throws Exception{
        return PoolExec.super.exec(initListeningExecutorService(),function,listData,i);
    }
    public  <O,S> ListenableFuture<List<O>> exec( Map<CallableFunction<O,S>,List<?>> map) throws Exception{
        return PoolExec.super.exec(initListeningExecutorService(),map);
    }
    public <O,D,S> ListenableFuture<List<O>> exec( CallableFunction<O,S> function , Map<? extends Consumer<List<D>>,List<D>>[] consumersList) throws Exception{
        return PoolExec.super.exec(initListeningExecutorService(),function,Arrays.asList(consumersList),(Integer)null);
    }
    public ListenableFuture<?> exec(List<Runnable> threads){
       return PoolExec.super.exec(initListeningExecutorService(),threads);
    }
    @Override
    public void shutdown(){
        synchronized (this){
            if(null != executorService && !executorService.isShutdown()){
                executorService.shutdown();
            }
            if(null != listeningExecutorService && !listeningExecutorService.isShutdown()){
                listeningExecutorService.shutdown();
            }
        }
    }
    @Override
    public void shutdownNow(){
        synchronized (this){
            if(null != executorService && !executorService.isShutdown()){
                executorService.shutdownNow();
            }
            if(null != listeningExecutorService && !listeningExecutorService.isShutdown()){
                listeningExecutorService.shutdownNow();
            }
        }
    }
}
