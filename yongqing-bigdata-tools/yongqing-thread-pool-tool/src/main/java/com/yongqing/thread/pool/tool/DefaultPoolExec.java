package com.yongqing.thread.pool.tool;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class DefaultPoolExec extends AbstractPoolExec{
    public DefaultPoolExec(){
        super();
    }
    public DefaultPoolExec(int poolSize){
        this.poolSize = poolSize;
    }
    public DefaultPoolExec(ExecutorService executorService){
        this.executorService = executorService;
    }
}
