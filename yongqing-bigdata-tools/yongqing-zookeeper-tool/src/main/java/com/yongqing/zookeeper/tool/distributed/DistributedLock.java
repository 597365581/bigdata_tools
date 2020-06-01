package com.yongqing.zookeeper.tool.distributed;

import com.yongqing.zookeeper.tool.exec.Exec;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public interface DistributedLock {
    <T, R> R distributedLockExec(String lockPath, Exec exec, T parameters);
    <T, R> R distributedLockExec(String lockPath, Exec exec, T parameters, Long time, TimeUnit unit);
    <T> void distributedLockExecNoReturn(String lockPath, Exec exec);
    <T> void distributedLockExecNoReturn(String lockPath, Exec exec, T parameters);
    <T, R> R distributedLockExec(String lockPath, Exec exec);
    <T> void distributedLockExecNoReturn(String lockPath, Exec exec,Long time, TimeUnit unit);
    <T, R> R distributedLockExec(String lockPath, Exec exec, Long time, TimeUnit unit);
    <T> void distributedLockExecNoReturn(String lockPath, Exec exec, T parameters, Long time, TimeUnit unit);
}
