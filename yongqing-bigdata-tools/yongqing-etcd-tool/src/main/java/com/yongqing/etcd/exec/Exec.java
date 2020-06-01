package com.yongqing.etcd.exec;

/**
 *  分布式执行
 */
@FunctionalInterface
public interface Exec {
    <T,R> R  exec(T parameters);
}
