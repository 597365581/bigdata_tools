package com.yongqing.zookeeper.tool.exec;

/**
 *
 */
@FunctionalInterface
public interface Exec {
    <T,R> R  exec(T parameters);
}
