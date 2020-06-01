package com.yongqing.thread.pool.tool;


/**
 *
 */
@FunctionalInterface
public interface FutureFunction<E,Q,L,D> {
    void futuresAdd(E e,Q q,L l, D data);
}
