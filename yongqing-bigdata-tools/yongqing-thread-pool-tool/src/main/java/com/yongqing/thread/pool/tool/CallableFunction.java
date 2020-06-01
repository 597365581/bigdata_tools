package com.yongqing.thread.pool.tool;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 *
 */
@FunctionalInterface
public interface CallableFunction<O,Q> {
     Callable<O> getCallableObject(Q data);
}
