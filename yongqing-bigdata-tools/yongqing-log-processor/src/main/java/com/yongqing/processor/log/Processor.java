package com.yongqing.processor.log;

/**
 *
 */
public interface Processor<T> {
    T process(T log);
}
