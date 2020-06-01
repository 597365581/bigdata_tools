package com.yongqing.etcd.action;

import java.util.Properties;

/**
 *在etcd配置发生变化后，需要去触发的操作。
 */
public interface Action {
    /**
     *   需要执行的操作
     * @param oldProp  老的配置属性
     * @param newProp  新的配置属性
     */
    void doAction(Properties oldProp, Properties newProp);
}
