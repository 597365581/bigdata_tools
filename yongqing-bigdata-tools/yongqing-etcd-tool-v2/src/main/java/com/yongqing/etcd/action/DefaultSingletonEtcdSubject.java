package com.yongqing.etcd.action;

/**
 *
 */
public class DefaultSingletonEtcdSubject {

    private DefaultSingletonEtcdSubject(){}

    public static class SingletonEtcdSubject{
        public static EtcdSubject etcdSubject = new EtcdSubject();
    }
    public static EtcdSubject getDefaultSingletonEtcdSubject(){
        return SingletonEtcdSubject.etcdSubject;
    }
}
