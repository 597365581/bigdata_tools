package com.yongqing.etcd.action;

/**
 *
 */
public abstract class Observer implements Action {
    protected EtcdSubject etcdSubject;

    public Observer(EtcdSubject etcdSubject) {
        this.etcdSubject = etcdSubject;
        this.etcdSubject.addObserver(this);
    }
    public Observer(){
        this.etcdSubject = DefaultSingletonEtcdSubject.getDefaultSingletonEtcdSubject();
        this.etcdSubject.addObserver(this);
    }
}
