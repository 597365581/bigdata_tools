package com.yongqing.etcd.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class EtcdSubject {

    private List<Observer> observerList = new ArrayList<>();
    private Properties oldProp;
    private Properties newProp;

    public Properties getNewProperties() {
        return newProp;
    }

    public Properties getOldProperties() {
        return oldProp;
    }

    public void setProperties(Properties oldProp, Properties newProp) {
        this.oldProp = oldProp;
        this.newProp = newProp;
        notifyAllObservers(oldProp, newProp);
    }

    public void addObserver(Observer observer) {
        observerList.add(observer);
    }

    public void notifyAllObservers(Properties oldProp, Properties newProp) {
        for (Observer observer : observerList) {
            observer.doAction(oldProp, newProp);
        }
    }
}
