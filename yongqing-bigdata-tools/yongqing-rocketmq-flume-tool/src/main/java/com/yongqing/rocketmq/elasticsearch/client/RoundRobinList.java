package com.yongqing.rocketmq.elasticsearch.client;

import java.util.Collection;
import java.util.Iterator;

/*
 *
 */

public class RoundRobinList<T> {

  private Iterator<T> iterator;
  private final Collection<T> elements;

  public RoundRobinList(Collection<T> elements) {
    this.elements = elements;
    iterator = this.elements.iterator();
  }

  public synchronized T get() {
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      iterator = elements.iterator();
      return iterator.next();
    }
  }

  public int size() {
    return elements.size();
  }
}
