/*
 *
 */
package com.yongqing.rocketmq.elasticsearch;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;

public interface IndexNameBuilder extends Configurable,
        ConfigurableComponent {
  /**
   * Gets the name of the index to use for an index request
   * @param event
   *          Event which determines index name
   * @return index name of the form 'indexPrefix-indexDynamicName'
   */
   String getIndexName(Event event);
  
  /**
   * Gets the prefix of index to use for an index request.
   * @param event
   *          Event which determines index name
   * @return Index prefix name
   */
   String getIndexPrefix(Event event);
}
