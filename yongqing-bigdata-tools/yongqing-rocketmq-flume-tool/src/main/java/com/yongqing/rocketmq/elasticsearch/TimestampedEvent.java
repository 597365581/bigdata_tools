/*
 *
 */
package com.yongqing.rocketmq.elasticsearch;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.joda.time.DateTimeUtils;

import java.util.Map;

/**
 * {@link org.apache.flume.Event} implementation that has a timestamp.
 * The timestamp is taken from (in order of precedence):<ol>
 * <li>The "timestamp" header of the base event, if present</li>
 * <li>The "@timestamp" header of the base event, if present</li>
 * <li>The current time in millis, otherwise</li>
 * </ol>
 */
final class TimestampedEvent extends SimpleEvent {

  private final long timestamp;

  TimestampedEvent(Event base) {
    setBody(base.getBody());
    Map<String, String> headers = Maps.newHashMap(base.getHeaders());
    String timestampString = headers.get("timestamp");
    if (StringUtils.isBlank(timestampString)) {
      timestampString = headers.get("@timestamp");
    }
    if (StringUtils.isBlank(timestampString)) {
      this.timestamp = DateTimeUtils.currentTimeMillis();
      headers.put("timestamp", String.valueOf(timestamp ));
    } else {
      this.timestamp = Long.valueOf(timestampString);
    }
    setHeaders(headers);
  }

  long getTimestamp() {
    return timestamp;
  }
}
