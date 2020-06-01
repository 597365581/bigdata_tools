package com.yongqing.log.flume.notice;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface Notice {
    void noticePostLog(String logType);
    void noticePostLog(List<Map<String,Object>> noticeMsg);
}
