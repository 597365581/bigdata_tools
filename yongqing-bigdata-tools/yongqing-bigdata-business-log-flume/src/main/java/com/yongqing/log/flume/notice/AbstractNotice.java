package com.yongqing.log.flume.notice;

import com.yongqing.common.bigdata.tool.BigDataHttpClient;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.etcd.tools.EtcdUtil;

import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class AbstractNotice implements Notice{

    @Override
    public void noticePostLog(List<Map<String,Object>> noticeMsg) {
        BigDataHttpClient.postJsonData(EtcdUtil.getLocalPropertie("noticeUrl"), GsonUtil.gson.toJson(noticeMsg));
    }
}
