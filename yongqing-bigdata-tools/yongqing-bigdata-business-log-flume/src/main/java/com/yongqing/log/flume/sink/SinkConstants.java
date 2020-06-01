package com.yongqing.log.flume.sink;

import com.google.gson.reflect.TypeToken;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.processor.log.Processor;
import com.yongqing.processor.log.bean.ProcessorBean;

import java.util.List;
import java.util.Properties;

/**
 *
 */
public class SinkConstants implements Action {

    public static final String BATCH_SIZE = "batchSize";
    public static final String LOG_TYPE = "logType";
    public static final String KAFKA_TOPIC = "kafkaTopic";
    public static final String KAFKA_PARTITION_NUMBER = "kafkaPartitionNumber";
    public static final String ETCD_CONFIG = "etcdConfig";
    public static volatile List<ProcessorBean> processorBeanList;

    @Override
    public void doAction(Properties oldProp, Properties newProp) {
        synchronized (SinkConstants.class) {
            if ((null == oldProp.getProperty("processors") && null != newProp.getProperty("processors")) || (null != oldProp.getProperty("processors") && null != newProp.getProperty("processors") && !oldProp.getProperty("processors").equals(newProp.getProperty("processors")))) {
                processorBeanList = GsonUtil.gson.fromJson(EtcdUtil.getLocalPropertie("processors"), new TypeToken<List<ProcessorBean>>() {
                }.getType());
                processorBeanList.forEach(processorBean -> {
                    try {
                        Processor<?> processor = (Processor<?>) Class.forName(processorBean.getProcessor()).newInstance();
                        processorBean.setProcessorInstance(processor);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                });
            } else if (null != oldProp.getProperty("processors") && null == newProp.getProperty("processors")) {
                if (null != processorBeanList) {
                    processorBeanList.clear();
                    processorBeanList = null;
                }
            }
        }
    }
}
