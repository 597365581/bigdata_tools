package com.yongqing.log.flume.sink.log.process;

import com.google.gson.reflect.TypeToken;
import com.yongqing.log.flume.notice.AbstractNotice;
import com.yongqing.common.bigdata.tool.DateCommonUtil;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.common.bigdata.tool.UUIDGenerator;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.log.flume.sink.bean.DataBaseConfig;
import com.yongqing.processor.log.bean.BusinessLog;
import com.yongqing.processor.log.bean.CrawlerLog;
import com.yongqing.processor.log.bean.NginxLog;
import com.yongqing.processor.log.bean.ProcessorBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.util.*;

import static com.yongqing.common.bigdata.tool.GsonUtil.gson;
import static com.yongqing.log.flume.sink.SinkConstants.processorBeanList;

/**
 *
 */
public abstract class AbstractEventTo extends AbstractNotice implements EventTo {
    private static final Logger logger = LoggerFactory
            .getLogger(AbstractEventTo.class);
    protected List<Object> logList;

    public void execute(String logType, EventExec eventExec) {
        List<Object> logs;
        synchronized (logList) {
            logs = logList;
            logs.forEach(log -> {
                List<DataBaseConfig> dataBaseConfigList = gson.fromJson(EtcdUtil.getLocalPropertie("dataBaseConfig"), new TypeToken<List<DataBaseConfig>>() {
                }.getType());
                for (DataBaseConfig dataBaseConfig : dataBaseConfigList) {
                    String docId = null;
                    Map<String, Object> customizeField = null;
                    String businessId = null;
                    switch (logType) {
                        case "2":
                            businessId = ((BusinessLog) log).getBusinessId();
                            docId = (String) ((BusinessLog) log).getCustomizeField().get(dataBaseConfig.getPrimaryKeyField());
                            customizeField = ((BusinessLog) log).getCustomizeField();
                            break;
                        case "5":
                            businessId = ((CrawlerLog) log).getBusinessId();
                            docId = (String) ((CrawlerLog) log).getCustomizeField().get(dataBaseConfig.getPrimaryKeyField());
                            customizeField = ((CrawlerLog) log).getCustomizeField();
                            break;
                        case "3":
                            businessId = ((NginxLog) log).getBusinessId();
                            docId = (String) ((NginxLog) log).getCustomizeField().get(dataBaseConfig.getPrimaryKeyField());
                            customizeField = ((NginxLog) log).getCustomizeField();
                            break;
                        default:
                            break;
                    }
                    //从etcd配置中获取哪些字段不需要入库。
                    if (null != EtcdUtil.getLocalPropertie("publicFilterFields")) {
                        for (String field : EtcdUtil.getLocalPropertie("publicFilterFields").split(":")) {
                            if (null != customizeField && customizeField.containsKey(field)) {
                                customizeField.remove(field);
                            }
                        }
                    }
                    if (StringUtils.isBlank(docId)) {
                        docId = UUIDGenerator.getUUID();
                    }
                    try {
                        if (businessId.equals(dataBaseConfig.getBusinessId())) {
                            if (StringUtils.isNotBlank(dataBaseConfig.getFilterFields()) && dataBaseConfig.getFilterFields().split(":").length > 0) {
                                for (String field : dataBaseConfig.getFilterFields().split(":")) {
                                    if (null != customizeField && customizeField.containsKey(field)) {
                                        customizeField.remove(field);
                                    }
                                }
                            }
                            eventExec.exec(docId, customizeField, dataBaseConfig.getDatabaseName(), dataBaseConfig.getTableName());
                        }
                    } catch (Exception e) {
                        logger.info("eventExec cause Exception", e);
                    }
                }
            });
            noticePostLog(logType);
            logList.clear();
        }
    }

    private <T> T getLogObject(String log, Class<T> t) {
        try {
            if (log.split("- \\{").length >= 2) {
                return gson.fromJson("{" + (log.split("- \\{"))[1], t);
            } else if (log.startsWith("{") && log.endsWith("}")) {
                return gson.fromJson(log, t);
            } else {
                logger.info("log:" + log + " can not to process...");
            }
        } catch (Exception e) {
            logger.error("Exception log is:" + log);
            logger.error("getLogObject cause Exception", e);
        }
        return null;
    }

    private String nginxRequestBodyTojson(String requestBody) throws Exception {
        requestBody = requestBody.replace("\\x22","\"");
        if (null != requestBody && !"-".equals(requestBody)) {
            String[] strs = URLDecoder.decode(requestBody, "utf-8").split("&");
            Map<String, Object> result = new HashMap<>();
            if (strs.length > 0) {
                for (String str : strs) {
                    String[] tmp = str.split("=");
                    if (tmp.length > 1) {
                        result.put(tmp[0], tmp[1]);
                    }
                }
            }
            if (result.size() > 0) {
                return GsonUtil.gson.toJson(result);
            } else {
                return requestBody;
            }
        }
        return requestBody;
    }

    private NginxLog getNginxLog(String log) {
        try {
            String nginxLogFormat = EtcdUtil.getLocalPropertie("nginxLogFormat");
            String nginxLogSplit = EtcdUtil.getLocalPropertie("nginxLogSplit");
            if (null != nginxLogFormat && null != nginxLogSplit) {
                String[] logFieldValue = log.split(nginxLogSplit);
                String[] fields = nginxLogFormat.split(nginxLogSplit);
                String formatField = null;
                Field[] fs = NginxLog.class.getDeclaredFields();
                NginxLog nginxLog = new NginxLog();
                Map<String, Object> customizeField = new HashMap<>();
                Integer i = 0;
                for (String field : fields) {
                    formatField = field.replace("\"", "").replace("[", "").replace("]", "").replace("_", "").replace("$", "").toLowerCase();
                    for (Field nginxField : fs) {
                        nginxField.setAccessible(true);
                        if (nginxField.getName().toLowerCase().equals(formatField)) {
                            nginxField.set(nginxLog, logFieldValue[i].replace("\"", "").replace("[", "").replace("]", "").replace(";", ","));
                            customizeField.put(nginxField.getName(), "requestBody".equals(nginxField.getName()) ? nginxRequestBodyTojson((String) nginxField.get(nginxLog)) : nginxField.get(nginxLog));
                        }
                    }
                    i += 1;
                }
                nginxLog.setBusinessLogId(UUIDGenerator.getUUID());
                customizeField.put("businessLogId", nginxLog.getBusinessLogId());
                customizeField.put("collectionTime", DateCommonUtil.changeDateToMillisecondString(new Date()));
                nginxLog.setCustomizeField(customizeField);
                return nginxLog;
            } else {
                logger.error("etcd config nginxLogFormat or nginxLogSplit is null");
            }
        } catch (Exception e) {
            logger.error("getNginxLog cause Exception", e);
        }
        return null;
    }

    public void addEvent(Event event, String logType) {
        synchronized (logList) {
            String log = new String(event.getBody());
            if (null != log && log.contains("collectionSign")) {
                Object logTemp = null;
                if ("2".equals(logType)) {
                    logTemp = getLogObject(log, BusinessLog.class);
                } else if ("5".equals(logType)) {
                    logTemp = getLogObject(log, CrawlerLog.class);
                } else if ("3".equals(logType)) {
                    logTemp = getNginxLog(log);
                }
                if (null == logTemp) {
                    logger.error("log can not covert to Object...log:" + log);
                    return;
                }
                if (null != processorBeanList && processorBeanList.size() > 0) {
                    for (ProcessorBean processorBean : processorBeanList) {
                        try {
                            if (logType.equals(processorBean.getLogType())) {
                                if ("2".equals(logType)) {
                                    logTemp = (BusinessLog) processorBean.getProcessorInstance().process(logTemp);
                                } else if ("5".equals(logType)) {
                                    logTemp = (CrawlerLog) processorBean.getProcessorInstance().process(logTemp);
                                } else if ("3".equals(logType)) {
                                    logTemp = (NginxLog) processorBean.getProcessorInstance().process(logTemp);
                                }
                            }
                        } catch (Throwable e) {
                            logger.error("exec process cause Exception", e);
                        }
                    }
                }
                if (logTemp != null) {
                    logList.add(logTemp);
                }
            } else {
                logger.error("receive log:{} msg do not need to deal", log);
            }
        }
    }

    public void noticePostLog(String logType) {
        if (null != EtcdUtil.getLocalPropertie("isNotice") && "1".equals(EtcdUtil.getLocalPropertie("isNotice"))) {
            List<Map<String, Object>> callList = new ArrayList<>();
            logList.forEach(log -> {
                Map<String, Object> map = new HashMap<>();
                switch (logType) {
                    case "2":
                        map.put("businessId", ((BusinessLog) log).getBusinessId());
                        map.put("businessLogId", ((BusinessLog) log).getBusinessLogId());
                        map.put("businessSystem", ((BusinessLog) log).getBusinessSystem());
                        map.put("businessType", ((BusinessLog) log).getBusinessType());
                        break;
                    case "5":
                        map.put("businessId", ((CrawlerLog) log).getBusinessId());
                        map.put("businessSystem", ((CrawlerLog) log).getBusinessSystem());
                        map.put("businessType", ((CrawlerLog) log).getBusinessType());
                        map.put("taskId", ((CrawlerLog) log).getTaskId());
                        map.put("crawlerStarttime", ((CrawlerLog) log).getCrawlerStarttime());
                        map.put("crawlerEndtime", ((CrawlerLog) log).getCrawlerEndtime());
                        break;
                    case "3":
                        map.put("businessId", ((NginxLog) log).getBusinessId());
                        break;
                    default:
                        break;
                }
                if (null != EtcdUtil.getLocalPropertie("noticeFields") && EtcdUtil.getLocalPropertie("noticeFields").split(":").length > 0) {
                    for (String noticeField : EtcdUtil.getLocalPropertie("noticeFields").split(":")) {
                        switch (logType) {
                            case "2":
                                map.put(noticeField, ((BusinessLog) log).getCustomizeField().get(noticeField));
                                break;
                            case "5":
                                map.put(noticeField, ((CrawlerLog) log).getCustomizeField().get(noticeField));
                                break;
                            case "3":
                                map.put(noticeField, ((NginxLog) log).getCustomizeField().get(noticeField));
                                break;
                            default:
                                break;
                        }
                    }
                    callList.add(map);
                }
            });
            if (null != callList && callList.size() > 0) {
                noticePostLog(callList);
            }
        }
    }
}
