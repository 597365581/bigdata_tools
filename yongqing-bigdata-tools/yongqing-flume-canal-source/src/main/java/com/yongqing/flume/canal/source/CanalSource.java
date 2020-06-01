package com.yongqing.flume.canal.source;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.Preconditions;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.processor.log.bean.MysqlBinLog;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

import java.net.InetSocketAddress;
import java.util.*;

/**
 *
 */
public class CanalSource extends AbstractSource implements Configurable, PollableSource {
    private String etcdConfig;
    private CanalConnector connector;
    private String reloadConnector;
    private static final Logger logger = LoggerFactory
            .getLogger(CanalSource.class);

    @Override
    public Status process() throws EventDeliveryException {
        synchronized (this) {
            if ((null == reloadConnector && null != EtcdUtil.getLocalPropertie("reloadConnector")) || !reloadConnector.equals(EtcdUtil.getLocalPropertie("reloadConnector"))) {
                logger.info("start to reset connector...");
                reloadConnector = EtcdUtil.getLocalPropertie("reloadConnector");
                if (null != connector) {
                    connector.disconnect();
                    connector = null;
                }
                setConnector();
                connector.connect();
                connector.subscribe();
            }
        }
        Message message = connector.getWithoutAck(Integer.valueOf(EtcdUtil.getLocalPropertie("batchSize")));
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            logger.info("message is empty");
        } else {
            List<MysqlBinLog> mysqlBinLogList = new ArrayList<>();
            for (Entry entry : message.getEntries()) {
                if ((entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) && null != EtcdUtil.getLocalPropertie("filterTransaction") && "1".equals(EtcdUtil.getLocalPropertie("filterTransaction"))) {
                    logger.info("canal TRANSACTIONBEGIN or TRANSACTIONEND is filter");
                } else {
                    try {
                        MysqlBinLog mysqlBinLog = new MysqlBinLog();
                        //TODO
                        mysqlBinLog.setId(message.getId());
                        mysqlBinLogList.add(mysqlBinLog);
                        CanalEntry.RowChange rowChange = null;
                        rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        if (rowChange.getIsDdl()) {
                            logger.info("canal get a ddl sql,sql is:" + rowChange.getSql());
                        }
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        mysqlBinLog.setDatabase(entry.getHeader().getSchemaName());
                        mysqlBinLog.setTable(entry.getHeader().getTableName());
                        mysqlBinLog.setDdl(rowChange.getIsDdl());
                        mysqlBinLog.setType(eventType.toString());
                        mysqlBinLog.setEs(entry.getHeader().getExecuteTime());
                        mysqlBinLog.setTs(System.currentTimeMillis());
                        mysqlBinLog.setSql(rowChange.getSql());
                        if (!rowChange.getIsDdl()) {
                            Map<String, Object> sqlType = new LinkedHashMap<>();
                            Map<String, Object> mysqlType = new LinkedHashMap<>();
                            List<Map<String, Object>> data = new ArrayList<>();
                            List<Map<String, Object>> old = new ArrayList<>();
                            Set<String> updateSet = new HashSet<>();
                            boolean hasInitPkNames = false;
                            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                                if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE && eventType != CanalEntry.EventType.DELETE) {
                                    continue;
                                }
                                Map<String, Object> row = new LinkedHashMap<>();
                                List<CanalEntry.Column> columns;
                                if (eventType == CanalEntry.EventType.DELETE) {
                                    columns = rowData.getBeforeColumnsList();
                                } else {
                                    columns = rowData.getAfterColumnsList();
                                }
                                for (CanalEntry.Column column : columns) {
                                    if (!hasInitPkNames && column.getIsKey()) {
                                        mysqlBinLog.addPkName(column.getName());
                                    }
                                    sqlType.put(column.getName(), column.getSqlType());
                                    mysqlType.put(column.getName(), column.getMysqlType());
                                    if (column.getIsNull()) {
                                        row.put(column.getName(), null);
                                    } else {
                                        row.put(column.getName(), column.getValue());
                                    }
                                    // 获取update为true的字段
                                    if (column.getUpdated()) {
                                        updateSet.add(column.getName());
                                    }
                                }
                                hasInitPkNames = true;
                                if (!row.isEmpty()) {
                                    data.add(row);
                                }
                                if (eventType == CanalEntry.EventType.UPDATE) {
                                    Map<String, Object> rowOld = new LinkedHashMap<>();
                                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                        if (updateSet.contains(column.getName())) {
                                            if (column.getIsNull()) {
                                                rowOld.put(column.getName(), null);
                                            } else {
                                                rowOld.put(column.getName(), column.getValue());
                                            }
                                        }
                                    }
                                    // update操作将记录修改前的值
                                    if (!rowOld.isEmpty()) {
                                        old.add(rowOld);
                                    }
                                }
                                if (!sqlType.isEmpty()) {
                                    mysqlBinLog.setSqlType(sqlType);
                                }
                                if (!mysqlType.isEmpty()) {
                                    mysqlBinLog.setMysqlType(mysqlType);
                                }
                                if (!data.isEmpty()) {
                                    mysqlBinLog.setData(data);
                                }
                                if (!old.isEmpty()) {
                                    mysqlBinLog.setOld(old);
                                }
                            }
                        }
                        //事件
                        Event event = new SimpleEvent();
                        event.setBody(GsonUtil.gson.toJson(mysqlBinLog).getBytes("utf-8"));
                        Map<String, String> header = new HashMap<>();
                        header.put("schema", entry.getHeader().getSchemaName());
                        header.put("eventType", eventType.toString());
                        header.put("sql", rowChange.getSql());
                        header.put("table", entry.getHeader().getTableName());
                        header.put("execTime", String.valueOf(entry.getHeader().getExecuteTime()));
                        header.put("logFileName", entry.getHeader().getLogfileName());
                        header.put("isDdl", rowChange.getIsDdl() ? "1" : "0");
                        event.setHeaders(header);
                        // 将事件写入 channel
                        getChannelProcessor().processEvent(event);
                        connector.ack(batchId);
                        logger.info("put one event success...");

                    } catch (Exception e) {
                        logger.error("canal get RowChange cause Exception", e);
                        connector.rollback(batchId);
                        logger.info("batchId:{} is rollback", batchId);
                    }
                }
            }
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        if (StringUtils.isNotBlank(context.getString("etcdConfig"))) {
            this.etcdConfig = context.getString("etcdConfig");
        }
        Preconditions.checkState(null != etcdConfig, etcdConfig
                + " must not be null");
    }

    public void start() {
        logger.info("begin to start etcd listen...");
        EtcdUtil.initListen(etcdConfig);
        logger.info("start etcd listen sucess...");
        reloadConnector = EtcdUtil.getLocalPropertie("reloadConnector");
        setConnector();
        connector.connect();
        connector.subscribe();
        super.start();
    }

    public void stop() {
        if (null != connector) {
            connector.disconnect();
            connector = null;
        }
        EtcdUtil.getEtclClient().close();
        super.stop();
    }

    private void setConnector() {
        if (null == EtcdUtil.getLocalPropertie("serverType") || "1".equals(EtcdUtil.getLocalPropertie("serverType"))) {
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(EtcdUtil.getLocalPropertie("hostName"), Integer.valueOf(EtcdUtil.getLocalPropertie("port"))), EtcdUtil.getLocalPropertie("destination"), EtcdUtil.getLocalPropertie("username"), EtcdUtil.getLocalPropertie("password"));
        } else if ("2".equals(EtcdUtil.getLocalPropertie("serverType"))) {
            List<InetSocketAddress> inetSocketAddressList = new ArrayList<>();
            String[] hosts = EtcdUtil.getLocalPropertie("hosts").split(",");
            for (String host : hosts) {
                inetSocketAddressList.add(new InetSocketAddress(host.split(":")[0], Integer.valueOf(host.split(":")[1])));
            }
            connector = CanalConnectors.newClusterConnector(inetSocketAddressList, EtcdUtil.getLocalPropertie("destination"), EtcdUtil.getLocalPropertie("username"), EtcdUtil.getLocalPropertie("password"));

        } else if ("3".equals(EtcdUtil.getLocalPropertie("serverType"))) {
            connector = CanalConnectors.newClusterConnector(EtcdUtil.getLocalPropertie("zkServers"), EtcdUtil.getLocalPropertie("destination"), EtcdUtil.getLocalPropertie("username"), EtcdUtil.getLocalPropertie("password"));
        } else {
            throw new RuntimeException("serverType is not support,please check...");
        }
    }
}
