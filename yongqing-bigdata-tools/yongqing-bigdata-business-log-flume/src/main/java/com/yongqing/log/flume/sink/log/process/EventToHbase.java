package com.yongqing.log.flume.sink.log.process;
import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.tools.EtcdUtil;
import com.yongqing.hbase.utils.HbaseClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class EventToHbase extends AbstractEventTo implements Action {
    private static final Logger logger = LoggerFactory
            .getLogger(EventToHbase.class);

    public EventToHbase() {
        logList = new ArrayList<Object>();
    }

    public EventToHbase(List<Object> logList) {
        this.logList = logList;
    }

    public void execute(String logType){
        super.execute(logType, (String docId, Map<String, Object> customizeField, String dataBaseName, String tableName)->{
            List<Put> listPut = new ArrayList<Put>();
            Put put = new Put(Bytes.toBytes(docId));
            String family = tableName.split(":")[1];
            customizeField.forEach((k, v) -> {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes((String) k), null == v ? new byte[0] : Bytes.toBytes((String) v));
            });
            listPut.add(put);
            try {
                HbaseClient.getInstance(EtcdUtil.getLocalPropertie("zkClient"), EtcdUtil.getLocalPropertie("zkClientPort")).multiplePut(null, listPut, StringUtils.isBlank(dataBaseName)?tableName.split(":")[0]:dataBaseName+":"+tableName.split(":")[0]);
            } catch (IOException e) {
                logger.error("hbase multiplePut cause Exception",e);
            }
        });
    }
    @Override
    public void doAction(Properties oldProp, Properties newProp) {
        if (null != oldProp.getProperty("zkClient") && null != oldProp.getProperty("zkClientPort") && null != newProp.getProperty("zkClient") && null != newProp.getProperty("zkClientPort") && (!oldProp.getProperty("zkClient").equals(newProp.getProperty("zkClient")) || !oldProp.getProperty("zkClientPort").equals(newProp.getProperty("zkClientPort")))) {
            HbaseClient.getInstance(oldProp.getProperty("zkClient"), oldProp.getProperty("zkClientPort")).close();
            HbaseClient.getInstance(EtcdUtil.getLocalPropertie("zkClient"), EtcdUtil.getLocalPropertie("zkClientPort"));
        }
    }
}
