package com.yongqing.hive.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.hive.hcatalog.streaming.*;

import java.io.IOException;
import java.util.Collection;

/**
 *
 */
@Log4j2
public class HiveJsonDataSerializerImpl implements HiveDataSerializer{


    @Override
    public void write(TransactionBatch txnBatch, byte[] event) throws StreamingException, IOException, InterruptedException {
        log.info("start to write event...size:{},event:{}",event.length,new String(event));
        txnBatch.write(event);
    }

    @Override
    public void write(TransactionBatch txnBatch, Collection<byte[]> events)
            throws StreamingException, IOException, InterruptedException {
        log.info("start to write event...size:{}",events.size());
        txnBatch.write(events);
    }

    @Override
    public RecordWriter createRecordWriter(HiveEndPoint endPoint)
            throws StreamingException, IOException, ClassNotFoundException {
        return new StrictJsonWriter(endPoint);
    }
}
