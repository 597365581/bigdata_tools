package com.yongqing.hive.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.hive.hcatalog.streaming.*;

import java.io.IOException;
import java.util.Collection;

/**
 *
 */
@Log4j2
public class HiveDataSerializerImpl implements HiveDataSerializer {
    private String[] fieldToColMapping = null;
    private Character serdeSeparator = null;
    private String delimiter;

    public HiveDataSerializerImpl() {
        super();
    }

    public HiveDataSerializerImpl(String delimiter, Character serdeSeparator, String[] fieldToColMapping) {
        super();
        this.delimiter = delimiter;
        this.fieldToColMapping = fieldToColMapping;
        this.serdeSeparator = serdeSeparator;
    }

    @Override
    public void write(TransactionBatch txnBatch, byte[] e) throws StreamingException, IOException, InterruptedException {
        log.info("start to write event...event:{}",new String(e));
        txnBatch.write(e);
    }

    @Override
    public void write(TransactionBatch txnBatch, Collection<byte[]> events) throws StreamingException, IOException, InterruptedException {
        log.info("start to write event...size:{}",events.size());
        txnBatch.write(events);
    }

    @Override
    public RecordWriter createRecordWriter(HiveEndPoint endPoint) throws StreamingException, IOException, ClassNotFoundException {
        if(null==delimiter || null==fieldToColMapping){
            throw new NullPointerException("delimiter or fieldToColMapping can not be null");
        }
        if (serdeSeparator == null) {
            return new DelimitedInputWriter(fieldToColMapping, delimiter, endPoint);
        }
        return new DelimitedInputWriter(fieldToColMapping, delimiter, endPoint, null, serdeSeparator);
    }

    public void setFieldDeal(String[] fieldToColMapping, Character serdeSeparator, String delimiter) {
        this.serdeSeparator = serdeSeparator;
        this.fieldToColMapping = fieldToColMapping;
        this.delimiter = delimiter;
    }
}
