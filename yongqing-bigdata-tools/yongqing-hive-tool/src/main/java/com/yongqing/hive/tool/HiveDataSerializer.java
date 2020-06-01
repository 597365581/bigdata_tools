package com.yongqing.hive.tool;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.io.IOException;
import java.util.Collection;

/**
 *
 */
public interface HiveDataSerializer {

     void write(TransactionBatch txnBatch, byte[] e)
            throws StreamingException, IOException, InterruptedException;

     void write(TransactionBatch txnBatch, Collection<byte[]> events)
            throws StreamingException, IOException, InterruptedException;

    RecordWriter createRecordWriter(HiveEndPoint endPoint)
            throws StreamingException, IOException, ClassNotFoundException;
}
