package com.yongqing.hive.tool.hcatalog;

import com.yongqing.hdfs.tool.HdfsClient;
import com.yongqing.hive.tool.HiveException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.transfer.*;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatBaseOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class HCataLogClient {

    public HCatSchema getHCatSchema(HdfsClient hdfsClient, String type) throws IOException {
        if ("1".equals(type)) {
            return HCatBaseInputFormat.getTableSchema(hdfsClient.getConfiguration());
        } else if ("2".equals(type)) {
            return HCatBaseOutputFormat.getTableSchema(hdfsClient.getConfiguration());
        } else {
            throw new HiveException("type is not support,only support type is 1(InputFormar) or 2(OutputFormat)");
        }
    }

    public Job getJob(HdfsClient hdfsClient, String jobName) throws IOException {
        Job job = Job.getInstance(hdfsClient.getConfiguration());
        job.setJobName(jobName);
        return job;
    }

    public HCatReader getHCatReader(ReadEntity entity, Map<String, String> conf) {
        return DataTransferFactory.getHCatReader(entity, conf);
    }

    public ReaderContext getReaderContext(ReadEntity entity, Map<String, String> conf) throws HCatException {
        return getHCatReader(entity, conf).prepareRead();
    }

    public HCatWriter getHCatWriter(WriteEntity entity, Map<String, String> conf) {
        return DataTransferFactory.getHCatWriter(entity, conf);
    }

    public WriterContext getWriterContext(WriteEntity entity, Map<String, String> conf) throws HCatException {
        return getHCatWriter(entity, conf).prepareWrite();
    }
}
