package com.yongqing.hdfs.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 *
 */
public interface Clinet {

    FileSystem getFileSystem();
    Configuration getConfiguration();
    boolean createHdfspath(String path);
    void close() throws IOException;
}
