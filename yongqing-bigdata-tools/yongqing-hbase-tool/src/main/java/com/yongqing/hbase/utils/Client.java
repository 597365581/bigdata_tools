package com.yongqing.hbase.utils;

import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * hbase操作
 */
public interface Client {
     /**
      * 链接初始化接口 对hbase的Connection进行初始化
      */
     void initHbaseConnection();
     List<Map<String, Object>> getHbaseData(String queryType, String tableName, List<String> queryStrList, Boolean isNeedReverse) throws IOException;
     List<Map<String, Object>> getHbaseDataByScanAndStartRowAndEndRow(String queryType, String tableName, String startRow, String endRow) throws IOException;
     List<Map<String, Object>> getHbaseDataByScan(String queryType, String tableName) throws IOException;
     void multiplePut(String insertType, List<Put> listPut, String tableName) throws IOException;
     void close();

}
