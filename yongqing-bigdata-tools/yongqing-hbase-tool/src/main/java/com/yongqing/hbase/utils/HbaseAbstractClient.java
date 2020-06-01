package com.yongqing.hbase.utils;

import com.yongqing.hbase.exception.HbaseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * hbase客户端操作的抽象类
 */
public abstract class HbaseAbstractClient<T> implements Client {
    //log
    protected static final Logger logger = LoggerFactory.getLogger(HbaseAbstractClient.class);
    //hbase的连接
    protected static volatile Connection connection;

    /**
     * 批量get
     *
     * @param queryType     查询类型，预留字段
     * @param tableName     查询的表名
     * @param queryStrList  需要查询的list<rowkey>
     * @param isNeedReverse 是否需要对key进行反转操作
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> getHbaseData(String queryType, String tableName, List<String> queryStrList, Boolean isNeedReverse) throws IOException {
        List<Get> list = new ArrayList<>();
        queryStrList.forEach(rowKey -> {
            Get get = new Get(Bytes.toBytes(isNeedReverse ? StringUtils.reverse(rowKey) : rowKey));
            list.add(get);
        });
        return getHbaseData(queryType, tableName, list);
    }

    /**
     * @param queryType
     * @param tableName
     * @param queryGetList
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> getHbaseData(String queryType, String tableName, List<Get> queryGetList) throws IOException {
        Table table = null;
        List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Result[] results = table.get(queryGetList);
            Arrays.stream(results).forEach(a -> {
                Map<String, Object> map = new HashMap<String, Object>();
                if (!ArrayUtils.isEmpty(a.rawCells())) {
                    Arrays.stream(a.rawCells()).forEach(c -> {
                        map.put(Bytes.toString(CellUtil.cloneQualifier(c)), Bytes.toString(CellUtil.cloneValue(c)));
                    });
                    mapList.add(map);
                }
            });
        } catch (IOException e) {
            throw new HbaseException("hbase query error" + e.getMessage(), e);
        } finally {
            //关闭表
            if (null != table) {
                table.close();
            }
        }
        return mapList;
    }

    /**
     * @param queryType
     * @param tableName
     * @param filterList
     * @return
     * @throws IOException
     */

    public List<Map<String, Object>> getHbaseDataByScanAndFilter(String queryType, String tableName, FilterList filterList) throws IOException {
        Table table = null;
        ResultScanner rs = null;
        List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            rs = table.getScanner(scan);
            rs.forEach(a -> {
                Map<String, Object> map = new HashMap<String, Object>();
                Arrays.stream(a.rawCells()).forEach(s -> {
                    map.put(Bytes.toString(CellUtil.cloneQualifier(s)), Bytes.toString(CellUtil.cloneValue(s)));
                    logger.info("列:{},值:{}", Bytes.toString(CellUtil.cloneQualifier(s)), Bytes.toString(CellUtil.cloneValue(s)));
                });
                mapList.add(map);
                logger.info("=======列分隔符号========");
            });
        } catch (Exception e) {
            throw new HbaseException("hbase query error" + e.getMessage(), e);
        } finally {
            //关闭表
            if (null != table) {
                table.close();
            }
            if (null != rs) {
                rs.close();
            }
        }
        return mapList;
    }

    /**
     * @param queryType 查询类型 预留字段
     * @param tableName hbase表名称
     * @param startRow  开始的row
     * @param endRow    结束 的row
     * @return
     * @throws IOException
     */

    public List<Map<String, Object>> getHbaseDataByScanAndStartRowAndEndRow(String queryType, String tableName, String startRow, String endRow) throws IOException {
        Table table = null;
        ResultScanner rs = null;
        List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if (StringUtils.isNotBlank(startRow)) {
                scan.withStartRow(Bytes.toBytes(startRow));
            }
            if (StringUtils.isNotBlank(endRow)) {
                scan.withStopRow(Bytes.toBytes(endRow));
            }
            rs = table.getScanner(scan);
            rs.forEach(a -> {
                Map<String, Object> map = new HashMap<String, Object>();
                Arrays.stream(a.rawCells()).forEach(s -> {
                    map.put(Bytes.toString(CellUtil.cloneQualifier(s)), Bytes.toString(CellUtil.cloneValue(s)));
                    logger.info("列:{},值:{}", Bytes.toString(CellUtil.cloneQualifier(s)), Bytes.toString(CellUtil.cloneValue(s)));
                });
                mapList.add(map);
                logger.info("=======列分隔符号========");
            });
        } catch (Exception e) {
            throw new HbaseException("hbase query error" + e.getMessage(), e);
        } finally {
            //关闭表
            if (null != table) {
                table.close();
            }
            if (null != rs) {
                rs.close();
            }
        }
        return mapList;
    }

    /**
     * 全表扫描，慎用,没有指定start和end
     *
     * @param queryType 查询类型，预留字段
     * @param tableName 查询的表名
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> getHbaseDataByScan(String queryType, String tableName) throws IOException {
        return getHbaseDataByScanAndStartRowAndEndRow(queryType, tableName, null, null);
    }

    /**
     * hbase的插入操作
     *
     * @param insertType 插入类型
     * @param listPut    插入的list
     * @param tableName  需要插入到哪张表中
     * @throws IOException
     */


    public void multiplePut(String insertType, List<Put> listPut, String tableName) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(listPut);
        } catch (IOException e) {
            throw new HbaseException("hbase insert error" + e.getMessage(), e);
        } finally {
            //关闭表
            if (null != table) {
                table.close();
            }
        }
    }


    public void multipleDeleteByRowkey(List<String> rowkeyList, String tableName) throws IOException {
        if (null == rowkeyList || rowkeyList.size() == 0) {
            throw new HbaseException("rowkeyList is null or rowkeyList size is 0");
        }
        List<Delete> deleteList = new ArrayList<>();
        for (String rowkey : rowkeyList) {
            if (StringUtils.isBlank(rowkey)) {
                throw new HbaseException("rowkey is null");
            }
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            deleteList.add(delete);
        }
        multipleDelete(deleteList, tableName);
    }


    /**
     * 删除操作
     *
     * @param deleteList
     * @param tableName
     * @throws IOException
     */

    public void multipleDelete(List<Delete> deleteList, String tableName) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            table.delete(deleteList);
        } catch (Exception e) {
            throw new HbaseException("hbase delete error" + e.getMessage(), e);
        } finally {
            //关闭表
            if (null != table) {
                table.close();
            }
        }
    }

    /**
     * @param insertType
     * @param listT
     * @param tableName
     * @param tclass
     * @param family
     */
    public void multipleInsert(String insertType, List<T> listT, String tableName, Class<T> tclass, String family) throws Exception {
        if (family == null || tableName == null) {
            throw new HbaseException("Hbase family or tableName is null");
        }
        List<Put> listPut = new ArrayList<Put>();
        //TODO 通过反射获取每一个字段的值，封装进去PUT中。
        Field[] fs = tclass.getDeclaredFields();
        if (fs.length == 0) {
            throw new HbaseException(tclass.getName() + "need define at least one Field");
        }
        listT.forEach(t -> {
            Put put = null;
            try {
                for (Field field : fs) {
                    field.setAccessible(true);
                    if (field.getType().getName().equals("java.lang.String") && field.getName().equals("rowkey")) {
                        put = new Put(Bytes.toBytes((String) field.get(t)));
                    }
                }
                if (null == put) {
                    throw new HbaseException(tclass + " need contain field rowkey and field rowkey's Type must be String");
                }
                for (Field field : fs) {
                    field.setAccessible(true);
                    if (field.getType().getName().equals("java.lang.String")) {
                        if (!field.getName().equals("rowkey")) {
                            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()), null != field.get(t) ? Bytes.toBytes((String) field.get(t)) : new byte[0]);
                        }
                    } else if (field.getType().getName().equals("java.lang.Integer") || field.getType().getName().equals("int")) {
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()), null != field.get(t) ? Bytes.toBytes((Integer) field.get(t)) : new byte[0]);
                    } else if (field.getType().getName().equals("java.lang.Float") || field.getType().getName().equals("float")) {
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()), null != field.get(t) ? Bytes.toBytes((Float) field.get(t)) : new byte[0]);
                    } else if (field.getType().getName().equals("java.lang.Long") || field.getType().getName().equals("long")) {
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()), null != field.get(t) ? Bytes.toBytes((Long) field.get(t)) : new byte[0]);
                    } else if (field.getType().getName().equals("java.lang.Double") || field.getType().getName().equals("double")) {
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()), null != field.get(t) ? Bytes.toBytes((Double) field.get(t)) : new byte[0]);
                    } else if (field.getType().getName().equals("java.util.Date")) {
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field.getName()), null != field.get(t) ? Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) field.get(t))) : new byte[0]);
                    } else {
                        throw new HbaseException(tclass + " do not support field Type" + field.getType().getName());
                    }
                }
            } catch (Exception e) {
                throw new HbaseException(e.getMessage() + e.getCause());
            }
            listPut.add(put);
        });
        multiplePut(null, listPut, tableName);
    }

    public synchronized void close() {
        if (null != connection && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.info("hbase connection close failed", e);
            }
            connection = null;
        }
    }
}
