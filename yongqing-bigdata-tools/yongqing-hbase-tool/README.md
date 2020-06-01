# yongqing-hbase-tool

大数据--hbase操作API

**API操作**
<p>hbase 提供HbaseClient.class工具类直接操作hbase数据库。</P>
<p>HbaseClient 提供单例模式的操作，首先通过HbaseClient.getInstance() 获取实例。getInstance 获取单例实例。</P>
<p>如果需要自定义Hbase的操作或者进行扩展以及自己对链接初始化实现，请继承HbaseAbstractClient这个抽象类。继承后可以自定义实现connection的初始化。</P>
<p>HbaseClient 提供如下对数据库的API操作</P>
<p>

<p>对hbase根据rowkey查询（支持批量查询）</P>

**public List<Map<String, Object>> getHbaseData(String queryType, String tableName, List<String> queryStrList, Boolean isNeedReverse)** 

</P>

**参数：**
<p>queryType 为查询类型，当前为预留字段，可以直接传入null</P>
<p>tableName为待查询的表名</P>
<p>queryStrList 为待查询的rowkey字符串列表</P>
<p>isNeedReverse 查询时是否需要对queryStrList 里面的字符串做反转</P>
<p>对hbase根据开始行和结束行进行 scan查询</P>
<p>对hbase根据scan查询</P>

**public List<Map<String, Object>> getHbaseDataByScanAndStartRowAndEndRow(String queryType, String tableName, String startRow, String endRow)** 

</P>

**参数：**
<p>queryType 为查询类型，当前为预留字段，可以直接传入null</P>
<p>tableName为待查询的表名</P>
<p>startRow为开始行，这个是必须填入</P>
<p>endRow为结束行，这个是必须填入</P>
<p>对hbase插入操作（支持批量插入）</P>

**public void multiplePut(String insertType, List<Put> listPut, String tableName)** 

</P>

**参数：**
<p>insertType 为插入类型，当前为预留字段，可以直接传入null</P>
<p>tableName为待插入的表名</P>
<p>listPut为待插入的数据</P>

**代码示例**

  
   <p>     Put put = new Put(Bytes.toBytes("123456789"));//123456789 代表hbase表的rowkey</P>
   <p>     put.addColumn(Bytes.toBytes("test"),Bytes.toBytes("name"),Bytes.toBytes("zhangyq"));</P>
    <p>    List<Put> list = new ArrayList<Put>();</P>
       <p>   list.add(put);</P>
   <p>     HbaseClient.getInstance().multiplePut(null,list,"zhangyongqing" );</P>

<p>对hbase全表scan扫描查询操作，请务必慎用</P>

**public List<Map<String, Object>> getHbaseDataByScan(String queryType, String tableName)** 

</P>

**参数：**
<p>queryType 为查询类型，当前为预留字段，可以直接传入null</P>
<p>tableName为待查询的表名</P>


<p>对hbase以class Pojo插入操作（支持批量插入）</P>

**public void multipleInsert(String insertType, List<T> listT, String tableName, Class<T> tclass, String family)** 
<p>insertType 为插入类型，当前为预留字段，可以直接传入null</P>
<p>listT为待插入的数据</P>
<p>tableName为待插入的表名</P>
<p>tclass 插入的Pojo对象类型</P>
<p>family 插入的表的列族（该方法只支持单列族）</P>


<p>对hbase做删除操作（支持批量插入）</P>

**public void multipleDelete(List<Delete> deleteList, String tableName)**

<p>deleteList为待删除的数据</P>

<p>tableName为待删除的表名</P>

<p>对hbase根据rowkey查询（支持批量查询）</P>

**public List<Map<String, Object>> getHbaseData(String queryType, String tableName, List<Get> queryGetList)**

<p>queryType 为查询类型，当前为预留字段，可以直接传入null</P>
<p>tableName为待查询的表名</P>
<p>待批量查询的条件字段</P>


<p>根据filter进行scan</P>

 **public List<Map<String, Object>> getHbaseDataByScanAndFilter(String queryType, String tableName, FilterList filterList) throws IOException** 
 <p>queryType 为查询类型，当前为预留字段，可以直接传入null</P>
 <p>tableName为待查询的表名</P>
 <p>FilterList</P>