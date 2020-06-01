# yongqing-etcd-tool

大数据工具包-etcd的操作
<p> **API** </p>

**工具类:EtcdUtil.class**
 
<p>public static void initListen(String key);</p>
<p>etcd的监听，监听指定的key，当key 发生变化后，监听自动感知到变化。 key发生变化后，会自动更新本地缓存数据</p>
<p>参数：需要监听的key值</p>

<p>每个项目在etcd中只有一个唯一的key，建议key以系统+war包来命名</p>
<p>etcd中每个key的value中以key/value的形式存储值</p>
<p>示例(value中的值以换行分隔，每一行是一个key和value，以=号隔开，某一行如果以#开头，代表为注释，在代码中不起任何的作用，将不会读入本地缓存):</p>
<p>aa=123456</p>
<p>bb=654321</p>
<p>dd=23456</p>
<p>eee=23456</p>
<p>ggg=12345</p>
<p>hhhh=123456789</p>

<p>public static String getLocalPropertie(String localKey) ;</p>

<p>需要从本地属性缓存中读取的key的值</p>
<p>参数：本地属性缓存中的key的值</p>

**高级选项用法**

<p>当本地缓存中发生变化后，可以自己决定缓存变化后，需要自动完成事情</p>
<p>自己定义class实现Action这个接口中的方法即可。
<p>/**</p>
 <p>*在etcd配置发生变化后，需要去触发的操作。</p>
 <p>*/</p>
<p>public interface Action {</p>
    /**</p>
     *   需要执行的操作</p>
     * @param oldProp  老的配置属性</p>
     * @param newProp  新的配置属性</p>
     */</p>
   <p> void doAction(Properties oldProp, Properties newProp);</p>
<p>}</p>



