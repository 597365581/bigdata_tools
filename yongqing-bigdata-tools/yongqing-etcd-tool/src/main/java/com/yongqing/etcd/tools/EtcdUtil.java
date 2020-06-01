package com.yongqing.etcd.tools;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Lock;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.coreos.jetcd.lock.LockResponse;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.WatchOption;

import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.exec.Exec;
import com.yongqing.etcd.tools.core.CommonUtil;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * etcd 链接和操作工具，包括启动监听 操作etcd v3 版本协议，此操作不支持v2 版本协议。
 * v2版本的协议可以参考 https://www.cnblogs.com/laoqing/p/8967549.html
 */
@Log4j2
public class EtcdUtil {
    //    public static Logger log = LoggerFactory.getLogger(EtcdUtil.class);
    //etcl客户端链接
    private static volatile Client client = null;
    // 换行符
    private static final String LINE_SEPARATOR = "\n";
    // 资源文件注释标记
    private static final String PROPERTIES_COMMENT = "#";
    // 资源文件Key/Value分割符  在etcd中每一个key中 存储了 key和value组成的数据
    private static final String PROPERTIES_SEPARATOR = "=";

    // 定义资源文件存储配置信息  本地缓存
    private static Properties properties = new Properties();

    private EtcdUtil() {
    }


    //链接初始化 单例模式
    public static synchronized Client getEtclClient() {
        if (null == client) {
            // 获取资源文件
            InputStream is = EtcdUtil.class.getClassLoader().getResourceAsStream("etcd.properties");
            //属性列表
            Properties prop = new Properties();
            try {
                prop.load(is);
            } catch (IOException e) {
                log.error("read etcd.properties fail", e);
            } finally {
                if (null != is) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        log.error("close etcd.properties fail", e);
                    }
                }
            }
            client = Client.builder().endpoints(prop.getProperty("endpoints")).build();
        }
        return client;
    }

    /**
     * 根据指定的配置名称获取对应的value
     *
     * @param key 配置项
     * @return
     * @throws Exception
     */
    public static String getEtcdValueByKey(String key) throws Exception {
        List<KeyValue> kvs = EtcdUtil.getEtclClient().getKVClient().get(ByteSequence.fromString(key)).get().getKvs();
        if (kvs.size() > 0) {
            String value = kvs.get(0).getValue().toStringUtf8();
            return value;
        } else {
            return null;
        }
    }

    public static long putEtcdKeyWithExpireTime(String key, String value, long expireTime) throws Exception {

        CompletableFuture<LeaseGrantResponse> leaseGrantResponse = EtcdUtil.getEtclClient().getLeaseClient().grant(expireTime);
        PutOption putOption;
        putOption = PutOption.newBuilder().withLeaseId(leaseGrantResponse.get().getID()).build();
        EtcdUtil.getEtclClient().getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value), putOption);
        return leaseGrantResponse.get().getID();
    }

    public static long putEtcdKeyWithLeaseId(String key, String value, long leaseId) throws Exception {
        PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
        CompletableFuture<PutResponse> putResponse = EtcdUtil.getEtclClient().getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromString(value), putOption);
        return putResponse.get().getHeader().getRevision();
    }

    public static void keepAliveEtcdSingleLease(long leaseId) {
        EtcdUtil.getEtclClient().getLeaseClient().keepAlive(leaseId);
    }

    public static Watch.Watcher getCustomWatcherForSingleKey(String key) {
        return EtcdUtil.getEtclClient().getWatchClient().watch(ByteSequence.fromString(key));
    }

    public static Watch.Watcher getCustomWatcherForPrefix(String prefix) {
        WatchOption watchOption = WatchOption.newBuilder().withPrefix(ByteSequence.fromString(prefix)).build();
        return EtcdUtil.getEtclClient().getWatchClient().watch(ByteSequence.fromString(prefix), watchOption);
    }

    public static void deleteEtcdKeyWithPrefix(String prefix) {
        DeleteOption deleteOption = DeleteOption.newBuilder().withPrefix(ByteSequence.fromString(prefix)).build();
        EtcdUtil.getEtclClient().getKVClient().delete(ByteSequence.fromString(prefix), deleteOption);
    }

    public static List<KeyValue> getEtcdKeyWithPrefix(String prefix) {
        List<KeyValue> keyValues = new ArrayList<>();
        GetOption getOption = GetOption.newBuilder().withPrefix(ByteSequence.fromString(prefix)).build();
        try {
            keyValues = EtcdUtil.getEtclClient().getKVClient().get(ByteSequence.fromString(prefix), getOption).get().getKvs();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return keyValues;
    }

    /**
     * 新增或者修改指定的配置
     *
     * @param key
     * @param value
     * @return
     */
    public static void putEtcdValueByKey(String key, String value) throws Exception {
        EtcdUtil.getEtclClient().getKVClient().put(ByteSequence.fromString(key), ByteSequence.fromBytes(value.getBytes("utf-8")));
    }

    /**
     * 删除指定的配置
     *
     * @param key
     * @return
     */
    public static void deleteEtcdValueByKey(String key) {
        EtcdUtil.getEtclClient().getKVClient().delete(ByteSequence.fromString(key));
    }

    /**
     * etcd的监听，监听指定的key，当key 发生变化后，监听自动感知到变化。 key发生变化后，会更新本地缓存数据
     *
     * @param key 指定需要监听的key
     */
    public static void initListen(String key) {
        try {

            //加载配置
            loadProperties(getConfig(EtcdUtil.getEtclClient().getKVClient().get(ByteSequence.fromString(key)).get().getKvs()));
            new Thread(() -> {
                Watch.Watcher watcher = EtcdUtil.getEtclClient().getWatchClient().watch(ByteSequence.fromString(key));
                try {
                    while (true) {
                        watcher.listen().getEvents().stream().forEach(watchEvent -> {
                            KeyValue kv = watchEvent.getKeyValue();
                            log.info("etcd event:{} ,change key is:{},afterChangeValue:{}", watchEvent.getEventType(), kv.getKey().toStringUtf8(), kv.getValue().toStringUtf8());
                            loadProperties(kv.getValue().toStringUtf8());
                        });
                    }
                } catch (InterruptedException e) {
                    log.error("etcd listen start cause Exception:{}", e);
                }
            }).start();
        } catch (Exception e) {
            log.error("etcd listen start cause Exception:{}", e);
        }
    }

    private static String getConfig(List<KeyValue> kvs) {
        if (kvs.size() > 0) {
            String config = kvs.get(0).getKey().toStringUtf8();
            String value = kvs.get(0).getValue().toStringUtf8();
            log.info("etcd 's config 's config key is :{},value is:{}", config, value);
            return value;
        } else {
            return null;
        }
    }

    /**
     * 更新本地缓存
     */
    public static synchronized void loadProperties(String propertiesConfig) {
        // 拷贝一份修改前的配置, 用于做比较
        final Properties oldProp = new Properties();
        if (null != properties) {
            oldProp.putAll(properties);
        }
        //更新前，先删除缓存
        properties.clear();
        String[] lines = propertiesConfig.split(LINE_SEPARATOR);// 回车换行符分割
        int lineNumber = 0;// 行号
        if (lines != null && lines.length > 0) {
            for (String line : lines) {
                // 注释行不处理
                if (line.startsWith(PROPERTIES_COMMENT))
                    continue;
                String[] data = line.split(PROPERTIES_SEPARATOR);
                if (data != null && data.length == 2) {
                    String key = data[0];
                    String value = data[1];
                    properties.put(key.trim(), value.trim());
                    log.info(" load config item :{}=:{} ", key.trim(), value.trim());
                } else {
                    log.info("config file line :{} ,Line Number:{} is empty config item!", line, lineNumber);
                }
                lineNumber++;
            }
        }
        //需要另外触发的操作
        doAction(oldProp, properties);
    }

    /**
     * @param localKey 本地缓存的key
     * @return 返回缓存的key对应的value
     */
    public static String getLocalPropertie(String localKey) {
        return properties.getProperty(localKey);
    }

    /**
     * 本地某个key指发生了变化后，需要执行的操作
     */
    public static void doAction(Properties oldProp, Properties newProp) {
        //通过反射动态获取实现Action接口的实现类，然后执行实现类里面的doAction
        try {
            CommonUtil.getAllActionSubClass("com.yongqing.etcd.action.Action").forEach(actionClass -> {
                try {
                    Action action = (Action) actionClass.newInstance();
                    action.doAction(oldProp, newProp);
                } catch (Exception e) {
                    log.error("action cause Exception", e);
                }
            });
        } catch (Throwable e) {
            log.error("action cause Exception", e);
        }
    }

    public static Class<?> loadActionClass(String actionClassName) throws ClassNotFoundException {
        return ClassLoader.getSystemClassLoader().loadClass(actionClassName);
    }

    public static void loadActionClasses(List<String> actionClassNames) throws ClassNotFoundException {
        if (null != actionClassNames && actionClassNames.size() > 0) {
            for (String actionClassName : actionClassNames) {
                ClassLoader.getSystemClassLoader().loadClass(actionClassName);
            }
        }
    }

    public static <T, R> R distributedLockExec(Exec exec, String name, long ttl) {
        return distributedLockExec(exec, null, name, ttl);
    }

    public static <T, R> R distributedLockExec(Exec exec, T parameters, String name, long ttl) {
        return distributedLockExec(exec, parameters, name, ttl, null);
    }

    /**
     * @param exec
     * @param parameters
     * @param name       the identifier for the distributed shared lock to be acquired.
     * @param ttl
     * @param <T>
     * @param <R>
     * @return
     */

    public static <T, R> R distributedLockExec(Exec exec, T parameters, String name, long ttl, Long timeOutMs) {
        Lock lock = null;
        LockResponse response = null;
        try {
            CompletableFuture<LockResponse> feature;
            lock = EtcdUtil.getEtclClient().getLockClient();
            if (null != timeOutMs && timeOutMs > 0) {
                feature = lock.lock(ByteSequence.fromString(name), EtcdUtil.getEtclClient().getLeaseClient().grant(ttl).get(timeOutMs, TimeUnit.MILLISECONDS).getID());
            } else {
                feature = lock.lock(ByteSequence.fromString(name), EtcdUtil.getEtclClient().getLeaseClient().grant(ttl).get().getID());
            }
            response = feature.get();
            return exec.exec(parameters);
        } catch (Exception e) {
            log.error("etcd distributed lock  exec failed", e);
            throw new RuntimeException("etcd distributed lock  exec failed "+e.getMessage());
        } finally {
            if (null != lock && null != response) {
                try {
                    lock.unlock(response.getKey()).get();
                    lock.close();
                } catch (Exception e) {
                    log.error("etcd lock cause Exception", e);
                }
            } else if (null != lock) {
                lock.close();
            }
        }
    }

}
