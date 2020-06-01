package com.yongqing.zookeeper.tool.client;

import com.yongqing.zookeeper.tool.distributed.DistributedLock;
import com.yongqing.zookeeper.tool.exec.Exec;
import lombok.extern.log4j.Log4j2;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Log4j2
public class AbstractZKClient implements ZKClient, DistributedLock {
    private CuratorFramework zkClient = null;
    private AsyncCuratorFramework asyncCuratorFramework;

    public AbstractZKClient(String zookeeperConnectionString, RetryPolicy retryPolicy) {
        zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        asyncCuratorFramework = AsyncCuratorFramework.wrap(zkClient);
        zkClient.start();
        initStateLister();
    }

    public AbstractZKClient(String zookeeperConnectionString, Integer zookeeperRetrySleep, Integer zookeeperRetryMaxtime) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(zookeeperRetrySleep, zookeeperRetryMaxtime);
        zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        asyncCuratorFramework = AsyncCuratorFramework.wrap(zkClient);
        zkClient.start();
        initStateLister();
    }

    public AbstractZKClient(String zookeeperConnectionString, Integer zookeeperRetrySleep, Integer zookeeperRetryMaxtime, Integer zookeeperSessionTimeout, Integer zookeeperConnectionTimeout) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(zookeeperRetrySleep, zookeeperRetryMaxtime);
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(zookeeperConnectionString)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(1000 * zookeeperSessionTimeout)
                .connectionTimeoutMs(1000 * zookeeperConnectionTimeout)
                .build();
        asyncCuratorFramework = AsyncCuratorFramework.wrap(zkClient);
        zkClient.start();
        initStateLister();
    }


    public void pathChildrenCache(String path, PathChildrenCacheListener pathChildrenCacheListener, boolean cacheData) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(zkClient, path, cacheData);
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start();
    }

    public void pathChildrenCache(PathChildrenCache pathChildrenCache, PathChildrenCacheListener pathChildrenCacheListener) throws Exception {
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start();
    }

    public void nodeCache(NodeCache nodeCache, NodeCacheListener nodeCacheListener) throws Exception {
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start();
    }

    public void nodeCache(String path, NodeCacheListener nodeCacheListener, boolean dataIsCompressed) throws Exception {
        NodeCache nodeCache = new NodeCache(zkClient, path, dataIsCompressed);
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start();
    }

    public void treeCache(TreeCache treeCache, TreeCacheListener treeCacheListener) throws Exception {
        treeCache.getListenable().addListener(treeCacheListener);
        treeCache.start();
    }

    public void treeCache(String path, boolean cacheData, TreeCacheListener treeCacheListener) throws Exception {
        TreeCache treeCache = TreeCache.newBuilder(zkClient, path).setCacheData(cacheData).build();
        treeCache.getListenable().addListener(treeCacheListener);
        treeCache.start();
    }

    public void createZNode(String path, byte[] payload) throws Exception {

        zkClient.create().forPath(path, payload);
    }

    public void createEphemeral(String path, byte[] payload) throws Exception {
        zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    public String createEphemeralSequential(String path, byte[] payload) throws Exception {
        // this will create the given EPHEMERAL-SEQUENTIAL ZNode with the given data using Curator protection.

        /*
            Protection Mode:
            It turns out there is an edge case that exists when creating sequential-ephemeral nodes. The creation
            can succeed on the server, but the server can crash before the created node name is returned to the
            client. However, the ZK session is still valid so the ephemeral node is not deleted. Thus, there is no
            way for the client to determine what node was created for them.
            Even without sequential-ephemeral, however, the create can succeed on the sever but the client (for various
            reasons) will not know it. Putting the create builder into protection mode works around this. The name of
            the node that is created is prefixed with a GUID. If node creation fails the normal retry mechanism will
            occur. On the retry, the parent path is first searched for a node that has the GUID in it. If that node is
            found, it is assumed to be the lost node that was successfully created on the first try and is returned to
            the caller.
         */
        return zkClient.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
    }

    public void setData(String path, byte[] payload) throws Exception {

        zkClient.setData().forPath(path, payload);
    }

    public void setDataAsync(String path, byte[] payload, CuratorListener listener) throws Exception {

        zkClient.getCuratorListenable().addListener(listener);

        zkClient.setData().inBackground().forPath(path, payload);
    }

    public void setDataAsyncWithCallback(BackgroundCallback callback, String path, byte[] payload) throws Exception {
        zkClient.setData().inBackground(callback).forPath(path, payload);
    }

    public void delete(String path) throws Exception {

        zkClient.delete().forPath(path);
    }

    public <T, R> R distributedLockExec(String lockPath, Exec exec, T parameters) {
        return distributedLockExec(lockPath, exec, parameters, null, null);
    }


    public void guaranteedDelete(String path) throws Exception {


        /*
            Guaranteed Delete
            Solves this edge case: deleting a node can fail due to connection issues. Further, if the node was
            ephemeral, the node will not get auto-deleted as the session is still valid. This can wreak havoc
            with lock implementations.
            When guaranteed is set, Curator will record failed node deletions and attempt to delete them in the
            background until successful. NOTE: you will still get an exception when the deletion fails. But, you
            can be assured that as long as the CuratorFramework instance is open attempts will be made to delete
            the node.
         */

        zkClient.delete().guaranteed().forPath(path);
    }

    public List<String> watchedGetChildren(String path) throws Exception {

        return zkClient.getChildren().watched().forPath(path);
    }

    public List<String> watchedGetChildren(String path, Watcher watcher) throws Exception {

        return zkClient.getChildren().usingWatcher(watcher).forPath(path);
    }

    public <T, R> R distributedLockExec(String lockPath, Exec exec, T parameters, Long time, TimeUnit unit) {
        InterProcessMutex lock = null;
        try {
            lock = new InterProcessMutex(zkClient, lockPath);
            if (null == time && null == unit) {
                lock.acquire();
                return exec.exec(parameters);
            } else if (null != time && null != unit) {
                if (lock.acquire(time, unit)) {
                    return exec.exec(parameters);
                } else {
                    log.error("zk distributedLockExec timeout...");
                    throw new RuntimeException("zk distributedLockExec timeout...");
                }
            } else {
                log.error("zk distributedLockExec   time or unit is null");
                throw new RuntimeException("zk distributedLockExec   time or unit is null");
            }
        } catch (Exception e) {
            log.error("zk distributed lock  exec failed", e);
            throw new RuntimeException("zk distributed lock  exec failed "+e.getMessage());
        } finally {
            try {
                if (null != lock) {
                    lock.release();
                }
            } catch (Exception e) {
                log.error("zk distributed lock  relase failed", e);
            }
        }
    }


    public <T> void distributedLockExecNoReturn(String lockPath, Exec exec, Long time, TimeUnit unit) {
        distributedLockExec(lockPath, exec, null, time, unit);
    }

    public <T> void distributedLockExecNoReturn(String lockPath, Exec exec) {
        distributedLockExec(lockPath, exec, null);
    }

    public <T> void distributedLockExecNoReturn(String lockPath, Exec exec, T parameters) {
        distributedLockExec(lockPath, exec, parameters);
    }

    public <T> void distributedLockExecNoReturn(String lockPath, Exec exec, T parameters, Long time, TimeUnit unit) {
        distributedLockExec(lockPath, exec, parameters, time, unit);
    }

    public <T, R> R distributedLockExec(String lockPath, Exec exec) {
        return distributedLockExec(lockPath, exec, null);
    }

    public <T, R> R distributedLockExec(String lockPath, Exec exec, Long time, TimeUnit unit) {
        return distributedLockExec(lockPath, exec, null, time, unit);
    }

    private void initStateLister() {
        if (zkClient == null) {
            return;
        }
        ConnectionStateListener csLister = (client, newState) -> {
            log.info("state changed , current state : " + newState.name());
            /**
             * probably session expired
             */
            if (newState == ConnectionState.LOST) {
                // if lost , then exit
                log.info("current zookeepr connection state : connection lost ");
            }
        };

        zkClient.getConnectionStateListenable().addListener(csLister);
    }

    public void start() {
        if (null != zkClient) {
            if (!zkClient.isStarted()) {
                zkClient.start();
            }
            log.info("zookeeper start ...");
        } else {
            log.info("zkClient need to init,please check...");
        }
    }

    public CuratorFramework getZkClient() {
        return this.zkClient;
    }

    public AsyncCuratorFramework getAsyncCuratorFramework() {
        return this.asyncCuratorFramework;
    }

    public void close() {
        if (null != zkClient) {
            zkClient.getZookeeperClient().close();
            zkClient.close();
            log.info("zookeeper close ...");
        }
    }

    public Collection<CuratorTransactionResult> getCreateOpCuratorTransactionResult(String path, byte[] payload) throws Exception {
        CuratorOp createOp = zkClient.transactionOp().create().forPath(path, payload);
        return getCuratorTransactionResult(createOp);
    }

    public Collection<CuratorTransactionResult> getSetDataOpCuratorTransactionResult(String path, byte[] payload) throws Exception {
        CuratorOp setDataOp = zkClient.transactionOp().setData().forPath(path, payload);
        return getCuratorTransactionResult(setDataOp);
    }

    public Collection<CuratorTransactionResult> getSetDataOpCuratorTransactionResult(String path) throws Exception {
        CuratorOp deleteOp = zkClient.transactionOp().delete().forPath(path);
        return getCuratorTransactionResult(deleteOp);
    }

    public Collection<CuratorTransactionResult> getCuratorTransactionResult(CuratorOp curatorOp) throws Exception {

        return zkClient.transaction().forOperations(curatorOp);
    }

    public Collection<CuratorTransactionResult> getCuratorTransactionResults(CuratorOp... curatorOp) throws Exception {

        return zkClient.transaction().forOperations(curatorOp);
    }
}
