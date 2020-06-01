package com.yongqing.hive.tool;

import com.yongqing.hive.tool.constant.Constants;
import lombok.extern.log4j.Log4j2;
import org.apache.hive.hcatalog.streaming.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.hive.hcatalog.streaming.StreamingIOFailure;

/**
 * hive写入
 */
@Log4j2
public class HiveWriter {

    private final HiveEndPoint endPoint;
    private HiveDataSerializer serializer;
    private final StreamingConnection connection;
    private final int txnsPerBatch;
    private final RecordWriter recordWriter;
    private TransactionBatch txnBatch;
    private final ExecutorService callTimeoutPool;

    private int batchCounter;
    private long eventCounter;
    private long processSize;

    private final long callTimeout;

    private long lastUsed; // time of last flush on this writer

    protected boolean closed; // flag indicating HiveWriter was closed
    private boolean autoCreatePartitions;

    private boolean hearbeatNeeded = false;

    private final int writeBatchSz = 5;
    private ArrayList<byte[]> batch = new ArrayList<byte[]>(writeBatchSz);


    public HiveWriter(HiveEndPoint endPoint, int txnsPerBatch,
                      boolean autoCreatePartitions, long callTimeout,
                      ExecutorService callTimeoutPool, String hiveUser,
                      HiveDataSerializer serializer)
            throws ConnectException, InterruptedException {
        try {
            this.autoCreatePartitions = autoCreatePartitions;
            this.callTimeout = callTimeout;
            this.callTimeoutPool = callTimeoutPool;
            this.endPoint = endPoint;
            this.connection = newConnection(hiveUser);
            this.txnsPerBatch = txnsPerBatch;
            this.serializer = serializer;
            this.recordWriter = serializer.createRecordWriter(endPoint);
            this.txnBatch = nextTxnBatch(recordWriter);
            this.txnBatch.beginNextTransaction();
            this.closed = false;
            this.lastUsed = System.currentTimeMillis();
        } catch (InterruptedException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectException(endPoint, e);
        }
    }

    public String getEndPointDatabase() {
        return this.endPoint.database;
    }

    public String getEndPointTable() {
        return this.endPoint.table;
    }

    @Override
    public String toString() {
        return endPoint.toString();
    }

    public void setHearbeatNeeded() {
        hearbeatNeeded = true;
    }

    public int getRemainingTxns() {
        return txnBatch.remainingTransactions();
    }

    public synchronized void write(final byte[] event)
            throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("Writer closed. Cannot write to : " + endPoint);
        }

        batch.add(event);
        if (batch.size() == writeBatchSz) {
            log.info("writeEventBatchToSerializer exec....");
            // write the event
            writeEventBatchToSerializer();
        }

        // Update Statistics
        processSize += event.length;
        eventCounter++;
    }

    private void resetCounters() {
        eventCounter = 0;
        processSize = 0;
        batchCounter = 0;
    }

    private void writeEventBatchToSerializer()
            throws InterruptedException, HiveException {
        try {
            timedCall(new CallRunner1<Void>() {
                @Override
                public Void call() throws InterruptedException, StreamingException {
                    try {
                        for (byte[] event : batch) {
                            try {
                                log.info("writeEventBatchToSerializer execute....");
                                serializer.write(txnBatch, event);
                            } catch (SerializationError err) {
                                log.info("Parse failed : {}  : {}", err.getMessage(), new String(event));
                            }
                        }
                        return null;
                    } catch (IOException e) {
                        throw new StreamingIOFailure(e.getMessage(), e);
                    }
                }
            });
            batch.clear();
        } catch (StreamingException e) {
            throw new HiveException(endPoint, txnBatch.getCurrentTxnId(), e);
        } catch (TimeoutException e) {
            throw new HiveException(endPoint, txnBatch.getCurrentTxnId(), e);
        }
    }


    public void flush(boolean rollToNext)
            throws CommitException, TxnBatchException, TxnFailure, InterruptedException,
            WriteException {
        if (!batch.isEmpty()) {
            writeEventBatchToSerializer();
            batch.clear();
        }

        //0 Heart beat on TxnBatch
        if (hearbeatNeeded) {
            hearbeatNeeded = false;
            heartBeat();
        }
        lastUsed = System.currentTimeMillis();

        try {
            //1 commit txn & close batch if needed
            commitTxn();
            if (txnBatch.remainingTransactions() == 0) {
                closeTxnBatch();
                txnBatch = null;
                if (rollToNext) {
                    txnBatch = nextTxnBatch(recordWriter);
                }
            }

            //2 roll to next Txn
            if (rollToNext) {
                log.debug("Switching to next Txn for {}", endPoint);
                txnBatch.beginNextTransaction(); // does not block
            }
            log.info("flush finish...");
        } catch (StreamingException e) {
            throw new TxnFailure(txnBatch, e);
        }
    }

    long getLastUsed() {
        return lastUsed;
    }

    public void abort() throws InterruptedException {
        batch.clear();
        abortTxn();
    }


    public void heartBeat() throws InterruptedException {
        // 1) schedule the heartbeat on one thread in pool
        try {
            timedCall(new CallRunner1<Void>() {
                @Override
                public Void call() throws StreamingException {
                    log.info("Sending heartbeat on batch " + txnBatch);
                    txnBatch.heartbeat();
                    return null;
                }
            });
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Unable to send heartbeat on Txn Batch " + txnBatch, e);
            // Suppressing exceptions as we don't care for errors on heartbeats
        }
    }


    public void close() throws InterruptedException {
        batch.clear();
        abortRemainingTxns();
        closeTxnBatch();
        closeConnection();
        closed = true;
    }

    public void closeCallTimeoutPool() {
//        if (null != callTimeoutPool && (!callTimeoutPool.isShutdown() || !callTimeoutPool.isTerminated())) {
//            callTimeoutPool.shutdownNow();
//        }
        if (null != callTimeoutPool && (!callTimeoutPool.isShutdown() || !callTimeoutPool.isTerminated())){
            callTimeoutPool.shutdown();
            try {
                while (callTimeoutPool.isTerminated() == false) {
                    callTimeoutPool.awaitTermination(
                            Math.max(Constants.DEFAULT_CALLTIMEOUT, callTimeout), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                log.warn("hive writer :Shutdown interrupted on " + callTimeoutPool, ex);
            }
        }

    }


    public void closeConnection() throws InterruptedException {
        log.info("Closing connection to EndPoint : {}", endPoint);
        try {
            timedCall(new CallRunner1<Void>() {
                @Override
                public Void call() {
                    connection.close(); // could block
                    return null;
                }
            });
        } catch (Exception e) {
            log.warn("Error closing connection to EndPoint : " + endPoint, e);
            // Suppressing exceptions as we don't care for errors on connection close
        }
    }


    private StreamingConnection newConnection(final String proxyUser)
            throws InterruptedException, ConnectException {
        try {
            return timedCall(new CallRunner1<StreamingConnection>() {
                @Override
                public StreamingConnection call() throws InterruptedException, StreamingException {
                    return endPoint.newConnection(autoCreatePartitions); // could block
                }
            });
        } catch (Exception e) {
            throw new ConnectException(endPoint, e);
        }
    }


    private boolean isClosed(TransactionBatch.TxnState txnState) {
        if (txnState == TransactionBatch.TxnState.COMMITTED) {
            return true;
        }
        if (txnState == TransactionBatch.TxnState.ABORTED) {
            return true;
        }
        return false;
    }


    private void abortRemainingTxns() throws InterruptedException {
        try {
            if (!isClosed(txnBatch.getCurrentTransactionState())) {
                abortCurrTxnHelper();
            }

            // recursively abort remaining txns
            if (txnBatch.remainingTransactions() > 0) {
                timedCall(
                        new CallRunner1<Void>() {
                            @Override
                            public Void call() throws StreamingException, InterruptedException {
                                txnBatch.beginNextTransaction();
                                return null;
                            }
                        });
                abortRemainingTxns();
            }
        } catch (StreamingException e) {
            log.warn("Error when aborting remaining transactions in batch " + txnBatch, e);
            return;
        } catch (TimeoutException e) {
            log.warn("Timed out when aborting remaining transactions in batch " + txnBatch, e);
            return;
        }
    }


    private void abortCurrTxnHelper() throws TimeoutException, InterruptedException {
        try {
            timedCall(
                    new CallRunner1<Void>() {
                        @Override
                        public Void call() throws StreamingException, InterruptedException {
                            txnBatch.abort();
                            log.info("Aborted txn " + txnBatch.getCurrentTxnId());
                            return null;
                        }
                    }
            );
        } catch (StreamingException e) {
            log.warn("Unable to abort transaction " + txnBatch.getCurrentTxnId(), e);
            // continue to attempt to abort other txns in the batch
        }
    }


    private void abortTxn() throws InterruptedException {
        log.info("Aborting Txn id {} on End Point {}", txnBatch.getCurrentTxnId(), endPoint);
        try {
            timedCall(new CallRunner1<Void>() {
                @Override
                public Void call() throws StreamingException, InterruptedException {
                    txnBatch.abort(); // could block
                    return null;
                }
            });
        } catch (InterruptedException e) {
            throw e;
        } catch (TimeoutException e) {
            log.warn("Timeout while aborting Txn " + txnBatch.getCurrentTxnId() +
                    " on EndPoint: " + endPoint, e);
        } catch (Exception e) {
            log.warn("Error aborting Txn " + txnBatch.getCurrentTxnId() + " on EndPoint: " + endPoint, e);
            // Suppressing exceptions as we don't care for errors on abort
        }
    }

    private TransactionBatch nextTxnBatch(final RecordWriter recordWriter)
            throws InterruptedException, TxnBatchException {
        log.debug("Fetching new Txn Batch for {}", endPoint);
        TransactionBatch batch = null;
        try {
            batch = timedCall(new CallRunner1<TransactionBatch>() {
                @Override
                public TransactionBatch call() throws InterruptedException, StreamingException {
                    return connection.fetchTransactionBatch(txnsPerBatch, recordWriter); // could block
                }
            });
            log.info("Acquired Transaction batch {}", batch);
        } catch (Exception e) {
            throw new TxnBatchException(endPoint, e);
        }
        return batch;
    }


    private void closeTxnBatch() throws InterruptedException {
        try {
            log.info("Closing Txn Batch {}.", txnBatch);
            timedCall(new CallRunner1<Void>() {
                @Override
                public Void call() throws InterruptedException, StreamingException {
                    txnBatch.close(); // could block
                    return null;
                }
            });
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Error closing Txn Batch " + txnBatch, e);
            // Suppressing exceptions as we don't care for errors on batch close
        }
    }


    private void commitTxn() throws CommitException, InterruptedException {
        if (log.isInfoEnabled()) {
            log.info("Committing Txn " + txnBatch.getCurrentTxnId() + " on EndPoint: " + endPoint);
        }
        try {
            timedCall(new CallRunner1<Void>() {
                @Override
                public Void call() throws StreamingException, InterruptedException {
                    txnBatch.commit(); // could block
                    return null;
                }
            });
        } catch (Exception e) {
            throw new CommitException(endPoint, txnBatch.getCurrentTxnId(), e);
        }
    }


    private interface CallRunner<T> {
        T call() throws Exception;
    }

    private interface CallRunner1<T> {
        T call() throws StreamingException, InterruptedException, Failure;
    }

    private <T> T timedCall(final CallRunner1<T> callRunner)
            throws TimeoutException, InterruptedException, StreamingException {
        Future<T> future = callTimeoutPool.submit(new Callable<T>() {
            @Override
            public T call() throws StreamingException, InterruptedException, Failure {
                return callRunner.call();
            }
        });

        try {
            if (callTimeout > 0) {
                return future.get(callTimeout, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (TimeoutException eT) {
            future.cancel(true);

            throw eT;
        } catch (ExecutionException e1) {

            Throwable cause = e1.getCause();
            if (cause instanceof IOException) {
                throw new StreamingException("I/O Failure", (IOException) cause);
            } else if (cause instanceof StreamingException) {
                throw (StreamingException) cause;
            } else if (cause instanceof TimeoutException) {
                throw new StreamingException("Operation Timed Out.", (TimeoutException) cause);
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            }
            throw new StreamingException(e1.getMessage(), e1);
        }
    }

    public static class Failure extends Exception {
        public Failure(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    public static class WriteException extends Failure {
        public WriteException(HiveEndPoint endPoint, Long currentTxnId, Throwable cause) {
            super("Failed writing to : " + endPoint + ". TxnID : " + currentTxnId, cause);
        }
    }

    public static class CommitException extends Failure {
        public CommitException(HiveEndPoint endPoint, Long txnID, Throwable cause) {
            super("Commit of Txn " + txnID + " failed on EndPoint: " + endPoint, cause);
        }
    }

    public static class ConnectException extends Failure {
        public ConnectException(HiveEndPoint ep, Throwable cause) {
            super("Failed connecting to EndPoint " + ep, cause);
        }
    }

    public static class TxnBatchException extends Failure {
        public TxnBatchException(HiveEndPoint ep, Throwable cause) {
            super("Failed acquiring Transaction Batch from EndPoint: " + ep, cause);
        }
    }

    public class TxnFailure extends Failure {
        public TxnFailure(TransactionBatch txnBatch, Throwable cause) {
            super("Failed switching to next Txn in TxnBatch " + txnBatch, cause);
        }
    }


}
