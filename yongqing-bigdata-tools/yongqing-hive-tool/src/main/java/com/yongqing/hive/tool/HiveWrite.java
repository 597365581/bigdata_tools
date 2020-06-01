package com.yongqing.hive.tool;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yongqing.hive.tool.pojo.DefaultEvent;
import com.yongqing.hive.tool.pojo.FieldDealPojo;
import com.yongqing.hive.tool.queue.EventQueue;
import lombok.extern.log4j.Log4j2;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.yongqing.hive.tool.constant.Constants.*;

/**
 * hive写入
 */
@Log4j2
public class HiveWrite {
    private List<String> partitionVals;
    private Integer txnsPerBatchAsk;
    private Integer batchSize;
    private Integer maxOpenConnections;
    private boolean autoCreatePartitions;
    private HiveDataSerializer serializer;
    private ExecutorService callTimeoutPool;
    private String metaStoreUri;
    private String proxyUser;
    private String database;
    private String table;
    private volatile int idleTimeout;
    private boolean needRounding;
    private int roundUnit;
    private Integer roundValue;
    private boolean useLocalTime;
    private Map<HiveEndPoint, HiveWriter> allWriters;
    private Integer callTimeout;
    private Integer heartBeatInterval;
    private Timer heartBeatTimer = new Timer();
    private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);
    private TimeZone timeZone;
    private FieldDealPojo fieldDealPojo;
    private String dataType;


    public HiveWrite(String metaStoreUri, String proxyUser, String database, String table, String tzName, FieldDealPojo fieldDealPojo, String dataType) {
        this.metaStoreUri = metaStoreUri;
        this.proxyUser = proxyUser;
        this.database = database;
        this.table = table;
        this.partitionVals = null;
        this.txnsPerBatchAsk = DEFAULT_TXNSPERBATCH;
        this.batchSize = DEFAULT_BATCHSIZE;
        this.idleTimeout = DEFAULT_IDLETIMEOUT;
        this.callTimeout = DEFAULT_CALLTIMEOUT;
        this.heartBeatInterval = DEFAULT_HEARTBEATINTERVAL;
        this.maxOpenConnections = DEFAULT_MAXOPENCONNECTIONS;
        this.autoCreatePartitions = false;
        this.useLocalTime = false;
        this.timeZone = (tzName == null) ? null : TimeZone.getTimeZone(tzName);
        this.needRounding = false;
        this.roundUnit = 12;
        this.roundValue = 1;
        this.fieldDealPojo = fieldDealPojo;
        this.dataType = dataType;
        init();
    }

    public HiveWrite(String metaStoreUri, String proxyUser, String database, String table, String hivePartition, String tzName, FieldDealPojo fieldDealPojo, String dataType) {
        this.metaStoreUri = metaStoreUri;
        this.proxyUser = proxyUser;
        this.database = database;
        this.table = table;
        this.partitionVals = Arrays.asList(hivePartition.split(","));
        this.txnsPerBatchAsk = DEFAULT_TXNSPERBATCH;
        this.batchSize = DEFAULT_BATCHSIZE;
        this.idleTimeout = DEFAULT_IDLETIMEOUT;
        this.callTimeout = DEFAULT_CALLTIMEOUT;
        this.heartBeatInterval = DEFAULT_HEARTBEATINTERVAL;
        this.maxOpenConnections = DEFAULT_MAXOPENCONNECTIONS;
        this.autoCreatePartitions = false;
        this.useLocalTime = false;
        this.timeZone = (tzName == null) ? null : TimeZone.getTimeZone(tzName);
        this.needRounding = false;
        this.roundUnit = 12;
        this.roundValue = 1;
        this.fieldDealPojo = fieldDealPojo;
        this.dataType = dataType;
        init();
    }


    public HiveWrite(String metaStoreUri, String proxyUser, String database, String table, String hivePartition, Integer hiveTxnsPerBatchAsk, Integer batchSize, Integer idleTimeout, Integer callTimeout, Integer heartBeatInterval, Integer maxOpenConnections, Boolean autoCreatePartitions, Boolean useLocalTime, String tzName, Boolean needRounding, Integer roundUnit, Integer roundValue, FieldDealPojo fieldDealPojo, String dataType) {
        this.metaStoreUri = metaStoreUri;
        this.proxyUser = proxyUser;
        this.database = database;
        this.table = table;
        this.partitionVals = Arrays.asList(hivePartition.split(","));
        this.txnsPerBatchAsk = hiveTxnsPerBatchAsk;
        this.batchSize = batchSize;
        this.idleTimeout = idleTimeout;
        this.callTimeout = callTimeout;
        this.heartBeatInterval = heartBeatInterval;
        this.maxOpenConnections = maxOpenConnections;
        this.autoCreatePartitions = autoCreatePartitions;
        this.useLocalTime = useLocalTime;
        this.timeZone = (tzName == null) ? null : TimeZone.getTimeZone(tzName);
        this.needRounding = needRounding;
        this.roundUnit = roundUnit;
        this.roundValue = roundValue;
        this.fieldDealPojo = fieldDealPojo;
        this.dataType = dataType;
        init();
    }

    private void init() {
        if (null == dataType || "DELIMITED".equals(dataType)) {
            this.serializer = new HiveDataSerializerImpl(parseDelimiterSpec(fieldDealPojo.getDelimiter()), parseSerdeSeparatorSpec(fieldDealPojo.getSerdeSeparator()), fieldDealPojo.getFieldNames().trim().split(",", -1));
        } else if ("JSON".equals(dataType)) {
            this.serializer = new HiveJsonDataSerializerImpl();
        } else {
            throw new RuntimeException("dataType:" + dataType + " is not support");
        }
        this.allWriters = Maps.newHashMap();
        callTimeoutPool = Executors.newFixedThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("Hive Write").build());
        setupHeartBeatTimer();
        if ((this.roundUnit == Calendar.SECOND || this.roundUnit == Calendar.MINUTE) && !(roundValue > 0 && roundValue <= 60)) {
            throw new HiveException("Round value must be > 0 and <= 60");
        } else if (roundUnit == Calendar.HOUR_OF_DAY && !(roundValue > 0 && roundValue <= 24)) {
            throw new HiveException("Round value must be > 0 and <= 24");
        }
    }

    public void close() {
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                HiveWriter hiveWriter = entry.getValue();
                hiveWriter.close();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        // shut down all thread pools
        callTimeoutPool.shutdown();
        try {
            while (callTimeoutPool.isTerminated() == false) {
                callTimeoutPool.awaitTermination(
                        Math.max(DEFAULT_CALLTIMEOUT, callTimeout), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ex) {
            log.warn("hive write :Shutdown interrupted on " + callTimeoutPool, ex);
        }

        callTimeoutPool = null;
        allWriters.clear();
        allWriters = null;
        log.info("Hive Write  stopped");
    }


    public int addByteToWrite(byte[] bytes) throws HiveWriter.Failure, InterruptedException {
        Event event = new DefaultEvent();
        event.setBody(bytes);
        event.setHeaders(new HashMap<String, String>());
        return addEventToWrite(event);
    }

    public int addEventToWrite(Event event) throws HiveWriter.Failure, InterruptedException {
        if (timeToSendHeartBeat.compareAndSet(true, false)) {
            enableHeartBeatOnAllWriters();
        }
        int txnEventCount = 0;
        try {
            Map<HiveEndPoint, HiveWriter> activeWriters = Maps.newHashMap();

            if (event == null) {
                return 0;
            }
            HiveEndPoint endPoint = makeEndPoint(metaStoreUri, database, table,
                    partitionVals, event.getHeaders(), timeZone,
                    needRounding, roundUnit, roundValue, useLocalTime);

            HiveWriter writer = getOrCreateWriter(activeWriters, endPoint);
            //3) Write
            log.debug(" Writing event to {}", endPoint);
            writer.write(event.getBody());
            txnEventCount = 1;
            // 4) Flush all Writers
            for (HiveWriter activeWriter : activeWriters.values()) {
                activeWriter.flush(true);
            }
            return txnEventCount;
        } catch (HiveWriter.Failure e) {
            log.warn("addEventToWrite : " + e.getMessage(), e);
            abortAllWriters();
            closeAllWriters();
            throw e;
        }
    }

    public int addEventBatchToWrite(Event event) throws HiveWriter.Failure, InterruptedException {
        if (timeToSendHeartBeat.compareAndSet(true, false)) {
            enableHeartBeatOnAllWriters();
        }
        EventQueue.getEventQueue().put(event);
        int txnEventCount = 0;
        if (EventQueue.getEventQueue().size() <= batchSize) {
            return txnEventCount;
        }
        try {
            Map<HiveEndPoint, HiveWriter> activeWriters = Maps.newHashMap();
            for (; txnEventCount < batchSize; ++txnEventCount) {
                Event eventTemp = EventQueue.getEventQueue().take();
                if (eventTemp == null) {
                    break;
                }
                HiveEndPoint endPoint = makeEndPoint(metaStoreUri, database, table,
                        partitionVals, eventTemp.getHeaders(), timeZone,
                        needRounding, roundUnit, roundValue, useLocalTime);

                HiveWriter writer = getOrCreateWriter(activeWriters, endPoint);
                //3) Write
                log.debug(" Writing event to {}", endPoint);
                writer.write(eventTemp.getBody());
            }
            // 4) Flush all Writers
            for (HiveWriter writer : activeWriters.values()) {
                writer.flush(true);
            }
            return txnEventCount;
        } catch (HiveWriter.Failure e) {
            log.warn("addEventToWrite : " + e.getMessage(), e);
            abortAllWriters();
            closeAllWriters();
            throw e;
        }
    }

    private void enableHeartBeatOnAllWriters() {
        for (HiveWriter writer : allWriters.values()) {
            writer.setHearbeatNeeded();
        }
    }

    private static Character parseSerdeSeparatorSpec(String separatorStr) {
        if (separatorStr == null) {
            return null;
        }
        if (separatorStr.length() == 1) {
            return separatorStr.charAt(0);
        }
        if (separatorStr.length() == 3 &&
                separatorStr.charAt(2) == '\'' &&
                separatorStr.charAt(separatorStr.length() - 1) == '\'') {
            return separatorStr.charAt(1);
        }

        throw new IllegalArgumentException("serializer.serdeSeparator spec is invalid " +
                "for serializer ");
    }

    private static String parseDelimiterSpec(String delimiter) {
        if (delimiter == null) {
            return null;
        }
        if (delimiter.charAt(0) == '"' &&
                delimiter.charAt(delimiter.length() - 1) == '"') {
            return delimiter.substring(1, delimiter.length() - 1);
        }
        return delimiter;
    }

    private void setupHeartBeatTimer() {
        if (heartBeatInterval > 0) {
            heartBeatTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    timeToSendHeartBeat.set(true);
                    setupHeartBeatTimer();
                }
            }, heartBeatInterval * 1000);
        }
    }

    private void closeAllWriters() throws InterruptedException {
        //1) Retire writers
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            entry.getValue().close();
        }

        //2) Clear cache
        allWriters.clear();
    }


    private void abortAllWriters() throws InterruptedException {
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            entry.getValue().abort();
        }
    }

    private HiveWriter getOrCreateWriter(Map<HiveEndPoint, HiveWriter> activeWriters,
                                         HiveEndPoint endPoint)
            throws HiveWriter.ConnectException, InterruptedException {
        try {
            HiveWriter writer = allWriters.get(endPoint);
            if (null == writer) {
                log.info("Creating Writer to Hive end point : " + endPoint);
                writer = new HiveWriter(endPoint, txnsPerBatchAsk, autoCreatePartitions,
                        callTimeout, callTimeoutPool, proxyUser, serializer);

                if (allWriters.size() > maxOpenConnections) {
                    int retired = closeIdleWriters();
                    if (retired == 0) {
                        closeEldestWriter();
                    }
                }
                allWriters.put(endPoint, writer);
                activeWriters.put(endPoint, writer);
            } else {
                activeWriters.putIfAbsent(endPoint, writer);
            }
            return writer;
        } catch (HiveWriter.ConnectException e) {
            throw e;
        }

    }

    private void closeEldestWriter() throws InterruptedException {
        long oldestTimeStamp = System.currentTimeMillis();
        HiveEndPoint eldest = null;
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            if (entry.getValue().getLastUsed() < oldestTimeStamp) {
                eldest = entry.getKey();
                oldestTimeStamp = entry.getValue().getLastUsed();
            }
        }

        try {

            log.info(": Closing least used Writer to Hive EndPoint : " + eldest);
            allWriters.remove(eldest).close();
        } catch (InterruptedException e) {
            log.warn(": Interrupted when attempting to close writer for end point: "
                    + eldest, e);
            throw e;
        }
    }

    private int closeIdleWriters() throws InterruptedException {
        int count = 0;
        long now = System.currentTimeMillis();
        ArrayList<HiveEndPoint> retirees = Lists.newArrayList();

        //1) Find retirement candidates
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            if (now - entry.getValue().getLastUsed() > idleTimeout) {
                ++count;
                retirees.add(entry.getKey());
            }
        }
        //2) Retire them
        for (HiveEndPoint ep : retirees) {
            log.info(" Closing idle Writer to Hive end point : {}", ep);
            allWriters.remove(ep).close();
        }
        return count;
    }


    private HiveEndPoint makeEndPoint(String metaStoreUri, String database, String table,
                                      List<String> partVals, Map<String, String> headers,
                                      TimeZone timeZone, boolean needRounding,
                                      int roundUnit, Integer roundValue,
                                      boolean useLocalTime) {
        if (partVals == null) {
            return new HiveEndPoint(metaStoreUri, database, table, null);
        }
        //TODO 分区实现
        ArrayList<String> realPartVals = Lists.newArrayList();
        return new HiveEndPoint(metaStoreUri, database, table, realPartVals);
    }
}
