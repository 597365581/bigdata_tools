package com.yongqing.processor.log.bean;

import com.yongqing.processor.log.Processor;

/**
 *
 */
public class ProcessorBean {
    private String processor;
    private String logType;
    private Processor processorInstance;

    public Processor getProcessorInstance() {
        return processorInstance;
    }

    public void setProcessorInstance(Processor processorInstance) {
        this.processorInstance = processorInstance;
    }

    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    @Override
    public String toString() {
        return "ProcessorBean{" +
                "processor='" + processor + '\'' +
                ", logType='" + logType + '\'' +
                ", processorInstance=" + processorInstance +
                '}';
    }
}
