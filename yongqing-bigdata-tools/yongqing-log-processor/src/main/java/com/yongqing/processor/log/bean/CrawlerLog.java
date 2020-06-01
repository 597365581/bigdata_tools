package com.yongqing.processor.log.bean;

import java.util.Map;

/**
 *  爬虫结果数据
 */
public class CrawlerLog {
    private String taskId;
    //1 、实时爬虫 2、离线爬虫
    private String taskType;
    private String businessSystem;
    //业务id  必填，业务ID（每个业务不可重复，建议未来统一生成和分配，在接入统一采集）
    private String businessId;
    //业务类型 必填，业务类型（统一定义）
    private String businessType;
    //业务名称 必填，业务名称
    private String businessName;
    private Map<String,Object> customizeField;
    //采集标志 必填，采集标志，用于采集判断，不带此标志的数据不采集。固定为 crawlerCollection
    private String collectionSign = "crawlerCollection";
    private String crawlerStarttime;
    private String crawlerEndtime;

    public String getCrawlerStarttime() {
        return crawlerStarttime;
    }

    public void setCrawlerStarttime(String crawlerStarttime) {
        this.crawlerStarttime = crawlerStarttime;
    }

    public String getCrawlerEndtime() {
        return crawlerEndtime;
    }

    public void setCrawlerEndtime(String crawlerEndtime) {
        this.crawlerEndtime = crawlerEndtime;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getBusinessSystem() {
        return businessSystem;
    }

    public void setBusinessSystem(String businessSystem) {
        this.businessSystem = businessSystem;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public String getBusinessType() {
        return businessType;
    }

    public void setBusinessType(String businessType) {
        this.businessType = businessType;
    }

    public String getBusinessName() {
        return businessName;
    }

    public void setBusinessName(String businessName) {
        this.businessName = businessName;
    }

    public Map<String, Object> getCustomizeField() {
        return customizeField;
    }

    public void setCustomizeField(Map<String, Object> customizeField) {
        this.customizeField = customizeField;
    }

    public String getCollectionSign() {
        return collectionSign;
    }

    public void setCollectionSign(String collectionSign) {
        this.collectionSign = collectionSign;
    }

    @Override
    public String toString() {
        return "CrawlerLog{" +
                "taskId='" + taskId + '\'' +
                ", taskType='" + taskType + '\'' +
                ", businessSystem='" + businessSystem + '\'' +
                ", businessId='" + businessId + '\'' +
                ", businessType='" + businessType + '\'' +
                ", businessName='" + businessName + '\'' +
                ", customizeField=" + customizeField +
                ", collectionSign='" + collectionSign + '\'' +
                ", crawlerStarttime='" + crawlerStarttime + '\'' +
                ", crawlerEndtime='" + crawlerEndtime + '\'' +
                '}';
    }
}
