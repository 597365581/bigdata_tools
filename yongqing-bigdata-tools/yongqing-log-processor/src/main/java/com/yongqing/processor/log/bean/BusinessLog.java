package com.yongqing.processor.log.bean;

import java.util.Map;

/**
 * 业务日志
 */
public class BusinessLog {
    //系统名称  必填，业务系统(系统的唯一英文简称)
    private String businessSystem;
    //业务id  必填，业务ID（每个业务不可重复，建议未来统一生成和分配，在接入统一采集）
    private String businessId;
    //业务类型 必填，业务类型（统一定义）
    private String businessType;
    //业务名称 必填，业务名称
    private String businessName;
    //业务日志打印时间 必填，系统统一生成获取(业务日志打印时间)
    private String businessLogTime;
    //日志ID  必填，系统统一生成获取
    private String businessLogId;
    //自定义字段这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ "key1":"value1","key2":"value2" }
    private Map<String,Object> customizeField;
    //采集标志 必填，采集标志，用于采集判断，不带此标志的数据不采集。固定为 businessCollection
    private String collectionSign = "businessCollection";

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

    public String getBusinessLogTime() {
        return businessLogTime;
    }

    public void setBusinessLogTime(String businessLogTime) {
        this.businessLogTime = businessLogTime;
    }

    public String getBusinessLogId() {
        return businessLogId;
    }

    public void setBusinessLogId(String businessLogId) {
        this.businessLogId = businessLogId;
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
}
