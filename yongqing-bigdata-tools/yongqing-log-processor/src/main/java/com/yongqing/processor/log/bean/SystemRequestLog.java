package com.yongqing.processor.log.bean;
import java.util.Map;

/**
 * 系统请求日志
 */
public class SystemRequestLog {
    //请求发起时间  非必填，一般是用户传入
    private String requestInitiationTime;
    //请求接收时间  必填，系统实际接收到请求的时间
    private String requestReceiveTime;
    //请求日志生成时间  必填，统一获取(日志打印的时间)
    private String requestLogTime;
    //请求的IP地址  必填，请求发起的IP地址，统一获取
    private String requestInitiationIp;
    //请求处理的IP 必填，请求实际被处理的系统的机器IP，统一获取
    private String requestHostIp;
    //请求的服务  必填，请求的系统服务，统一获取
    private String requestApi;
    //请求的url  必填，请求的Url地址，统一获取
    private String requestUrl;
    //请求的参数  必填，请求的参数，统一获取
    private String requestParameter;
    //请求处理完成时间  必填，请求处理完成的时间，统一获取
    private String requestCompletionTime;
    //请求的处理系统  请求实际被处理的系统(系统的唯一英文简称)
    private String requestHostSystem;
    //请求响应的Code  必填
    private String requestCompletionCode;
    //请求结果  必填
    private String requestCompletionResult;
    //请求id  必填，系统统一生成
    private String requestId;
    //请求的应用ID  非必填，主要针对应用接入的情况，比如需要先注册接入，接入后，会分配一个唯一的应用ID，请求时需要传入
    private String appId;
    //请求的签名  非必填，和appId结合使用，用于接口请求的校验
    private String sign;
    //采集标志 必填，采集标志，用于采集判断，不带此标志的数据不采集。固定为systemRequestCollection
    private String collectionSign = "systemRequestCollection";
    //自定义字段这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ "key1":"value1","key2":"value2" }
    private Map<String,Object> customizeField;

    public String getRequestInitiationTime() {
        return requestInitiationTime;
    }

    public void setRequestInitiationTime(String requestInitiationTime) {
        this.requestInitiationTime = requestInitiationTime;
    }

    public String getRequestReceiveTime() {
        return requestReceiveTime;
    }

    public void setRequestReceiveTime(String requestReceiveTime) {
        this.requestReceiveTime = requestReceiveTime;
    }

    public String getRequestLogTime() {
        return requestLogTime;
    }

    public void setRequestLogTime(String requestLogTime) {
        this.requestLogTime = requestLogTime;
    }

    public String getRequestInitiationIp() {
        return requestInitiationIp;
    }

    public void setRequestInitiationIp(String requestInitiationIp) {
        this.requestInitiationIp = requestInitiationIp;
    }

    public String getRequestHostIp() {
        return requestHostIp;
    }

    public void setRequestHostIp(String requestHostIp) {
        this.requestHostIp = requestHostIp;
    }

    public String getRequestApi() {
        return requestApi;
    }

    public void setRequestApi(String requestApi) {
        this.requestApi = requestApi;
    }

    public String getRequestUrl() {
        return requestUrl;
    }

    public void setRequestUrl(String requestUrl) {
        this.requestUrl = requestUrl;
    }

    public String getRequestParameter() {
        return requestParameter;
    }

    public void setRequestParameter(String requestParameter) {
        this.requestParameter = requestParameter;
    }

    public String getRequestCompletionTime() {
        return requestCompletionTime;
    }

    public void setRequestCompletionTime(String requestCompletionTime) {
        this.requestCompletionTime = requestCompletionTime;
    }

    public String getRequestHostSystem() {
        return requestHostSystem;
    }

    public void setRequestHostSystem(String requestHostSystem) {
        this.requestHostSystem = requestHostSystem;
    }

    public String getRequestCompletionCode() {
        return requestCompletionCode;
    }

    public void setRequestCompletionCode(String requestCompletionCode) {
        this.requestCompletionCode = requestCompletionCode;
    }

    public String getRequestCompletionResult() {
        return requestCompletionResult;
    }

    public void setRequestCompletionResult(String requestCompletionResult) {
        this.requestCompletionResult = requestCompletionResult;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getSign() {
        return sign;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }

    public String getCollectionSign() {
        return collectionSign;
    }

    public void setCollectionSign(String collectionSign) {
        this.collectionSign = collectionSign;
    }

    public Map<String, Object> getCustomizeField() {
        return customizeField;
    }

    public void setCustomizeField(Map<String, Object> customizeField) {
        this.customizeField = customizeField;
    }
}
