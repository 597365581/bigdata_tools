package com.yongqing.log.output.factory;

import com.yongqing.log.output.utils.UUIDGenerator;
import com.yongqing.log.output.utils.WebToolUtil;
import com.yongqing.log.output.utils.DateUtil;
import com.yongqing.log.output.utils.gson.GsonUtil;
import com.yongqing.processor.log.bean.BusinessLog;
import com.yongqing.processor.log.bean.SqlLog;
import com.yongqing.processor.log.bean.SystemRequestLog;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

//日志生成工厂
public class LogFactory {
    /**
     * sql 日志生成
     * @param sqlBusinessId
     * @param sqlBusinessType
     * @param sqlBusinessSystem
     * @param sqlBusinessName
     * @param sqlType
     * @param sqlList
     * @param customizeField
     * @return
     */
    public static String createSqlLog(String sqlBusinessId,String sqlBusinessType,String sqlBusinessSystem,String sqlBusinessName,String sqlType,List<String> sqlList,Map<String, Object> customizeField){
        SqlLog sqlLog = new SqlLog();
        sqlLog.setSqlBusinessId(sqlBusinessId);
        sqlLog.setSqlBusinessType(sqlBusinessType);
        sqlLog.setSqlBusinessSystem(sqlBusinessSystem);
        sqlLog.setSqlBusinessName(sqlBusinessName);
        sqlLog.setSqlType(sqlType);
        sqlLog.setSqlList(sqlList);
        sqlLog.setCustomizeField(customizeField);
        sqlLog.setSqlLogId(UUIDGenerator.getUUID());
        sqlLog.setSqlLogTime(DateUtil.getNormalTimeStr());
        return  GsonUtil.gson.toJson(sqlLog);
    }

    /**
     *  业务日志生成
     * @param businessSystem  系统名称 必填，业务系统(系统的唯一英文简称)
     * @param businessId  业务id  必填，业务ID（每个业务不可重复，建议未来统一生成和分配，在接入统一采集）
     * @param businessType  业务类型  必填，业务类型（统一定义）
     * @param businessName  业务名称  必填，业务名称
     * @param customizeField 自定义字段  这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ "key1":"value1","key2":"value2" }
     * @return json String
     */
    public static String createBusinessLog(String businessSystem,String businessId,String businessType,String businessName,Map<String,Object> customizeField){
        BusinessLog businessLog = new BusinessLog();
        businessLog.setBusinessId(businessId);
        //每条日志给一个唯一的id
        businessLog.setBusinessLogId(UUIDGenerator.getUUID());
        businessLog.setBusinessSystem(businessSystem);
        businessLog.setBusinessType(businessType);
        businessLog.setBusinessName(businessName);
        //日志的生成时间
        businessLog.setBusinessLogTime(DateUtil.getNormalTimeStr());
        businessLog.setCustomizeField(customizeField);
        return GsonUtil.gson.toJson(businessLog);
    }

    /**
     *
     * @param requestInitiationTime  请求发起时间  非必填，一般是用户传入，格式：yyyy-MM-dd HH:mm:ss.SSS
     * @param requestReceiveTime   请求接收时间  必填，系统实际接收到请求的时间，格式：yyyy-MM-dd HH:mm:ss.SSS
     * @param requestCompletionTime  请求处理完成时间  必填，请求处理完成的时间，统一获取，格式：yyyy-MM-dd HH:mm:ss.SSS
     * @param requestHostSystem    请求的处理系统  请求实际被处理的系统(系统的唯一英文简称)
     * @param requestCompletionCode  请求响应的Code  必填
     * @param requestCompletionResult  请求结果  必填
     * @param requestId  请求id  必填，系统统一生成
     * @param appId  请求的应用ID   非必填，主要针对应用接入的情况，比如需要先注册接入，接入后，会分配一个唯一的应用ID，请求时需要传入
     * @param sign   请求的签名   非必填，和appId结合使用，用于接口请求的校验
     * @param customizeField   customizeField  这是一个子Map，描述的是自定义的字段内容,一般输出的格式：{ “key1”:”value1”,”key2”:”value2” }
     * @return json Str
     */
    public static String createSystemRequestLog(HttpServletRequest httpServletRequest, String requestInitiationTime, String requestReceiveTime, String requestCompletionTime, String requestHostSystem, String requestCompletionCode, String requestCompletionResult, String requestId, String appId, String sign, Map<String,Object> customizeField){
        SystemRequestLog systemRequestLog = new SystemRequestLog();
        systemRequestLog.setRequestLogTime(DateUtil.getNormalTimeStr());
        systemRequestLog.setRequestInitiationTime(requestInitiationTime);
        systemRequestLog.setRequestReceiveTime(requestReceiveTime);
        systemRequestLog.setRequestCompletionTime(requestCompletionTime);
        systemRequestLog.setRequestHostIp(requestHostSystem);
        systemRequestLog.setRequestCompletionCode(requestCompletionCode);
        systemRequestLog.setRequestCompletionResult(requestCompletionResult);
        systemRequestLog.setRequestId(requestId);
        systemRequestLog.setAppId(appId);
        systemRequestLog.setSign(sign);
        systemRequestLog.setCustomizeField(customizeField);
        systemRequestLog.setRequestInitiationIp(WebToolUtil.getRemoteIpAddress(httpServletRequest));
        try {
            systemRequestLog.setRequestHostIp(WebToolUtil.getLocalIP());
        } catch (Exception e) {
            //获取失败时，直接设置为空
            systemRequestLog.setRequestHostIp("");
        }
        systemRequestLog.setRequestApi(httpServletRequest.getContextPath());
        systemRequestLog.setRequestUrl(httpServletRequest.getRequestURI());
        systemRequestLog.setRequestParameter(GsonUtil.gson.toJson(httpServletRequest.getParameterMap()));
        return GsonUtil.gson.toJson(systemRequestLog);
    }
}
