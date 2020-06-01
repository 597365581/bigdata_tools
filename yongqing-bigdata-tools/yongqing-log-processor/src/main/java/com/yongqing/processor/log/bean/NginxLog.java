package com.yongqing.processor.log.bean;

import java.util.Map;

/**
 *
 */
public class NginxLog {
    private String remoteAddr;
    private String remoteUser;
    private String timeLocal;
    private String request;
    private String requestUri;
    private String httpReferer;
    private String requestTime;
    private String connection;
    private String connectionRequests;
    private String requestLength;
    private String requestBody;
    private String status;
    private String bodyBytesSent;
    private String httpUserAgent;
    private String httpXForwardedFor;
    private String upstreamStatus;
    private String upstreamResponseTime;
    private String upstreamAddr;
    private String sslCipher;
    private String sslProtocol;
    private Map<String, Object> customizeField;
    private String collectionSign = "nginxCollection";
    private String businessLogId;
    private String businessId = "nginxAccessLog";

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getRequestUri() {
        return requestUri;
    }

    public void setRequestUri(String requestUri) {
        this.requestUri = requestUri;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    public void setHttpReferer(String httpReferer) {
        this.httpReferer = httpReferer;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(String requestTime) {
        this.requestTime = requestTime;
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getConnectionRequests() {
        return connectionRequests;
    }

    public void setConnectionRequests(String connectionRequests) {
        this.connectionRequests = connectionRequests;
    }

    public String getRequestLength() {
        return requestLength;
    }

    public void setRequestLength(String requestLength) {
        this.requestLength = requestLength;
    }

    public String getRequestBody() {
        return requestBody;
    }

    public void setRequestBody(String requestBody) {
        this.requestBody = requestBody;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBodyBytesSent() {
        return bodyBytesSent;
    }

    public void setBodyBytesSent(String bodyBytesSent) {
        this.bodyBytesSent = bodyBytesSent;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }

    public String getHttpXForwardedFor() {
        return httpXForwardedFor;
    }

    public void setHttpXForwardedFor(String httpXForwardedFor) {
        this.httpXForwardedFor = httpXForwardedFor;
    }

    public String getUpstreamStatus() {
        return upstreamStatus;
    }

    public void setUpstreamStatus(String upstreamStatus) {
        this.upstreamStatus = upstreamStatus;
    }

    public String getUpstreamResponseTime() {
        return upstreamResponseTime;
    }

    public void setUpstreamResponseTime(String upstreamResponseTime) {
        this.upstreamResponseTime = upstreamResponseTime;
    }

    public String getUpstreamAddr() {
        return upstreamAddr;
    }

    public void setUpstreamAddr(String upstreamAddr) {
        this.upstreamAddr = upstreamAddr;
    }

    public String getSslCipher() {
        return sslCipher;
    }

    public void setSslCipher(String sslCipher) {
        this.sslCipher = sslCipher;
    }

    public String getSslProtocol() {
        return sslProtocol;
    }

    public void setSslProtocol(String sslProtocol) {
        this.sslProtocol = sslProtocol;
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

    public String getBusinessLogId() {
        return businessLogId;
    }

    public void setBusinessLogId(String businessLogId) {
        this.businessLogId = businessLogId;
    }

    @Override
    public String toString() {
        return "NginxLog{" +
                "remoteAddr='" + remoteAddr + '\'' +
                ", remoteUser='" + remoteUser + '\'' +
                ", timeLocal='" + timeLocal + '\'' +
                ", request='" + request + '\'' +
                ", requestUri='" + requestUri + '\'' +
                ", httpReferer='" + httpReferer + '\'' +
                ", requestTime='" + requestTime + '\'' +
                ", connection='" + connection + '\'' +
                ", connectionRequests='" + connectionRequests + '\'' +
                ", requestLength='" + requestLength + '\'' +
                ", requestBody='" + requestBody + '\'' +
                ", status='" + status + '\'' +
                ", bodyBytesSent='" + bodyBytesSent + '\'' +
                ", httpUserAgent='" + httpUserAgent + '\'' +
                ", httpXForwardedFor='" + httpXForwardedFor + '\'' +
                ", upstreamStatus='" + upstreamStatus + '\'' +
                ", upstreamResponseTime='" + upstreamResponseTime + '\'' +
                ", upstreamAddr='" + upstreamAddr + '\'' +
                ", sslCipher='" + sslCipher + '\'' +
                ", sslProtocol='" + sslProtocol + '\'' +
                ", customizeField=" + customizeField +
                ", collectionSign='" + collectionSign + '\'' +
                ", businessLogId='" + businessLogId + '\'' +
                ", businessId='" + businessId + '\'' +
                '}';
    }
}
