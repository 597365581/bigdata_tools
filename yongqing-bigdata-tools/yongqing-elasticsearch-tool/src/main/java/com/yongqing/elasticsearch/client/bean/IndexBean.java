package com.yongqing.elasticsearch.client.bean;

/**
 *
 */
public class IndexBean {
    //index name
    private String index;
    //index type
    private String indexType;
    //index doc id
    private String docId;
    // 1 IndexRequest 2 UpdateRequest  3 DeleteRequest
    private String operateType;

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }
}
