package com.yongqing.crawler.analysis.tax.bean;

import java.util.Map;

/**
 *
 */
public class TaxTable {
    private Map<String,Object> tableHeader;
    private String tableData;
    private String declarationDate;
    private String startTaxDate;
    private String endTaxDate;
    private String taxBalance;
    private String declarationTableName;
    private String remark;
    private String docId;

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    //1 年报 2 季报或者月报
    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Map<String, Object> getTableHeader() {
        return tableHeader;
    }

    public void setTableHeader(Map<String, Object> tableHeader) {
        this.tableHeader = tableHeader;
    }

    public String getTableData() {
        return tableData;
    }

    public void setTableData(String tableData) {
        this.tableData = tableData;
    }

    public String getDeclarationDate() {
        return declarationDate;
    }

    public void setDeclarationDate(String declarationDate) {
        this.declarationDate = declarationDate;
    }

    public String getStartTaxDate() {
        return startTaxDate;
    }

    public void setStartTaxDate(String startTaxDate) {
        this.startTaxDate = startTaxDate;
    }

    public String getEndTaxDate() {
        return endTaxDate;
    }

    public void setEndTaxDate(String endTaxDate) {
        this.endTaxDate = endTaxDate;
    }

    public String getTaxBalance() {
        return taxBalance;
    }

    public void setTaxBalance(String taxBalance) {
        this.taxBalance = taxBalance;
    }

    public String getDeclarationTableName() {
        return declarationTableName;
    }

    public void setDeclarationTableName(String declarationTableName) {
        this.declarationTableName = declarationTableName;
    }

    @Override
    public String toString() {
        return "TaxTable{" +
                "tableHeader=" + tableHeader +
                ", tableData='" + tableData + '\'' +
                ", declarationDate='" + declarationDate + '\'' +
                ", startTaxDate='" + startTaxDate + '\'' +
                ", endTaxDate='" + endTaxDate + '\'' +
                ", taxBalance='" + taxBalance + '\'' +
                ", declarationTableName='" + declarationTableName + '\'' +
                ", remark='" + remark + '\'' +
                ", docId='" + docId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
