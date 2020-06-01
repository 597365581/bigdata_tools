package com.yongqing.crawler.analysis.tax.bean;

/**
 *
 */
public class TaxPdfTable {
    private String declarationDate;
    private String startTaxDate;
    private String endTaxDate;
    private String taxBalance;
    private String pdfUrl;
    private String declarationTableName;
    private String remark;
    //1 年报 2 季报或者月报
    private String type;
    private String docId;

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public String getPdfUrl() {
        return pdfUrl;
    }

    public void setPdfUrl(String pdfUrl) {
        this.pdfUrl = pdfUrl;
    }

    public String getDeclarationTableName() {
        return declarationTableName;
    }

    public void setDeclarationTableName(String declarationTableName) {
        this.declarationTableName = declarationTableName;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @Override
    public String toString() {
        return "TaxPdfTable{" +
                "declarationDate='" + declarationDate + '\'' +
                ", startTaxDate='" + startTaxDate + '\'' +
                ", endTaxDate='" + endTaxDate + '\'' +
                ", taxBalance='" + taxBalance + '\'' +
                ", pdfUrl='" + pdfUrl + '\'' +
                ", declarationTableName='" + declarationTableName + '\'' +
                ", remark='" + remark + '\'' +
                ", type='" + type + '\'' +
                ", docId='" + docId + '\'' +
                '}';
    }
}
