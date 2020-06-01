package com.yongqing.crawler.analysis.tax.bean;

import com.yongqing.crawler.analysis.TaxCrawlerAnalysis;

/**
 *
 */
public class TaxCrawlerAnalysisBean {
    private TaxCrawlerAnalysis taxCrawlerAnalysisInstance;

    private String taxCrawlerAnalysis;

    private String type;

    private String provinceCity;

    public TaxCrawlerAnalysis getTaxCrawlerAnalysisInstance() {
        return taxCrawlerAnalysisInstance;
    }

    public void setTaxCrawlerAnalysisInstance(TaxCrawlerAnalysis taxCrawlerAnalysisInstance) {
        this.taxCrawlerAnalysisInstance = taxCrawlerAnalysisInstance;
    }

    public String getTaxCrawlerAnalysis() {
        return taxCrawlerAnalysis;
    }

    public void setTaxCrawlerAnalysis(String taxCrawlerAnalysis) {
        this.taxCrawlerAnalysis = taxCrawlerAnalysis;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProvinceCity() {
        return provinceCity;
    }

    public void setProvinceCity(String provinceCity) {
        this.provinceCity = provinceCity;
    }

    @Override
    public String toString() {
        return "TaxCrawlerAnalysisBean{" +
                "taxCrawlerAnalysisInstance=" + taxCrawlerAnalysisInstance +
                ", taxCrawlerAnalysis='" + taxCrawlerAnalysis + '\'' +
                ", type='" + type + '\'' +
                ", provinceCity='" + provinceCity + '\'' +
                '}';
    }
}
