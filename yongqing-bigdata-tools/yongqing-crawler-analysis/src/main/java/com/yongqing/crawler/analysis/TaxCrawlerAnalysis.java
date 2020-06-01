package com.yongqing.crawler.analysis;

import com.yongqing.crawler.analysis.exception.CrawlerExcption;
import com.yongqing.crawler.analysis.tax.bean.TaxPdfTable;
import com.yongqing.crawler.analysis.tax.bean.TaxTable;

/**
 *
 */
public interface TaxCrawlerAnalysis {
    TaxTable analysisTaxTable(TaxPdfTable taxPdfTable, String taxTableType) throws CrawlerExcption;
}
