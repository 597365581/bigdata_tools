package com.yongqing.crawler.analysis;

import com.yongqing.crawler.analysis.tax.bean.TaxPdfTable;
import com.yongqing.crawler.analysis.tax.bean.TaxTable;

/**
 *
 */
public abstract class TaxCrawlerAbStractAnalysis implements TaxCrawlerAnalysis{
    @Override
   public TaxTable analysisTaxTable(TaxPdfTable taxPdfTable, String taxTableType){
        TaxTable taxTable = new TaxTable();
        taxTable.setDeclarationDate(taxPdfTable.getDeclarationDate());
        taxTable.setDeclarationTableName(taxPdfTable.getDeclarationTableName());
        taxTable.setStartTaxDate(taxPdfTable.getStartTaxDate());
        taxTable.setEndTaxDate(taxPdfTable.getEndTaxDate());
        taxTable.setTaxBalance(taxPdfTable.getTaxBalance());
        taxTable.setDocId(taxPdfTable.getDocId());
        taxTable.setRemark(taxPdfTable.getRemark());
        taxTable.setType(taxPdfTable.getType());
        return taxTable;
    }
}
