package com.yongqing.crawler.analysis.constants;

import com.google.gson.reflect.TypeToken;
import com.yongqing.common.bigdata.tool.GsonUtil;
import com.yongqing.crawler.analysis.TaxCrawlerAnalysis;
import com.yongqing.crawler.analysis.tax.bean.TaxCrawlerAnalysisBean;
import com.yongqing.etcd.action.Action;
import com.yongqing.etcd.tools.EtcdUtil;

import java.util.List;
import java.util.Properties;

/**
 *
 */
public class TaxCrawlerConstant implements Action {

    public static volatile List<TaxCrawlerAnalysisBean> taxCrawlerAnalysisBeanList;

    public enum TaxTableType {
        ZENGZHISHUITABLE("zengZhiShuiPdf", "增值税报表"),
        QIYESUODESHUITABLE("qiYeSuoDeshuiPdf", "企业所得税报表"),
        ZICHANFUZHAITABLE("ziChanFuZhaiPdf", "资产负债报表"),
        LIRUNTABLE("liRunPdf", "利润报表"),
        XIANJINLIULIANGTABLE("xianJinLiuLiangPdf", "现金流量表");
        private String taxTableType;
        private String taxTableTypeName;

        private TaxTableType(String taxTableType, String taxTableTypeName) {
            this.taxTableType = taxTableType;
            this.taxTableTypeName = taxTableTypeName;
        }

        public String getTaxTableType() {
            return taxTableType;
        }

        public void setTaxTableType(String taxTableType) {
            this.taxTableType = taxTableType;
        }

        public String getTaxTableTypeName() {
            return taxTableTypeName;
        }

        public void setTaxTableTypeName(String taxTableTypeName) {
            this.taxTableTypeName = taxTableTypeName;
        }
    }

    private void loadTaxCrawlerAnalysises() {
        taxCrawlerAnalysisBeanList = GsonUtil.gson.fromJson(EtcdUtil.getLocalPropertie("taxCrawlerAnalysises"), new TypeToken<List<TaxCrawlerAnalysisBean>>() {
        }.getType());
        taxCrawlerAnalysisBeanList.forEach(taxCrawlerAnalysisBean -> {
            try {
                TaxCrawlerAnalysis taxCrawlerAnalysis = (TaxCrawlerAnalysis) Class.forName(taxCrawlerAnalysisBean.getTaxCrawlerAnalysis()).newInstance();
                taxCrawlerAnalysisBean.setTaxCrawlerAnalysisInstance(taxCrawlerAnalysis);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void doAction(Properties oldProp, Properties newProp) {
        synchronized (TaxCrawlerConstant.class) {
            if (null != newProp.getProperty("isLoadTaxCrawlerAnalysises") && null != oldProp.getProperty("isLoadTaxCrawlerAnalysises") && oldProp.getProperty("isLoadTaxCrawlerAnalysises").equals(newProp.getProperty("isLoadTaxCrawlerAnalysises"))) {
                loadTaxCrawlerAnalysises();
                return;
            }


            if ((null == oldProp.getProperty("taxCrawlerAnalysises") && null != newProp.getProperty("taxCrawlerAnalysises")) || (null != oldProp.getProperty("taxCrawlerAnalysises") && null != newProp.getProperty("taxCrawlerAnalysises") && !oldProp.getProperty("taxCrawlerAnalysises").equals(newProp.getProperty("taxCrawlerAnalysises")))) {
                loadTaxCrawlerAnalysises();
            } else if (null != oldProp.getProperty("taxCrawlerAnalysises") && null == newProp.getProperty("taxCrawlerAnalysises")) {
                if (null != taxCrawlerAnalysisBeanList) {
                    taxCrawlerAnalysisBeanList.clear();
                    taxCrawlerAnalysisBeanList = null;
                }
            }
        }
    }
}
