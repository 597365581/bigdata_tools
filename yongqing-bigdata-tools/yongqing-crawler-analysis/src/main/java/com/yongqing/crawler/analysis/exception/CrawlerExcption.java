package com.yongqing.crawler.analysis.exception;

/**
 *
 */
public class CrawlerExcption extends RuntimeException{
    public CrawlerExcption(String s, Exception e) {
        super(s, e);
    }
    public CrawlerExcption(String s){
        super(s);
    }
}
