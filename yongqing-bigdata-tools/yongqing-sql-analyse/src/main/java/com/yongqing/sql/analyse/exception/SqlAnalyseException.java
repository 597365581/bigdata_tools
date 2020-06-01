package com.yongqing.sql.analyse.exception;

/**
 *
 */
public class SqlAnalyseException extends RuntimeException {
    public SqlAnalyseException(String s) {
        super(s);
    }
    public SqlAnalyseException(String s,Exception e) {
        super(s,e);
    }
}
