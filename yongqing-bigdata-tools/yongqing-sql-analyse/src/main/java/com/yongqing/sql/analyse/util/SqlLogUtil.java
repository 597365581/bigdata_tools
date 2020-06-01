package com.yongqing.sql.analyse.util;

import com.yongqing.processor.log.bean.SqlLog;
import com.yongqing.sql.analyse.bean.SqlOutput;
import com.yongqing.sql.analyse.sqlexec.MybatisSqlExec;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@Log4j2
public class SqlLogUtil {
    private static MybatisSqlExec mybatisSqlExec = new MybatisSqlExec();

    public static List<SqlOutput> updateSqlToList(SqlLog sqlLog) {
        List<SqlOutput> result = new ArrayList<>();
        if (null != sqlLog && null != sqlLog.getSqlList() && sqlLog.getSqlList().size() > 0) {
            if (null != sqlLog.getSqlType() && sqlLog.getSqlType().equals("3")) {
                for (String sql : sqlLog.getSqlList()) {
                  result.add(ParseUpdateSqlUtil.parseUpdateSql(sql));
                }
            } else if (null != sqlLog.getSqlType() && sqlLog.getSqlType().equals("0")) {
                for (String sql : sqlLog.getSqlList()) {
                    String lowerSql = sql.toLowerCase();
                    if ((lowerSql.contains("update ") || lowerSql.contains("update\n")) && !(lowerSql.contains("insert ") || lowerSql.contains("insert\n")) && !(lowerSql.contains("select ") || lowerSql.contains("select\n")) && !(lowerSql.contains("delete ") || lowerSql.contains("delete\n"))) {
                        result.add(ParseUpdateSqlUtil.parseUpdateSql(sql));
                    }
                }
            } else {
                log.info("updateSqlToList SqlType:{} is not need to deal", sqlLog.getSqlType());
            }
            return result;
        }
        return null;
    }

    public static List<SqlOutput> insertSqlToList(SqlLog sqlLog) {
        List<SqlOutput> result = new ArrayList<>();
        if (null != sqlLog && null != sqlLog.getSqlList() && sqlLog.getSqlList().size() > 0) {
            if (null != sqlLog.getSqlType() && sqlLog.getSqlType().equals("2")) {
                for (String sql : sqlLog.getSqlList()) {
                    result.add(ParseUpdateSqlUtil.parseInsertSql(sql));
                }
            } else if (null != sqlLog.getSqlType() && sqlLog.getSqlType().equals("0")) {
                for (String sql : sqlLog.getSqlList()) {
                    String lowerSql = sql.toLowerCase();
                    if ((lowerSql.contains("insert ") || lowerSql.contains("insert\n")) && !(lowerSql.contains("update ") || lowerSql.contains("update\n")) && !(lowerSql.contains("delete ") || lowerSql.contains("delete\n"))) {
                        result.add(ParseUpdateSqlUtil.parseInsertSql(sql));
                    }
                }
            } else {
                log.info("insertSqlToList SqlType:{} is not need to deal", sqlLog.getSqlType());
            }
            return result;
        }
        return null;
    }

    public static List<SqlOutput> querySqlToList(SqlLog sqlLog) {
        List<SqlOutput> result = new ArrayList<SqlOutput>();
        synchronized (SqlLogUtil.class) {
            if (null == mybatisSqlExec) {
                mybatisSqlExec = new MybatisSqlExec();
            }
        }
        if (null != sqlLog && null != sqlLog.getSqlList() && sqlLog.getSqlList().size() > 0) {
            if (null != sqlLog.getSqlType() && sqlLog.getSqlType().equals("1")) {
                for (String sql : sqlLog.getSqlList()) {
                    result.add(mybatisSqlExec.executeQuery(sql));
                }
            } else if (null != sqlLog.getSqlType() && sqlLog.getSqlType().equals("0")) {
                for (String sql : sqlLog.getSqlList()) {
                    String lowerSql = sql.toLowerCase();
                    if ((lowerSql.contains("select ") || lowerSql.contains("select\n")) && !(lowerSql.contains("insert ") || lowerSql.contains("insert\n")) && !(lowerSql.contains("update ") || lowerSql.contains("update\n")) && !(lowerSql.contains("delete ") || lowerSql.contains("delete\n"))) {
                        result.add(mybatisSqlExec.executeQuery(sql));
                    }
                }
            } else {
                log.info("querySqlToList SqlType:{} is not need to deal", sqlLog.getSqlType());
            }
            return result;
        }
        return null;
    }
}
