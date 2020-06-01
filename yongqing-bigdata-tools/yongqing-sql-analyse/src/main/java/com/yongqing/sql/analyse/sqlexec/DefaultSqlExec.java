package com.yongqing.sql.analyse.sqlexec;


import com.yongqing.sql.analyse.bean.SqlOutput;
import com.yongqing.sql.analyse.exception.SqlAnalyseException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Log4j2
public class DefaultSqlExec implements SqlExec {
    /**
     * 执行SQL查询 SELECT
     * ResultSet executeQuery(String sql); 执行SQL查询，并返回ResultSet 对象。
     *
     * @param sql 要查询的SQL语句
     * @return 执行结果
     */

    @Override
    public SqlOutput executeQuery(Connection connection, String sql) {
        SqlOutput sqlOutput = new SqlOutput();
        sqlOutput.setSqlType("query");
        sqlOutput.setSql(sql);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (connection != null) {
                ps = connection.prepareStatement(sql);
            }
            if (ps != null) {
                rs = ps.executeQuery();
                List<Map<String, Object>> list = new ArrayList<>();
                while (rs.next()) {
                    //获取列数
                    int columnCount = rs.getMetaData().getColumnCount();
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rs.getMetaData().getColumnName(i);
                        String columnValue = rs.getString(i);
                        map.put(columnName, columnValue);
                    }
                    list.add(map);
                }
                sqlOutput.setSqlResult(list);
                sqlOutput.setExecStatus("1");
                sqlOutput.setEffectRowNum(list.size());
            }
        } catch (Exception e) {
            log.error("executeQuery sql error:", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                log.error("executeQuery close rs error:", e);
                sqlOutput.setExecStatus("0");
                sqlOutput.setExecMessage(e.getMessage());
            }
        }
        return sqlOutput;
    }

    /**
     * 执行SQL查询,查询结果与传入的List对应封装到Map
     *
     * @param connection
     * @param sql
     * @param columnList 要与查询结果对应的列
     * @return 执行结果
     */
    @Override
    public SqlOutput executeQuery(Connection connection, String sql, List<String> columnList) {
        SqlOutput sqlOutput = new SqlOutput();
        sqlOutput.setSqlType("query");
        sqlOutput.setSql(sql);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (connection != null) {
                ps = connection.prepareStatement(sql);
            }
            if (ps != null) {
                rs = ps.executeQuery();
                if (columnList.size() > rs.getMetaData().getColumnCount()) {
                    throw new SqlAnalyseException("columnList 's size is " + columnList.size() + " sql " + sql + " return columns is " + rs.getMetaData().getColumnCount());
                }
                List<Map<String, Object>> list = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 0; i < columnList.size(); i++) {
                        String columnName = columnList.get(i);
                        String columnValue = rs.getString(i + 1);
                        map.put(columnName, columnValue);
                    }
                    list.add(map);
                }
                sqlOutput.setSqlResult(list);
                sqlOutput.setExecStatus("1");
                sqlOutput.setEffectRowNum(list.size());
            }
        } catch (Exception e) {
            log.error("executeQuery sql error:", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                log.error("executeQuery close rs error:", e);
                sqlOutput.setExecStatus("0");
                sqlOutput.setExecMessage(e.getMessage());
            }
        }
        return sqlOutput;
    }

    /**
     * 执行SQL查询 SELECT
     * ResultSet executeQuery(String sql); 执行SQL查询，并返回ResultSet 对象。
     *
     * @param sql       要查询的SQL语句
     * @param separator 列分隔符
     * @param start     记录开始
     * @param length    返回记录数
     * @return 执行结果
     */
    @Override
    public String exportQuery(Connection connection, String sql, String separator, int start, int length) {
        //最终分页执行查询的SQL
        final String sqlStr = "SELECT * FROM (" + sql + ") data limit " + start + "," + length;
        StringBuilder finalSQL = new StringBuilder(sqlStr);
        log.info("finalSQL:" + finalSQL.toString());
        StringBuilder result = new StringBuilder("");
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (connection != null) {
                ps = connection.prepareStatement(finalSQL.toString());
            }
            if (ps != null) {
                rs = ps.executeQuery();
                while (rs.next()) {
                    //获取列数
                    int columnCount = rs.getMetaData().getColumnCount();
                    StringBuilder line = new StringBuilder("");
                    for (int i = 1; i <= columnCount; i++) {
                        String columnValue = rs.getString(i);
                        if (i == columnCount) {
                            //最后一列不加分隔符
                            line.append(columnValue);
                        } else {
                            line.append(columnValue).append(separator);
                        }
                    }
                    result.append(line).append("\r\n");//加入回车换行
                }
            }
        } catch (Exception e) {
            log.error("exportQuery sql error:", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                log.error("exportQuery close rs error:", e);
            }
        }
        return result.toString();
    }

    @Override
    public SqlOutput executeUpdate(Connection connection, String sql) {
        SqlOutput sqlOutput = new SqlOutput();
        PreparedStatement ps = null;
        sqlOutput.setSql(sql);
        String lowerSql = sql.toLowerCase();
        if (StringUtils.isNotBlank(sql) && (lowerSql.contains("update ") || lowerSql.contains("update\n")) && !(lowerSql.contains("insert ") || lowerSql.contains("insert\n")) && !(lowerSql.contains("delete ") || lowerSql.contains("delete\n"))) {
            sqlOutput.setSqlType("update");
        } else if (StringUtils.isNotBlank(sql) && !(lowerSql.contains("update ") || lowerSql.contains("update\n")) && (lowerSql.contains("insert ") || lowerSql.contains("insert\n")) && !(lowerSql.contains("delete ") || lowerSql.contains("delete\n"))) {
            sqlOutput.setSqlType("insert");
        } else if (StringUtils.isNotBlank(sql) && !(lowerSql.contains("update ") || lowerSql.contains("update\n")) && !(lowerSql.contains("insert ") || lowerSql.contains("insert\n")) && (lowerSql.contains("delete ") || lowerSql.contains("delete\n"))) {
            sqlOutput.setSqlType("delete");
        } else {
            sqlOutput.setSqlType("all");
        }
        try {
            if (connection != null) {
                ps = connection.prepareStatement(sql);
            }
            if (ps != null) {
                //影响行数
                int effectRowNum = ps.executeUpdate();
                sqlOutput.setExecStatus("1");
                sqlOutput.setEffectRowNum(effectRowNum);
                sqlOutput.setExecMessage("executeUpdate：" + effectRowNum);
            }
        } catch (SQLException e) {
            sqlOutput.setExecStatus("0");
            sqlOutput.setExecMessage(e.getMessage());
            log.error("executeUpdate sql error:", e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    sqlOutput.setExecStatus("0");
                    sqlOutput.setExecMessage(e.getMessage());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("executeUpdate close connection error:", e);
                    sqlOutput.setExecStatus("0");
                    sqlOutput.setExecMessage(e.getMessage());
                }
            }
        }
        return sqlOutput;
    }

    @Override
    public SqlOutput execute(Connection connection, String sql) {
        PreparedStatement ps = null;
        SqlOutput sqlOutput = new SqlOutput();
        sqlOutput.setSql(sql);
        try {
            if (connection != null) {
                ps = connection.prepareStatement(sql);
            }
            if (ps != null) {
                boolean executeResult = ps.execute();
                sqlOutput.setExecStatus("1");
                sqlOutput.setExecMessage("execute：" + executeResult);
            }
        } catch (SQLException e) {
            sqlOutput.setExecStatus("0");
            sqlOutput.setExecMessage(e.getMessage());
            log.error("execute sql error:", e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    log.error("close ps error:", e);
                    sqlOutput.setExecStatus("0");
                    sqlOutput.setExecMessage(e.getMessage());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("execute close connnection error:", e);
                    sqlOutput.setExecStatus("0");
                    sqlOutput.setExecMessage(e.getMessage());
                }
            }
        }
        return sqlOutput;
    }

}
