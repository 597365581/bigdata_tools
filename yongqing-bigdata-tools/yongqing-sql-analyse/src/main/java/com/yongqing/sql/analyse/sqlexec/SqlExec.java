package com.yongqing.sql.analyse.sqlexec;

import com.yongqing.sql.analyse.bean.SqlOutput;

import java.sql.Connection;
import java.util.List;

/**
 *
 */
public interface SqlExec {


    SqlOutput executeQuery(Connection connection, String sql);

    SqlOutput executeQuery(Connection connection, String sql, List<String> columnList);

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

    String exportQuery(Connection connection, String sql, String separator, int start, int length);

    SqlOutput executeUpdate(Connection connection, String sql);

    SqlOutput execute(Connection connection, String sql);
}
