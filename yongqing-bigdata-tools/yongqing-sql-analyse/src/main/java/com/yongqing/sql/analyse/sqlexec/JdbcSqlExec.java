package com.yongqing.sql.analyse.sqlexec;

import com.yongqing.sql.analyse.bean.SqlOutput;
import lombok.extern.log4j.Log4j2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 */
@Log4j2
public class JdbcSqlExec extends DefaultSqlExec {

    private String driver;
    private String jdbcUrl;
    private String user;
    private String password;

    public JdbcSqlExec(String driver, String jdbcUrl, String user, String password) {
        this.driver = driver;
        this.jdbcUrl = jdbcUrl;
        this.password = password;
        this.user = user;
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setAutoCommit(true);
        } catch (ClassNotFoundException e) {
            log.error("driver class is not exist", e);
        } catch (SQLException e) {
            log.error("DriverManager.getConnection cause Exception", e);
        }
        return connection;
    }

    public SqlOutput executeQuery(String sql) {
        SqlOutput result = null;
        Connection connection = getConnection();
        try {
            result = super.executeQuery(connection, sql);
        } catch (Exception e) {
            log.error("executeQuery cause Exception", e);
        } finally {
            try {
                if (null != connection && !connection.isClosed()) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error("executeQuery cause Exception", e);
                    }
                }
            } catch (SQLException e) {
                log.error("executeQuery cause Exception", e);
            }
        }
        return result;
    }

    public String exportQuery(String sql, String separator, int start, int length) {
        String result = null;
        Connection connection = getConnection();
        try {
            result = super.exportQuery(connection, sql, separator, start, length);
        } catch (Exception e) {
            log.error("exportQuery cause Exception", e);
        } finally {
            try {
                if (null != connection && !connection.isClosed()) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error("exportQuery cause Exception", e);
                    }
                }
            } catch (SQLException e) {
                log.error("exportQuery cause Exception", e);
            }
        }
        return result;
    }

    public SqlOutput executeUpdate(String sql) {
        SqlOutput result = null;
        Connection connection = getConnection();
        try {
            result = super.executeUpdate(connection, sql);
        } catch (Exception e) {
            log.error("executeUpdate cause Exception", e);
        } finally {
            try {
                if (null != connection && !connection.isClosed()) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error("executeUpdate cause Exception", e);
                    }
                }
            } catch (SQLException e) {
                log.error("executeUpdate cause Exception", e);
            }
        }
        return result;
    }

    public SqlOutput execute(String sql) {
        SqlOutput result = null;
        Connection connection = getConnection();
        try {
            result = super.execute(connection, sql);
        } catch (Exception e) {
            log.error("execute sql cause Exception", e);
        }finally {
            try {
                if (null != connection && !connection.isClosed()) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        log.error("execute sql cause Exception", e);
                    }
                }
            } catch (SQLException e) {
                log.error("execute sql cause Exception", e);
            }
        }
        return result;
    }
}
