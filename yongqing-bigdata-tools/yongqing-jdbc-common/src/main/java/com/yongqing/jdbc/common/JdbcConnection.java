package com.yongqing.jdbc.common;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcConnection {
    private volatile Connection connection;
    private final String jdbcUrl;
    private final String driverName;
    private String user;
    private String password;

    public JdbcConnection(String user, String password, String driverName, String jdbcUrl) {
        this.user = user;
        this.password = password;
        this.driverName = driverName;
        this.jdbcUrl = jdbcUrl;
    }

    public JdbcConnection(String driverName, String jdbcUrl) {
        this.driverName = driverName;
        this.jdbcUrl = jdbcUrl;
    }

    public synchronized void close() {
        if (null != connection) {

            try {
                connection.close();
                connection = null;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public synchronized Connection getConnection() throws Exception {
        if (null == connection || connection.isClosed()) {
            Class.forName(this.driverName);
            if (null == this.user) {
                connection = DriverManager.getConnection(this.jdbcUrl);
            } else {

                connection = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
            }
        }
        return connection;

    }

    public List<Map<String, Object>> executeQuery(String sql) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
            preparedStatement = getConnection().prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                int coloumCount = resultSet.getMetaData().getColumnCount();
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= coloumCount; i++) {
                    map.put(resultSet.getMetaData().getCatalogName(i), resultSet.getString(i));
                }
                resultList.add(map);
            }
        } catch (SQLException e) {
            close();
        } catch (Exception t) {
            t.printStackTrace();
        } finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }

}
