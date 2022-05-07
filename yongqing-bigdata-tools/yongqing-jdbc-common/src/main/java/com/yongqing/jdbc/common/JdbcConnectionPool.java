package com.yongqing.jdbc.common;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class JdbcConnectionPool {

    private final static List<JdbcConnection> jdbcConnectionList = new ArrayList<>();

    public static synchronized void initPool(int size, String driverName, String jdbcUrl) {
        if (size == 0) {
            throw new RuntimeException("size must >0");
        }
        if (jdbcConnectionList.size() == 0) {
            for (int i = 0; i <= size; i++) {
                jdbcConnectionList.add(new JdbcConnection(driverName, jdbcUrl));
            }

        }
    }

    public synchronized static void initPool(int size, String driverName, String jdbcUrl, String user, String password) {
        if (size == 0) {
            throw new RuntimeException("size must >0");
        }
        if (jdbcConnectionList.size() == 0) {
            for (int i = 0; i <= size; i++) {
                jdbcConnectionList.add(new JdbcConnection(user, password, driverName, jdbcUrl));
            }

        }

    }

    public static Connection getConnection() throws Exception {
        if (jdbcConnectionList.size() == 0) {
            throw new RuntimeException("please init JdbcConnectionPool");
        } else {
            return jdbcConnectionList.get(new Random().nextInt(jdbcConnectionList.size())).getConnection();
        }
    }

    public static JdbcConnection getJdbcConnection() throws Exception {
        if (jdbcConnectionList.size() == 0) {
            throw new RuntimeException("please init JdbcConnectionPool");
        } else {
            return jdbcConnectionList.get(new Random().nextInt(jdbcConnectionList.size()));
        }
    }

    public static List<Map<String, Object>> executeQuery(String sql) {
        if (jdbcConnectionList.size() == 0) {
            throw new RuntimeException("please init JdbcConnectionPool");
        } else {
            return jdbcConnectionList.get(new Random().nextInt(jdbcConnectionList.size())).executeQuery(sql);
        }

    }
}
