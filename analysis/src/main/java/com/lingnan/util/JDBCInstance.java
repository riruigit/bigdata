package com.lingnan.util;

import java.sql.Connection;
import java.sql.SQLException;

public class JDBCInstance {
    private static Connection connection;

    private JDBCInstance(){

    }

    public static Connection getConnection() throws SQLException {
        if(connection == null || connection.isClosed()){
            connection = JDBCUtil.getConnection();
        }
        return connection;
    }

}
