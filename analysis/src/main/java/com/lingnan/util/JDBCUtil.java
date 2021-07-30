package com.lingnan.util;



import java.sql.*;

public class JDBCUtil {
    private static final String MYSQL_JDBC_DRIVER="com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_JDBC_URL="jdbc:mysql://hadoop01:3306/callData?useUnicode=true&characterEncoding" +
            "=UTF-8&serverTimezone=UTC";
    private static final String MYSQL_JDBC_USERNAME="root";
    private static final String MYSQL_JDBC_PASSWORD="0289";

    //???
    public static Connection getConnection(){
        try {
            Class.forName(MYSQL_JDBC_DRIVER);
            return DriverManager.getConnection(MYSQL_JDBC_URL,MYSQL_JDBC_USERNAME,MYSQL_JDBC_PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        if(connection !=null){
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        if(statement !=null){
            try {
                statement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }
}
