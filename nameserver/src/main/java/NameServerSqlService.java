/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.sql.*;

class NameServerSqlService {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    // 去除了安全连接，待在实验室机器上检查
    private static final String DB_URL = "jdbc:mysql://localhost:3306/dfsSystem?useSSL=false";
    private static final String sql_USER = "root";
    private static final String sql_PASSWORD = "12345678";

    // 用于运行SQL语句，需要在程序中手动关闭connection和statement
    public ResultSet executeSql(String sql, Connection connection, Statement statement) {
        connection = null;
        statement = null;
        ResultSet result = null;
        try {
            Class.forName(JDBC_DRIVER);
            logger.trace("连接数据库...");
            connection = DriverManager.getConnection(DB_URL, sql_USER, sql_PASSWORD);
            statement = connection.createStatement();
            result = statement.executeQuery(sql);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("SQL数据库连接错误！");
            e.printStackTrace();
        }
        logger.trace("连接数据库成功，返回数据中...");
        return result;
    }

    public boolean executeSqlUpdate (String sql, Connection connection, Statement statement) {
        connection = null;
        statement = null;
        boolean result = false;
        try {
            Class.forName(JDBC_DRIVER);
            logger.trace("连接数据库...");
            connection = DriverManager.getConnection(DB_URL, sql_USER, sql_PASSWORD);
            statement = connection.createStatement();
            result = statement.execute(sql);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("SQL数据库连接错误！");
            e.printStackTrace();
        }
        logger.trace("连接数据库成功，返回数据中...");
        return result;
    }

    // 释放sql的statement和connection
    public void releaseSql (Connection connection, Statement statement) {
        try {
            if (statement != null) { statement.close(); }
            if (connection != null) { connection.close(); }
        } catch (SQLException e) {
            logger.error("SQL数据库资源释放错误！");
            e.printStackTrace();
        }
    }
}