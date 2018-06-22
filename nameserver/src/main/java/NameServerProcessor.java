/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.Random;

class NameServerProcessor {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");
    private NameServerSocket nameSocket;
    private byte[] buffer;

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    // 去除了安全连接，待在实验室机器上检查
    private static final String DB_URL = "jdbc:mysql://localhost:3306/dfsSystem?useSSL=false";
    private static final String sql_USER = "root";
    private static final String sql_PASSWORD = "12345678";
    private Connection connection;
    private Statement statement;
    private TreeMap<Integer, sqlServerListNode> dataServerListMap;

    private String[] commandList = { "upload", "list", "download" };
    private int fileIdUpperBound = 1 << 30;

    NameServerProcessor () {
        logger.trace("NameServer启动服务。");
        // port默认为36000;
        nameSocket = new NameServerSocket(36000);
        dataServerListMap = new TreeMap<>();
        loadSqlInfo();
    }

    void run () {
        while (true) {
            try {
                buffer = nameSocket.receiveCommand();
                logger.trace("收到总共" + buffer.length + "Bytes数据。");
                String command = new String(buffer, "UTF-8");
                int commandIndex;
                for (commandIndex = 0; commandIndex < commandList.length; ++commandIndex) {
                    if (command.contains(commandList[commandIndex])) { break; }
                }
                switch (commandIndex) {
                    case 0:
                        // 上传
                        this.upload();
                        break;
                    case 1:
                        // 列出
                        break;
                    case 2:
                        // 下载
                        break;
                    default:
                        logger.error("client发送的命令不正确！");
                        break;
                }
            } catch (UnsupportedEncodingException e) {
                logger.error("命令编码不正确！");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void upload () {
        Random random = new Random(System.nanoTime());
        byte[] chunkData;
        int nextFileId = random.nextInt(fileIdUpperBound);
        while (dataServerListMap.containsKey(nextFileId)) {
            nextFileId = random.nextInt(fileIdUpperBound);
        }
        logger.trace("新的FileID = " + nextFileId);
        this.nameSocket.sendData(intToBytes(nextFileId));

        long totIndex, curIndex;
        do {
            chunkData = nameSocket.receiveChunk();
            byte[] tmp = Arrays.copyOfRange(chunkData, 24, 40);
            totIndex = 0;
            for (int i = 0; i < tmp.length; ++i) {
                totIndex = (totIndex << 4) + tmp[i];
            }
            tmp = Arrays.copyOfRange(chunkData, 40, 56);
            curIndex = 0;
            for (int i = 0; i < tmp.length; ++i) {
                curIndex = (curIndex << 4) + tmp[i];
            }
            logger.trace("目前接收区块序号为：" + curIndex + ", 总序号为：" + totIndex);
        } while (curIndex < totIndex);
    }

    private void loadSqlInfo () {
        String sql = "SELECT * from dataServerList";
        try {
            ResultSet result = executeSql(sql);
            while (result.next()) {
                int fileID = result.getInt("fileID");
                String fileName = result.getString("fileName");
                String filePath = result.getString("filePath");
                if (dataServerListMap.containsKey(fileID) == false) {
                    logger.trace("已读取fileID = " + fileID + "的记录。");
                    dataServerListMap.put(fileID, new sqlServerListNode(fileID, fileName, filePath));
                } else {
                    logger.error("SQL数据库中出现重复fileID！重复ID为：" + fileID);
                    break;
                }
            }
            result.close();
            try {
                if (statement != null) { statement.close(); }
                if (connection != null) { connection.close(); }
            } catch (SQLException e) {
                logger.error("SQL数据库资源释放错误！");
                e.printStackTrace();
            }
            logger.trace("获取dataServerList信息完毕！");
        } catch (SQLException e) {
            logger.error("获取dataServerList信息错误！");
            e.printStackTrace();
        }
    }

    private ResultSet executeSql (String sql) {
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

    private byte[] intToBytes(int value) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; --i) {
            result[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
        return result;
    }

    class sqlServerListNode {
        int fileID;
        String fileName;
        String filePath;
        sqlServerListNode () {
            fileID = 0; 
            fileName = null; 
            filePath = null;
        }

        sqlServerListNode (int fileID, String fileName, String filePath) {
            this.fileID = fileID;
            this.fileName = fileName;
            this.filePath = filePath;
        }
    }

}

