/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.sql.*;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.ArrayList;

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
    private TreeMap<Integer, SqlServerListNode> nameServerFileListMap;
    private ArrayList<DataServerInfo> dataServers;

    private String[] commandList = { "upload", "list", "download", "register" };
    private int fileIdUpperBound = 1 << 30;

    NameServerProcessor () {
        logger.trace("NameServer启动服务。");
        // port默认为36000;
        nameSocket = new NameServerSocket(36000);
        nameServerFileListMap = new TreeMap<>();
        dataServers.clear();
        loadSqlInfo();
    }

    // 主要逻辑函数
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
                        this.download();
                        break;
                    case 3:
                        // 注册
                        this.register();
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

    // 下载功能函数
    private void download () {
        // TODO: 下载
    }

    // 上传功能函数
    private void upload () {
        Random random = new Random(System.nanoTime());
        byte[] chunkData;
        int nextFileId = random.nextInt(fileIdUpperBound);
        while (nameServerFileListMap.containsKey(nextFileId)) {
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
        // 分发策略：分发至少3个负载最少的dataServer
        ArrayList<DataServerInfo> distriList = new ArrayList<>(dataServers);
        distriList.sort(new Comparator<DataServerInfo>() {
            @Override
            public int compare(DataServerInfo o1, DataServerInfo o2) {
                return o1.load - o2.load;
            }
        });
        // 前三个
        for (int i = 0; i < 3; ++i) {
            this.nameSocket.sendData(chunkData, distriList.get(i).address, distriList.get(i).port);
        }
    }

    // 内部函数，用于加载SQL信息
    private void loadSqlInfo () {
        String sql = "SELECT * from nameServerFileList";
        try {
            ResultSet result = executeSql(sql);
            while (result.next()) {
                int fileID = result.getInt("fileID");
                String fileName = result.getString("fileName");
                String filePath = result.getString("filePath");
                if (nameServerFileListMap.containsKey(fileID) == false) {
                    logger.trace("已读取fileID = " + fileID + "的记录。");
                    nameServerFileListMap.put(fileID, new SqlServerListNode(fileID, fileName, filePath));
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
            logger.trace("获取nameServerFileList信息完毕！");
        } catch (SQLException e) {
            logger.error("获取nameServerFileList信息错误！");
            e.printStackTrace();
        }
    }

    // 用于运行SQL语句，需要在程序中手动关闭connection和statement
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

    // int转bytes数组
    private byte[] intToBytes (int value) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; --i) {
            result[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
        return result;
    }

    // 注册函数
    private void register () {
        DataServerInfo result = nameSocket.register();
        dataServers.add(result);
    }

    // 用于服务sqlServerList的私有类
    private class SqlServerListNode {
        int fileID;
        String fileName;
        String filePath;
        SqlServerListNode () {
            fileID = 0; 
            fileName = null; 
            filePath = null;
        }

        SqlServerListNode (int fileID, String fileName, String filePath) {
            this.fileID = fileID;
            this.fileName = fileName;
            this.filePath = filePath;
        }
    }

}

// 用于IP注册的类
class DataServerInfo {
    InetAddress address;
    int port;
    int load;
    DataServerInfo () {
        address = null;
        port = 0;
        load = 0;
    }

    DataServerInfo (InetAddress address, int port) {
        this.address = address;
        this.port = port;
        load = 0;
    }
}