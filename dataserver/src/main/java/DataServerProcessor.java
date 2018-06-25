/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class DataServerProcessor {
    private static final Logger logger = LogManager.getLogger("dataServerLogger");
    private int portNumber = 0;
    private String dfsFilePath = "/Users/cplayer/DailyDocuments/dfs-files";
    private int commandLen = 8;
    private int headerLen = 64;
    private int chunkLen = 2 * 1024 * 1024;
    private int ipLen = 2 * 1024;
    private ServerSocket serverSocket;
    private String[] commandList = { "save", "get" };

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/dfsSystem?useSSL=false";
    private static final String sql_USER = "root";
    private static final String sql_PASSWORD = "12345678";
    private Connection connection;
    private Statement statement;
    private int portNameServer = 36000;
    private String addressNameServer = "127.0.0.1";

    DataServerProcessor () {};
    DataServerProcessor (int port) {
        this.portNumber = port;
        try {
            serverSocket = new ServerSocket(this.portNumber);
            register();
        } catch (IOException e) {
            logger.error("DataServer Socket初始化错误！");
        }
    }

    void register () {
        try {
            // 第一次register
            Socket socket = new Socket(addressNameServer, portNameServer);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write("register".getBytes());
            outputStream.flush();
            socket.close();

            // 第二次register
            socket = new Socket(addressNameServer, portNameServer);
            outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();
            String resultStr;
            byte[] result = new byte[8];
            outputStream.write("register".getBytes());
            outputStream.flush();
            inputStream.read(result);
            resultStr = new String(result);
            if (!"Success!".equals(resultStr)) {
                logger.error("DataServer注册返回值不正确，注册失败！");
            }
            inputStream.close();
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("DataServer注册错误！");
            e.printStackTrace();
        }
    }

    // 监听函数
    void listen () {
        try {
            while (true) {
                // 在给定socket上持续监听
                // 先监听命令
                String command = receiveCommand();
                int index;
                for (index = 0; index < commandList.length; ++index) {
                    if (commandList[index].contains(command)) {
                        break;
                    }
                }
                switch (index) {
                    case 0:
                        // 存储chunk
                        this.save();
                        break;
                    case 1:
                        // 获取chunk
                        this.get();
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("DataServer监听出错！");
            e.printStackTrace();
        }
    }

    private String receiveCommand () {
        String command = null;
        try {
            Socket socket = serverSocket.accept();
            InputStream inputStream = socket.getInputStream();
            byte[] commandBuffer = new byte[commandLen];
            int readLen;
            readLen = inputStream.read(commandBuffer);
            if (readLen < 8) {
                logger.error("接受到的命令长度不对，标准：8Byte，接受：" + readLen + "Byte.");
            } else {
                command = new String(commandBuffer, "UTF-8");
            }
            inputStream.close();
            socket.close();
            serverSocket.close();
        } catch (IOException e) {
            logger.error("DataServer接收Command出错！");
            e.printStackTrace();
        }
        return command;
    }

    // 存储接受到的chunkData
    private void save () {
        try {
            byte[] chunkData = receiveChunk();
            int fileId = byteToInt(chunkData, 56, 64);
            long fileChunkTotal = byteToLong(chunkData, 24, 40);
            long fileChunk = byteToLong(chunkData, 40, 56);
            long currentMillSecond = System.currentTimeMillis();
            String fileFullPath = dfsFilePath
                                + Integer.valueOf(fileId).toString() + "-"
                                + Long.valueOf(fileChunkTotal).toString() + "-"
                                + Long.valueOf(fileChunk).toString() + "-"
                                + Long.valueOf(currentMillSecond).toString() + ".chunkFile";
            File saveFile = new File(fileFullPath);
            if (!saveFile.exists()) {
                saveFile.createNewFile();
            }
            FileOutputStream outputStream = new FileOutputStream(saveFile);
            outputStream.write(chunkData);
            outputStream.close();
            sqlInsert(fileId, fileChunk, fileChunkTotal, fileFullPath);
        } catch (IOException e) {
            logger.error("DataServer创建文件遇到IO错误！");
            e.printStackTrace();
        }
    }

    void sqlInsert (int fileId, long fileChunk, long fileChunkTotal, String fileFullPath) {
        String sql = String.format("INSERT INTO dataServerFileList VALUES (%d %ld %d %s)", fileId, fileChunk, fileChunkTotal, fileFullPath);
        try {
            ResultSet result = executeSql(sql);
            result.close();
        } catch (SQLException e) {
            logger.error("获取dataServerFileList信息错误！");
            e.printStackTrace();
        }
        try {
            if (statement != null) { statement.close(); }
            if (connection != null) { connection.close(); }
        } catch (SQLException e) {
            logger.error("SQL数据库资源释放错误！");
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
            logger.trace("DataServer连接数据库...");
            connection = DriverManager.getConnection(DB_URL, sql_USER, sql_PASSWORD);
            statement = connection.createStatement();
            result = statement.executeQuery(sql);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("DataServerSQL数据库连接错误！");
            e.printStackTrace();
        }
        logger.trace("DataServer连接数据库成功，返回数据中...");
        return result;
    }

    private void get () {
        byte[] header = receiveHeader();
        int fileId = byteToInt(header, 56, 64);
        long fileChunkTotal = byteToLong(header, 24, 40);
        long fileChunk = byteToLong(header, 40, 56);
        String sql = String.format("SELECT * FROM dataServerFileList WHERE fileID=%d and fileChunk=%d and fileChunkTotal=%d",
                                    fileId, fileChunk, fileChunkTotal);
        ResultSet result = executeSql(sql);
        try {
            int size = 0;
            result.last();
            size = result.getRow();
            if (size > 1) {
                logger.error("返回值不止一个，请检查SQL数据库数据！");
                return;
            }
            result.first();
            String filePath = null;
            while (result.next()) {
                filePath = result.getString("filePath");
            }
            File file = new File(filePath);
            FileInputStream inputStream = new FileInputStream(file);
            byte[] chunkData = new byte[chunkLen + headerLen];
            inputStream.read(chunkData);
            sendChunk(chunkData, headerLen, chunkLen);
            inputStream.close();
        } catch (SQLException e) {
            logger.error("DataServer读取SQL数据库错误，请检查get方法！");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("DataServer get方法读取文件错误！");
            e.printStackTrace();
        }
    }

    private void sendChunk (byte[] sendData, int headerlen, int chunklen) {
        try {
            Socket socket = serverSocket.accept();
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData, 0, headerLen);
            outputStream.flush();
            for (int i = 0; i < chunklen; i += ipLen) {
                outputStream.write(sendData, headerLen + i, ipLen);
                outputStream.flush();
            }
            logger.trace("dataServer发送了" + chunklen + "Bytes数据。");
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("发送数据错误！");
            e.printStackTrace();
        }
    }

    byte[] receiveChunk () {
        byte[] chunkData = new byte[chunkLen + headerLen];
        try {
            Socket socket = serverSocket.accept();
            int readLen;
            InputStream instream = socket.getInputStream();
            readLen = instream.read(chunkData, 0, headerLen);
            logger.trace("读取了" + readLen + "Bytes数据头。");
            readLen = 0;
            for (int i = 0; i < chunkLen; i += ipLen) {
                readLen += instream.read(chunkData, headerLen + i, ipLen);
            }
            logger.trace("读取了" + readLen + "Bytes数据。");
            instream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("socket连接错误，请检查网络连接，并将此错误报告系统管理员！");
            e.printStackTrace();
        }
        return chunkData;
    }

    byte[] receiveHeader() {
        byte[] header = new byte[headerLen];
        try {
            Socket socket = serverSocket.accept();
            int readLen;
            InputStream instream = socket.getInputStream();
            readLen = instream.read(header);
            if (readLen < headerLen) {
                logger.error("socket读取header错误，header长度不足！");
            }
            logger.trace("读取了" + readLen + "Bytes长度的命令。");
            logger.trace("本次读取的命令为：" + new String(header, "UTF-8"));
            instream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("socket连接错误，请检查网络连接，并将此错误报告系统管理员！");
            e.printStackTrace();
        }
        return header;
    }

    int byteToInt (byte[] arr, int start, int end) {
        int ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret * 256 + arr[i];
        }
        return ret;
    }

    long byteToLong (byte[] arr, int start, int end) {
        long ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret * 256 + arr[i];
        }
        return ret;
    }
}
