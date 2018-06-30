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
import java.net.Socket;
import java.sql.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class DataServerProcessor {
    private static final Logger logger = LogManager.getLogger("dataServerLogger");
    private int portNumber = 0;
    private String dfsFilePath = "/Users/cplayer/DailyDocuments/dfs-files/";
    private String sqlTable = "dataServerFileList";
    
    private int headerLen = 64;
    private int chunkLen = 2 * 1024 * 1024;
    
    private String[] commandList = { "save", "get", "checkID", "checkNum" };
    private DataServerSocket serverSocket;

    private Connection connection;
    private PreparedStatement statement;
    private DataServerSqlService sqlService;
    private int portNameServer = 36000;
    private String addressNameServer = "127.0.0.1";

    DataServerProcessor () {};
    DataServerProcessor (int port) {
        this.portNumber = port;
        serverSocket = new DataServerSocket(this.portNumber);
        sqlService = new DataServerSqlService();
        register();
    }

    DataServerProcessor (int port, String dfsFilePath) {
        this.portNumber = port;
        this.dfsFilePath = dfsFilePath;
        serverSocket = new DataServerSocket(this.portNumber);
        sqlService = new DataServerSqlService();
        register();
    }

    DataServerProcessor (int port, String dfsFilePath, String sqlTable) {
        this.portNumber = port;
        this.dfsFilePath = dfsFilePath;
        this.sqlTable = sqlTable;
        serverSocket = new DataServerSocket(this.portNumber);
        sqlService = new DataServerSqlService();
        register();
    }

    void register () {
        try {
            // 第一次register，发送命令
            serverSocket.sendCommand("register".getBytes(), addressNameServer, portNameServer);

            // 第二次register
            Socket socket = new Socket(addressNameServer, portNameServer);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();
            String resultStr;
            byte[] result = new byte[8];
            outputStream.write("register".getBytes());
            outputStream.flush();
            // 发送端口号
            outputStream.write(Convert.intToBytes(this.portNumber));
            outputStream.flush();
            // 取tableSize，即负载
            String sql = "SELECT * FROM dataServerFileList";
            ResultSet resultSet = sqlService.executeSql(sql, connection, statement);
            int resultSetSize = 0;
            resultSet.last();
            resultSetSize = resultSet.getRow();
            resultSet.close();
            sqlService.releaseSql(connection, statement);
            byte[] writeBuffer = new byte[8];
            Convert.copyTo(writeBuffer, 4, 8, Convert.intToBytes(resultSetSize));
            outputStream.write(writeBuffer);

            inputStream.read(result);
            resultStr = new String(result);
            if (!"Success!".equals(resultStr)) {
                logger.error("DataServer注册返回值不正确，注册失败！");
            }
            inputStream.close();
            outputStream.close();
            socket.close();
            logger.trace("注册成功！");
        } catch (IOException e) {
            logger.error("DataServer注册错误！");
            System.exit(0);
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("DataServer注册读取SQL数据库错误！");
            System.exit(0);
            e.printStackTrace();
        }
    }

    // 监听函数
    void listen () {
        try {
            while (true) {
                // 在给定socket上持续监听
                // 先监听命令
                logger.trace("监听中...");
                String command = serverSocket.receiveCommand();
                logger.trace("命令为：" + command);
                int index;
                for (index = 0; index < commandList.length; ++index) {
                    if (command.contains(commandList[index])) {
                        break;
                    }
                }
                switch (index) {
                    case 0:
                        // 存储chunk
                        logger.trace("NameServer发送了存储命令。");
                        this.save();
                        break;
                    case 1:
                        // 获取chunk
                        this.get();
                        break;
                    case 2:
                        // 检查chunk
                        this.check();
                        break;
                    case 3:
                        // 检查chunkNumber是否存在
                        this.checkChunk();
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


    // 存储接受到的chunkData
    private void save () {
        try {
            logger.trace("开始收取NameServer发送的数据。");
            byte[] chunkData = serverSocket.receiveChunk();
            int fileId = Convert.byteToInt(chunkData, 56, 64);
            long fileChunkTotal = Convert.byteToLong(chunkData, 24, 40);
            long fileChunk = Convert.byteToLong(chunkData, 40, 56);
            long currentMillSecond = System.currentTimeMillis();
            String fileFullPath = dfsFilePath
                                + Integer.valueOf(fileId).toString() + "-"
                                + Long.valueOf(fileChunkTotal).toString() + "-"
                                + Long.valueOf(fileChunk).toString() + "-"
                                + Long.valueOf(currentMillSecond).toString() + ".chunkFile";
            logger.trace(String.format("fileId = %d, fileChunkTotal = %d, fileChunk = %d, \n fileFullPath = %s",
                                        fileId, fileChunkTotal, fileChunk, fileFullPath));
            File saveFile = new File(fileFullPath);
            if (!saveFile.exists()) {
                saveFile.createNewFile();
            }
            FileOutputStream outputStream = new FileOutputStream(saveFile);
            outputStream.write(chunkData);
            outputStream.close();
            logger.trace("写入文件完毕！");
            sqlInsert(fileId, fileChunk, fileChunkTotal, fileFullPath);
            logger.trace("sql插入完毕！");
        } catch (IOException e) {
            logger.error("DataServer创建文件遇到IO错误！");
            e.printStackTrace();
        }
    }

    void sqlInsert (int fileId, long fileChunk, long fileChunkTotal, String fileFullPath) {
        String sql = String.format("INSERT INTO dataServerFileList (fileID, fileChunk, fileChunkTotal, filePath) VALUES (%d, %d, %d, '%s')", fileId, fileChunk, fileChunkTotal, fileFullPath);
        logger.trace("sql语句为: " + sql);
        boolean result = sqlService.executeSqlUpdate(sql, connection, statement);
        if (!result) {
            logger.error("SQL数据库更新错误！");
        }
        sqlService.releaseSql(connection, statement);
    }

    private void check () {
        byte[] id = new byte[4];
        String message;
        try {
            id = serverSocket.receive(4);
            int fileId = Convert.byteToInt(id, 0, 4);
            String sql = String.format("SELECT * FROM dataServerFileList WHERE fileID=%d",
                                        fileId);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            if (result.next()) {
                message = "Accepted";
            } else {
                message = "Denied  ";
            }
            serverSocket.send(message.getBytes());
            if (message.equals("Accepted")) {
                int fileTotalLen = result.getInt("fileChunkTotal");
                serverSocket.send(Convert.intToBytes(fileTotalLen));
            }
            sqlService.releaseSql(connection, statement);                                    
        } catch (SQLException e) {
            logger.error("dataServer数据库访问错误！");
            e.printStackTrace();
        }
    }

    private void checkChunk () {
        try {
            int fileID = Convert.byteToInt(serverSocket.receive(4), 0, 4);
            long fileChunk = Convert.byteToLong(serverSocket.receive(4), 0, 4);
            String sql = String.format("SELECT * FROM dataServerFileList WHERE fileID=%d and fileChunk=%d",
                                        fileID,
                                        fileChunk);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            String message;
            if (result.next()) {
                message = "Accepted";
            } else {
                message = "Denied";
            }
            serverSocket.send(message.getBytes());
        } catch (SQLException e) {
            logger.error("dataServer数据库访问错误！");
            e.printStackTrace();
        }
    }

    private void get () {
        byte[] header = serverSocket.receiveHeader();
        int fileId = Convert.byteToInt(header, 56, 64);
        long fileChunkTotal = Convert.byteToLong(header, 24, 40);
        long fileChunk = Convert.byteToLong(header, 40, 56);
        logger.trace(String.format("NameServer需要的fileID = %d, fileChunk = %d, fileChunkTotal = %d", fileId, fileChunk, fileChunkTotal));
        String sql = String.format("SELECT * FROM dataServerFileList WHERE fileID=%d and fileChunk=%d and fileChunkTotal=%d",
                                    fileId, fileChunk, fileChunkTotal);
        ResultSet result = sqlService.executeSql(sql, connection, statement);
        try {
            int size = 0;
            result.last();
            size = result.getRow();
            if (size > 1) {
                logger.error("返回值不止一个，请检查SQL数据库数据！");
                serverSocket.sendCommand("error".getBytes());
                return;
            }
            result.first();
            String filePath = null;
            filePath = result.getString("filePath");
            // while (result.next()) {
            // }
            logger.trace(String.format("已找到文件路径为：%s", filePath));
            File file = new File(filePath);
            FileInputStream inputStream = new FileInputStream(file);
            byte[] chunkData = new byte[chunkLen + headerLen];
            inputStream.read(chunkData);
            serverSocket.sendChunk(chunkData, headerLen, chunkLen);
            inputStream.close();
            logger.trace("已发送指定文件！");
        } catch (SQLException e) {
            logger.error("DataServer读取SQL数据库错误，请检查get方法！");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("DataServer get方法读取文件错误！");
            e.printStackTrace();
        } finally {
            sqlService.releaseSql(connection, statement);
        }
    }

}
