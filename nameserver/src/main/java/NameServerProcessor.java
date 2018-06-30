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
import java.util.Comparator;
import java.util.Random;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class NameServerProcessor {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");
    private NameServerSocket nameSocket;
    private byte[] buffer;

    private Connection connection;
    private Statement statement;
    private NameServerSqlService sqlService;
    private TreeMap<Integer, SqlServerListNode> nameServerFileListMap;
    private ArrayList<DataServerInfo> dataServers;

    private String[] commandList = { "upload", "list", "download", "register", "checkID", "checkFth", "offset", "md5" };
    private int fileIdUpperBound = 1 << 30;

    NameServerProcessor () {
        logger.trace("NameServer启动服务。");
        // port默认为36000;
        nameSocket = new NameServerSocket(36000);
        nameServerFileListMap = new TreeMap<>();
        sqlService = new NameServerSqlService();
        dataServers = new ArrayList<>();
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
                        this.list();
                        break;
                    case 2:
                        // 下载
                        this.download();
                        break;
                    case 3:
                        // 注册
                        this.register();
                        break;
                    case 4:
                        // （针对客户端）检查id对应的path
                        this.checkID();
                        break;
                    case 5:
                        // 检查给定path文件是否存在
                        this.checkPath();
                        break;
                    case 6:
                        // 检查offset的数据
                        this.checkOffset();
                        break;
                    case 7:
                        // 检查同样chunk的md5值
                        this.checkMD5();
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

    private void list () {
        try {
            String path = new String(this.nameSocket.receive(255, false), "UTF-8").trim();
            String sql = String.format("SELECT * FROM nameServerFileList WHERE filePath LIKE '%%%s%%'", path);
            logger.trace(String.format("待执行的list命令的sql语句为：%s", sql));
            ResultSet result = this.sqlService.executeSql(sql, connection, statement);
            ArrayList<String> fileNames = new ArrayList<>();
            ArrayList<String> filePaths = new ArrayList<>();
            ArrayList<Long> fileLens = new ArrayList<>();
            ArrayList<String> fileIDs = new ArrayList<>();
            fileLens.clear(); fileNames.clear(); filePaths.clear(); fileIDs.clear();
            while (result.next()) {
                String fileName = result.getString("fileName");
                String filePath = result.getString("filePath");
                String fileID = result.getString("fileID");
                long fileLen = Long.parseLong(result.getString("fileLen"));
                fileNames.add(fileName);
                filePaths.add(filePath);
                fileLens.add(fileLen);
                fileIDs.add(fileID);
            }
            if (fileNames.size() > 0) {
                // 发送状态
                this.nameSocket.sendData("Accepted".getBytes());
                this.nameSocket.sendData(Convert.intToBytes(fileNames.size()));
                for (int i = 0; i < fileNames.size(); ++i) {
                    this.nameSocket.sendData(fileNames.get(i).getBytes());
                    this.nameSocket.sendData(filePaths.get(i).getBytes());
                    this.nameSocket.sendData(Convert.longToBytes(fileLens.get(i)));
                    this.nameSocket.sendData(fileIDs.get(i).getBytes());
                }
            } else {
                this.nameSocket.sendData("Denied".getBytes());
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("client发送的路径编码不正确！");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("执行列举时访问sql不正确！");
            e.printStackTrace();
        }
    }

    private int check (int fileID) {
        int totalChunks = -1;
        try {
            logger.trace("准备向DataServers查询文件总块数");
            for (DataServerInfo dataServer : dataServers) {
                InetAddress curAddress = dataServer.address;
                int curPort = dataServer.port;
                this.nameSocket.sendDataDirect("checkID ".getBytes(), curAddress, curPort);
                this.nameSocket.sendDataDirect(Convert.intToBytes(fileID), curAddress, curPort);
                String status = new String(this.nameSocket.receive(8, curAddress, curPort), "UTF-8");
                if (status.equals("Accepted")) {
                    totalChunks = Convert.byteToInt(this.nameSocket.receive(4, curAddress, curPort), 0, 4);
                    break;
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("DataServer返回编码不正确！");
            e.printStackTrace();
        }
        logger.trace("查询成功！");
        return totalChunks;
    }

    private boolean checkChunkNum (int chunkNum, int fileID, InetAddress curAddress, int port) {
        this.nameSocket.sendDataDirect("checkNum".getBytes(), curAddress, port);
        this.nameSocket.sendDataDirect(Convert.intToBytes(fileID), curAddress, port);
        this.nameSocket.sendDataDirect(Convert.intToBytes(chunkNum), curAddress, port);
        String status = new String(this.nameSocket.receive(8, curAddress, port));
        logger.trace(String.format("向%s:%d查询的结果为：%s", curAddress.getHostAddress(), port, status));
        if (status.contains("Accepted")) {
            return true;
        } else {
            return false;
        }
    }

    // 检查ID函数
    private void checkID () {
        try {
            int fileID = Convert.byteToInt(nameSocket.receive(4, false), 0, 4);
            String sql = String.format("SELECT * FROM nameServerFileList WHERE fileID=%d", fileID);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            String filePath = null;
            if (result.next()) {
                filePath = result.getString("filePath");
            } else {
                filePath = "File Not Found";
            }
            this.nameSocket.sendData(filePath.getBytes());
        } catch (SQLException e) {
            logger.error("DataServer数据库访问错误！");
            e.printStackTrace();
        }
    }

    private void checkPath () {
        try {
            logger.trace("开始接受数据...");
            byte[] pathData = nameSocket.receive(2048, false);
            String filePath = new String(pathData, "UTF-8").trim();
            logger.trace(String.format("接收的文件路径为：%s", filePath));
            String sql = String.format("SELECT * from nameServerFileList WHERE filePath='%s'", filePath);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            String returnStatus;
            if (result.next()) returnStatus = "Accepted";
            else returnStatus = "Denied";
            sqlService.releaseSql(connection, statement);
            this.nameSocket.sendData(returnStatus.getBytes());
        } catch (UnsupportedEncodingException e) {
            logger.error("client给出的文件路径不正确！");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("下载文件时访问SQL数据库错误！");
            e.printStackTrace();
        }
    }

    private void checkOffset () {
        try {
            int chunkNum = Convert.byteToInt(this.nameSocket.receive(4, false), 0, 4);
            String filePath = new String(this.nameSocket.receive(255, false), "UTF-8").trim();
            long offset = Convert.byteToLong(this.nameSocket.receive(8, false), 0, 8);
            logger.trace(String.format("接收的文件路径为：%s", filePath));
            String sql = String.format("SELECT * from nameServerFileList WHERE filePath='%s'", filePath);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            String status = "Denied";
            int fileID = -1;
            while (result.next()) {
                fileID = result.getInt("fileID");
            }
            result.close();
            sqlService.releaseSql(connection, statement);
            if (fileID != -1) {
                status = "Accepted";
            }
            this.nameSocket.sendData(status.getBytes());
            if (fileID != -1) {
                // 先依次查询对应的chunk的总长度
                int totalChunks = check(fileID);
                byte[] chunkData = null;
                for (DataServerInfo dataServer : dataServers) {
                    InetAddress curAddress = dataServer.address;
                    int curPort = dataServer.port;
                    logger.trace(String.format("正在向DataServer（%s）进行查询...", curAddress.getHostAddress()));
                    if (checkChunkNum(chunkNum, fileID, curAddress, curPort)) {
                        this.nameSocket.sendDataDirect("get     ".getBytes(), curAddress, curPort);
                        byte[] sendHeader = new byte[64];
                        Convert.intIntoBytes(fileID, sendHeader, 60, 64);
                        Convert.longIntoBytes(totalChunks, sendHeader, 32, 40);
                        Convert.intIntoBytes(chunkNum, sendHeader, 52, 56);
                        this.nameSocket.sendDataDirect(sendHeader, curAddress, curPort);
                        logger.trace(String.format("正在接收第%d个Chunk...", chunkNum));
                        chunkData = this.nameSocket.receiveChunk(curAddress, curPort);
                        logger.trace(String.format("查询成功！第%d个chunk已发送！", chunkNum));
                        break;
                    }
                }
                byte[] sends = new byte[1];
                sends[0] = chunkData[64 + (int)(offset % (2 * 1024 * 1024))];
                this.nameSocket.sendData(sends);
            }
            logger.trace("追踪offset结束。");
        } catch (UnsupportedEncodingException e) {
            logger.error("client给出的下载文件路径不正确！");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("下载文件时访问SQL数据库错误！");
            e.printStackTrace();
        }
    }

    private void checkMD5 () {
        try {
            String filePath = new String(this.nameSocket.receive(255, false), "UTF-8").trim();
            int chunkNum = Convert.byteToInt(this.nameSocket.receive(4, false), 0, 4);
            logger.trace(String.format("接收的文件路径为：%s", filePath));
            String sql = String.format("SELECT * from nameServerFileList WHERE filePath='%s'", filePath);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            int fileID = 0;
            while (result.next()) {
                fileID = result.getInt("fileID");
            }
            result.close();
            sqlService.releaseSql(connection, statement);
            logger.trace(String.format("查询到的文件ID为：%d", fileID));
            // 先依次查询对应的chunk的总长度
            int totalChunks = check(fileID);
            ArrayList<String> md5value = new ArrayList<>();
            md5value.clear();
            for (DataServerInfo dataServer : dataServers) {
                byte[] chunkData;
                InetAddress curAddress = dataServer.address;
                int curPort = dataServer.port;
                logger.trace(String.format("正在向DataServer（%s）进行查询...", curAddress.getHostAddress()));
                if (checkChunkNum(chunkNum, fileID, curAddress, curPort)) {
                    this.nameSocket.sendDataDirect("get     ".getBytes(), curAddress, curPort);
                    byte[] sendHeader = new byte[64];
                    Convert.intIntoBytes(fileID, sendHeader, 60, 64);
                    Convert.longIntoBytes(totalChunks, sendHeader, 32, 40);
                    Convert.intIntoBytes(chunkNum, sendHeader, 52, 56);
                    this.nameSocket.sendDataDirect(sendHeader, curAddress, curPort);
                    logger.trace(String.format("正在接收第%d个Chunk...", chunkNum));
                    chunkData = this.nameSocket.receiveChunk(curAddress, curPort);
                    logger.trace(String.format("查询成功！第%d个chunk已发送！", chunkNum));
                    MessageDigest messageDigest = MessageDigest.getInstance("MD5");
                    byte[] md5 = new byte[chunkData.length - 64];
                    System.arraycopy(chunkData, 64, md5, 0, chunkData.length - 64);
                    messageDigest.update(md5);
                    byte[] resultByteArray = messageDigest.digest();
                    char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9', 'A','B','C','D','E','F' };
                    char[] resultCharArray = new char[resultByteArray.length * 2];
                    int index = 0;
                    for (byte b : resultByteArray) {
                        resultCharArray[index++] = hexDigits[b >>> 4 & 0xf];
                        resultCharArray[index++] = hexDigits[b & 0xf];
                    }
                    String md5result = new String(resultCharArray);
                    md5value.add(md5result);
                }
            }
            String status = "Accepted";
            for (int i = 0; i < md5value.size(); ++i) {
                if (!md5value.get(i).equals(md5value.get(0))) {
                    status = "Denied";
                    break;
                }
            }
            this.nameSocket.sendData(status.getBytes());
            this.nameSocket.sendData(Convert.intToBytes(md5value.size()));
            for (int i = 0; i < md5value.size(); ++i) {
                this.nameSocket.sendData(md5value.get(i).getBytes());
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("client给出的下载文件路径不正确！");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("下载文件时访问SQL数据库错误！");
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            logger.error("没有md5算法！");
            e.printStackTrace();
        }
    }

    // 下载功能函数
    private void download () {
        try {
            // 先从client接收文件路径，暂时支持2KB的路径长度
            byte[] routeData = nameSocket.receive(2048, false);
            String filePath = new String(routeData, "UTF-8").trim();
            logger.trace(String.format("接收的文件路径为：%s", filePath));
            String sql = String.format("SELECT * from nameServerFileList WHERE filePath='%s'", filePath);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            int fileID = 0;
            String fileName = null;
            while (result.next()) {
                fileID = result.getInt("fileID");
                fileName = result.getString("fileName");
            }
            result.close();
            sqlService.releaseSql(connection, statement);
            logger.trace(String.format("查询到的文件ID为：%d，文件名为：%s", fileID, fileName));
            ArrayList<byte[]> chunks = new ArrayList<>();
            // 先依次查询对应的chunk的总长度
            int totalChunks = check(fileID);
            // 然后逐个chunk去获取
            for (int i = 1; i <= totalChunks; ++i) {
                byte[] chunkData;
                for (DataServerInfo dataServer : dataServers) {
                    InetAddress curAddress = dataServer.address;
                    int curPort = dataServer.port;
                    logger.trace(String.format("正在向DataServer（%s）进行查询...", curAddress.getHostAddress()));
                    if (checkChunkNum(i, fileID, curAddress, curPort)) {
                        this.nameSocket.sendDataDirect("get     ".getBytes(), curAddress, curPort);
                        byte[] sendHeader = new byte[64];
                        Convert.intIntoBytes(fileID, sendHeader, 60, 64);
                        Convert.longIntoBytes(totalChunks, sendHeader, 32, 40);
                        Convert.intIntoBytes(i, sendHeader, 52, 56);
                        this.nameSocket.sendDataDirect(sendHeader, curAddress, curPort);
                        logger.trace(String.format("正在接收第%d个Chunk...", i));
                        chunkData = this.nameSocket.receiveChunk(curAddress, curPort);
                        chunks.add(chunkData);
                        logger.trace(String.format("查询成功！第%d个chunk已发送！", i));
                        break;
                    }
                }
            }
            logger.trace("准备向Client发送chunk数据...");
            this.nameSocket.sendData(fileName.getBytes());
            this.nameSocket.sendData(Convert.intToBytes(totalChunks));
            for (byte[] chunkData : chunks) {
                this.nameSocket.sendData(chunkData, true);
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("client给出的下载文件路径不正确！");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("下载文件时访问SQL数据库错误！");
            e.printStackTrace();
        }
    }

    // 上传功能函数
    private void upload () {
        Random random = new Random(System.nanoTime());
        byte[] chunkData;
        // 接收文件总长度
        long fileLen = Convert.byteToLong(this.nameSocket.receive(8, false), 0, 8);
        // 文件路径
        String filePath = new String(this.nameSocket.receive(255, false), 0, 255).trim();
        // 文件名称
        String fileName = new String(this.nameSocket.receive(255, false), 0, 255).trim();
        filePath = filePath + fileName;
        logger.trace("fileLen = " + fileLen);
        logger.trace("filePath = " + filePath);
        logger.trace("fileName = " + fileName);
        int nextFileId = random.nextInt(fileIdUpperBound);
        while (nameServerFileListMap.containsKey(nextFileId)) {
            nextFileId = random.nextInt(fileIdUpperBound);
        }
        logger.trace("新的FileID = " + nextFileId);
        this.nameSocket.sendData(Convert.intToBytes(nextFileId));
        // 发送数据
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
            // 分发策略：分发至少3个负载最少的dataServer
            ArrayList<DataServerInfo> distriList = new ArrayList<>(dataServers);
            distriList.sort(new Comparator<DataServerInfo>() {
                @Override
                public int compare(DataServerInfo o1, DataServerInfo o2) {
                    return o1.load - o2.load;
                }
            });
            // 前三个，若不足三个按现有的算
            for (int i = 0; i < Math.min(3, dataServers.size()); ++i) {
                // 发送命令之后接数据
                this.nameSocket.sendDataDirect("save    ".getBytes(), distriList.get(i).address, distriList.get(i).port);
                this.nameSocket.sendData(chunkData, distriList.get(i).address, distriList.get(i).port);
                distriList.get(i).load += 1;
            }
        } while (curIndex < totIndex);
        // 在记录中添加sql信息
        String sql = String.format("INSERT INTO nameServerFileList VALUES (%d, '%s', '%s', %d)",
                                    nextFileId,
                                    fileName,
                                    filePath,
                                    fileLen);
        sqlService.executeSqlUpdate(sql, connection, statement);
        sqlService.releaseSql(connection, statement);
        logger.trace("向DataServer写入数据完毕！");
    }

    // 内部函数，用于加载SQL信息
    private void loadSqlInfo () {
        String sql = "SELECT * from nameServerFileList";
        try {
            ResultSet result = sqlService.executeSql(sql, connection, statement);
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
            sqlService.releaseSql(connection, statement);
            logger.trace("获取nameServerFileList信息完毕！");
        } catch (SQLException e) {
            logger.error("获取nameServerFileList信息错误！");
            e.printStackTrace();
        }
    }  

    // 注册函数
    private void register () {
        DataServerInfo result = nameSocket.register();
        logger.trace("注册完成！");
        dataServers.add(result);
    }
}
