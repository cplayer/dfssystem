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

class NameServerProcessor {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");
    private NameServerSocket nameSocket;
    private byte[] buffer;

    private Connection connection;
    private Statement statement;
    private NameServerSqlService sqlService;
    private TreeMap<Integer, SqlServerListNode> nameServerFileListMap;
    private ArrayList<DataServerInfo> dataServers;

    private String[] commandList = { "upload", "list", "download", "register" };
    private int fileIdUpperBound = 1 << 30;

    NameServerProcessor () {
        logger.trace("NameServer启动服务。");
        // port默认为36000;
        nameSocket = new NameServerSocket(36000);
        nameServerFileListMap = new TreeMap<>();
        sqlService = new NameServerSqlService();
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
        // 先从client接收文件路径，暂时支持2MB的路径长度
        try {
            byte[] routeData = nameSocket.receive(2 * 1024 * 1024);
            String filePath = new String(routeData, "UTF-8");
            String sql = String.format("SELECT * from nameServerFileList WHERE filePath='%s'", filePath);
            ResultSet result = sqlService.executeSql(sql, connection, statement);
            int fileID = 0;
            String fileName;
            while (result.next()) {
                fileID = result.getInt("fileID");
                fileName = result.getString("fileName");
            }
            result.close();
            sqlService.releaseSql(connection, statement);
            ArrayList<byte[]> chunks = new ArrayList<>();
            int totalChunks = 0;
            for (DataServerInfo dataServer : dataServers) {
                InetAddress curAddress = dataServer.address;
                int curPort = dataServer.port;
                this.nameSocket.sendData(Convert.intToBytes(fileID), curAddress, curPort);
                String status = new String(this.nameSocket.receive(8, curAddress, curPort), "UTF-8");
                if (status.equals("Accepted")) {
                    totalChunks = Convert.byteToInt(this.nameSocket.receive(4, curAddress, curPort), 0, 4);
                    break;
                }
            }
            for (int i = 0; i < totalChunks; ++i) {
                byte[] chunkData;
                for (DataServerInfo dataServer : dataServers) {
                    InetAddress curAddress = dataServer.address;
                    int curPort = dataServer.port;

                }
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
        int nextFileId = random.nextInt(fileIdUpperBound);
        while (nameServerFileListMap.containsKey(nextFileId)) {
            nextFileId = random.nextInt(fileIdUpperBound);
        }
        logger.trace("新的FileID = " + nextFileId);
        this.nameSocket.sendData(Convert.intToBytes(nextFileId));

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
        dataServers.add(result);
    }
}
