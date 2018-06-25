/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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
        byte[] chunkData = receiveChunk();
    }

    byte[] receiveChunk () {
        byte[] chunkData = new byte[chunkLen + headerLen];
        try {
            Socket socket = serverSocket.accept();
            int readLen;
            InputStream instream = socket.getInputStream();
            readLen = instream.read(chunkData, 0, headerLen);
            logger.trace("读取了" + readLen + "Bytes数据。");
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
}
