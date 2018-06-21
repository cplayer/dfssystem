/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.io.InputStream;
import java.io.OutputStream;

class NameServerSocket {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");
    private ServerSocket ssocket;
    private Socket socket;
    private int headerLen = 64;
    private int chunkLen = 2 * 1024 * 1024;
    private int commandLen = 8;
    private int port = 0;

    NameServerSocket (int port) {
        try {
            this.port = port;
            ssocket = new ServerSocket(this.port);
        } catch (IOException e) {
            logger.error("socket初始化错误，请检查传入的端口号是否正确！");
            e.printStackTrace();
        }
    }

    // 接收一个完整的块信息（包含块头+块内容），用于上传命令
    byte[] receiveChunk () {
        // byte[] header = new byte[headerLen];
        // byte[] chunk = new byte[chunkLen];
        byte[] chunkData = new byte[chunkLen + headerLen];
        try {
            socket = ssocket.accept();
            int readLen;
            InputStream instream = socket.getInputStream();
            // readLen = instream.read(header);
            // if (readLen < headerLen) {
            //     logger.error("socket读取chunk头错误：长度不足64Byte！");
            //     return new byte[0];
            // };
            // readLen = instream.read(chunk);
            // if (readLen < chunkLen) {
            //     logger.error("socket读取chunk数据错误，长度不足2MB！");
            // }
            readLen = instream.read(chunkData);
            logger.trace("读取了" + readLen + "Bytes数据。");
            instream.close();
        } catch (IOException e) {
            logger.error("socket连接错误，请检查网络连接，并将此错误报告系统管理员！");
            e.printStackTrace();
        }
        // byte[] result = new byte[headerLen + chunkLen];
        // for (int i = 0; i < headerLen; ++i) result[i] = header[i];
        // for (int i = 0; i < chunkLen; ++i) result[i + headerLen] = chunk[i];
        return chunkData;
    }

    // 接收客户端的下一个指令
    byte[] receiveCommand () {
        byte[] command = new byte[commandLen];
        try {
            socket = ssocket.accept();
            int readLen;
            InputStream instream = socket.getInputStream();
            readLen = instream.read(command);
            if (readLen < commandLen) {
                logger.error("socket读取command错误，command长度不足！");
            }
            logger.trace("读取了" + readLen + "Bytes长度的命令。");
            logger.trace("本次读取的命令为：" + new String(command, "UTF-8"));
            instream.close();
        } catch (IOException e) {
            logger.error("socket连接错误，请检查网络连接，并将此错误报告系统管理员！");
            e.printStackTrace();
        }
        return command;
    }

    void sendData (byte[] data) {
        try {
            socket = ssocket.accept();
            socket.setSendBufferSize(data.length);
            OutputStream outstream = socket.getOutputStream();
            outstream.write(data);
            outstream.flush();
            logger.trace("NameServer发送了" + data.length + "字节数据。");
            outstream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("NameServer发送数据出错！");
            e.printStackTrace();
        }
    }
}
