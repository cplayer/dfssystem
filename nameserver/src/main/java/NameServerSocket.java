/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;

class NameServerSocket {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");
    private ServerSocket ssocket;
    private Socket socket;
    private int headerLen = 64;
    private int chunkLen = 2 * 1024 * 1024;
    private int ipLen = 2 * 1024;
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
        byte[] chunkData = new byte[chunkLen + headerLen];
        try {
            socket = ssocket.accept();
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
            socket.close();
        } catch (IOException e) {
            logger.error("socket连接错误，请检查网络连接，并将此错误报告系统管理员！");
            e.printStackTrace();
        }
        return command;
    }

    // 此函数接收数据长度的必须是ipLen的倍数
    byte[] receive (int len) {
        byte[] result = new byte[len];
        try {
            socket = ssocket.accept();
            int readLen = 0;
            InputStream instream = socket.getInputStream();
            for (int i = 0; i < len; i += ipLen) {
                readLen += instream.read(result, i, ipLen);
            }
            logger.trace("读取了" + readLen + "Bytes长度的数据。");
            instream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("NameServer接收" + len + "MB时数据出错！");
            e.printStackTrace();
        }
        return result;
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
            // socket.shutdownOutput();
            socket.close();
        } catch (IOException e) {
            logger.error("NameServer发送数据出错！");
            e.printStackTrace();
        }
    }

    void sendData (byte[] data, boolean seperate) {
        if (seperate) {
            try {
                socket = ssocket.accept();
                socket.setSendBufferSize(data.length);
                OutputStream outstream = socket.getOutputStream();
                outstream.write(data);
                outstream.flush();
                for (int i = headerLen; i < headerLen + chunkLen; i += ipLen) {
                    outstream.write(data, i, ipLen);
                }
                outstream.flush();
                logger.trace("NameServer发送了" + data.length + "字节数据。");
                outstream.close();
                // socket.shutdownOutput();
                socket.close();
            } catch (IOException e) {
                logger.error("NameServer发送数据出错！");
                e.printStackTrace();
            }
        }
    }

    void sendData (byte[] data, InetAddress address, int port) {
        try {
            Socket socket = new Socket(address, port);
            OutputStream outstream = socket.getOutputStream();
            outstream.write(data, 0, headerLen);
            outstream.flush();
            for (int i = headerLen; i < headerLen + chunkLen; i += ipLen) {
                outstream.write(data, i, ipLen);
            }
            outstream.flush();
            outstream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("NameServer向DataServer发送chunk数据出错！");
            e.printStackTrace();
        }
    }

    byte[] receive (int len, InetAddress address, int port) {
        byte[] result = new byte[len];
        try {
            Socket socket = new Socket(address, port);
            InputStream instream = socket.getInputStream();
            int readLen;
            readLen = instream.read(result, 0, len);
            logger.trace(String.format("NameServer接收了%dBytes数据。", readLen));
            instream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("NameServer向DataServer接收数据出错！");
            e.printStackTrace();
        }
        return result;
    }

    DataServerInfo register () {
        DataServerInfo result = new DataServerInfo();
        try {
            socket = ssocket.accept();
            InputStream instream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            // 接收的信息应该是"register"
            byte[] data = new byte[8];
            int readLen;
            result.address = socket.getInetAddress();
            result.port = socket.getPort();
            readLen = instream.read(data);
            if (readLen < 8) {
                logger.warn("dataServer注册信息小于指定长度，不为\"Register!");
            }
            byte[] tableSize = new byte[8];
            readLen = instream.read(tableSize);
            if (readLen < 8) {
                logger.warn("dataServer负载信息小于指定长度!");
            }
            result.load = bytesToInt(tableSize);
            instream.close();
            outputStream.write("Success!".getBytes());
            outputStream.flush();
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("dataServer注册错误！");
            e.printStackTrace();
            return null;
        }
        return result;
    }

    private int bytesToInt(byte[] value) {
        int result = 0;
        for (int i = 0; i < value.length; ++i) {
            result = result << 8;
            result = result | (value[i] & 0xff);
        }
        return result;
    }
}
