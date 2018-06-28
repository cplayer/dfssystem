/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

 class DataServerSocket {
    private static final Logger logger = LogManager.getLogger("dataServerLogger");
    private ServerSocket serverSocket;
    private int port = 0;
    private int commandLen = 8;
    private int headerLen = 64;
    private int chunkLen = 2 * 1024 * 1024;
    private int ipLen = 2 * 1024;

    DataServerSocket (int port) {
        try {
            this.port = port;
            serverSocket = new ServerSocket(this.port);
        } catch (IOException e) {
            logger.error("DataServer发生socket通信错误！");
        }
    }

    public String receiveCommand () {
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

    public void sendChunk (byte[] sendData, int headerlen, int chunklen) {
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

    public void send (byte[] sendData) {
        try {
            Socket socket = serverSocket.accept();
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData);
            outputStream.flush();
            logger.trace("dataServer发送了" + sendData.length + "Bytes数据。");
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("dataserver发送数据错误！");
            e.printStackTrace();
        }
    }

    public void sendCommand (byte[] sendData) {
        try {
            Socket socket = serverSocket.accept();
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData);
            outputStream.flush();
            logger.trace("dataServer发送了" + sendData.length + "Bytes数据。");
            outputStream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("dataserver发送指令错误！");
            e.printStackTrace();
        }
    }

    public void sendCommand (byte[] sendData, InetAddress address, int port) {
        try {
            Socket socket = new Socket(address, port);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData);
            outputStream.flush();
            socket.close();
        } catch (IOException e) {
            logger.error("DataServer连接NameServer错误！");
            e.printStackTrace();
        }
    }

    public void sendCommand (byte[] sendData, String address, int port) {
        try {
            Socket socket = new Socket(address, port);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData);
            outputStream.flush();
            socket.close();
        } catch (IOException e) {
            logger.error("DataServer连接NameServer错误！");
            e.printStackTrace();
        }
    }

    public byte[] receiveChunk () {
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

    public byte[] receiveHeader() {
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

    public byte[] receive (int len) {
        byte[] receiveData = new byte[len];
        try {
            Socket socket = serverSocket.accept();
            int readLen;
            InputStream instream = socket.getInputStream();
            readLen = instream.read(receiveData);
            if (readLen < len) {
                logger.warn(String.format("socket读取数据错误，要求%dBytes数据，收到%dBytes数据。", len, readLen));
            }
            logger.trace(String.format("读取了%dBytes长度的数据。", readLen));
            instream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("socket连接错误，请检查网络连接，并将此错误报告系统管理员！");
            e.printStackTrace();
        }
        return receiveData;
    }
 }