/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.File;
import java.net.Socket;
import java.util.Arrays;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class ClientProcessor {
    private static final Logger logger = LogManager.getLogger("clientLogger");
    private Socket socket;
    private int chunkLen = 2 * 1024 * 1024;
    private int headerLen = 64;
    private int ipLen = 2 * 1024;
    ClientProcessor () {
        logger.info("处理过程启动！");
    }

    /** package private */
    void upload (String path) {
        try {
            File fileUpload = new File(path);
            FileInputStream stream = new FileInputStream(fileUpload);
            byte[] buffer = new byte[chunkLen + headerLen];
            int readLen;
            long fileLen = fileUpload.length();
            int fileId = 0;
            long totIndex, curIndex;
            logger.trace("待读取文件总长度：" + fileLen + " Bytes.");
            this.send("upload  ".getBytes());
            fileId = bytesToInt(this.receive(4));
            logger.trace("待上传文件的fileId为：" + fileId + ".");
            totIndex = fileUpload.length() / (chunkLen);
            if (fileUpload.length() % chunkLen != 0) { totIndex++; }
            curIndex = 1;
            while (true) {
                // 读取给定buffer长度的数据
                Arrays.fill(buffer, (byte)0);
                readLen = stream.read(buffer, headerLen, chunkLen);
                logger.trace("读取了" + readLen + "长度的数据，从" + path + "中.");
                Arrays.fill(buffer, 0, 64, (byte)0);
                // 填充数据头
                // 第一部分
                byte[] tmpBytes = "upload..".getBytes();
                for (int i = 0; i < 8; ++i) { buffer[i] = tmpBytes[i]; }
                // 第二部分
                tmpBytes = longToBytes(fileLen);
                for (int i = 0; i < tmpBytes.length; ++i) {
                    buffer[23 - i] = tmpBytes[tmpBytes.length - 1 - i];
                }
                // 第三部分
                tmpBytes = longToBytes(totIndex);
                for (int i = 0; i < tmpBytes.length; ++i) {
                    buffer[39 - i] = tmpBytes[tmpBytes.length - 1 - i];
                }
                // 第四部分
                tmpBytes = longToBytes(curIndex);
                for (int i = 0; i < tmpBytes.length; ++i) {
                    buffer[55 - i] = tmpBytes[tmpBytes.length - 1 - i];
                }
                // 第五部分
                tmpBytes = intToBytes(fileId);
                for (int i = 0; i < tmpBytes.length; ++i) {
                    buffer[63 - i] = tmpBytes[tmpBytes.length - 1 - i];
                }
                // send(buffer, 0, headerLen);
                // send(buffer, headerLen, readLen);
                sendChunk(buffer, headerLen, readLen);
                curIndex++;
                if (readLen < chunkLen) { break; }
            }
            stream.close();
        } catch (FileNotFoundException e) {
            logger.error("请检查上传文件路径是否正确！");
            return;
        } catch (IOException e) {
            logger.error("读取文件出现错误，请检查是否有足够的权限进行读取！");
            return;
        } catch (Exception e) {
            logger.error("出现未知错误！");
            e.printStackTrace();
            return;
        }
        logger.error("上传成功！");
    }

    void list () {
        System.out.println("列举成功！");
    }

    void getFileById (String id) {
        System.out.println("根据id获取文件成功！");
    }

    void getFileByName (String name) {
        System.out.println("根据文件名获取文件成功！");
    }

    // 向dataserver传送数据
    private int portServer = 36000;
    private String addressServer = "127.0.0.1";
    private void sendChunk (byte[] sendData, int headerlen, int chunklen) {
        try {
            socket = new Socket(this.addressServer, this.portServer);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData, 0, headerLen);
            outputStream.flush();
            for (int i = 0; i < chunklen; i += ipLen) {
                outputStream.write(sendData, headerLen + i, Math.min(ipLen, chunklen - headerLen - i));
                outputStream.flush();
            }
            logger.trace("Client发送了" + chunklen + "Bytes数据。");
            outputStream.close();
            socket.close();
            // socket.shutdownOutput();
        } catch (IOException e) {
            logger.error("发送数据错误！");
            e.printStackTrace();
        }
    }

    private void send (byte[] sendData) {
        try {
            socket = new Socket(this.addressServer, this.portServer);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData);
            outputStream.flush();
            logger.trace("Client发送了" + sendData.length + "Bytes数据。");
            outputStream.close();
            socket.close();
            // socket.shutdownOutput();
        } catch (IOException e) {
            logger.error("发送数据错误！");
            e.printStackTrace();
        }
    }

    private byte[] receive (int length) {
        byte[] result = new byte[length];
        try {
            socket = new Socket(this.addressServer, this.portServer);
            // socket.setReceiveBufferSize(length);
            InputStream instream = socket.getInputStream();
            int readLen;
            readLen = instream.read(result);
            if (readLen < length) {
                logger.warn("client接收长度与给定长度不符合！");
            }
            socket.close();
        } catch (IOException e) {
            logger.error("socket连接错误！");
            e.printStackTrace();
        }
        return result;
    }

    private byte[] longToBytes (long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; --i) {
            result[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
        return result;
    }

    private byte[] intToBytes (int value) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; --i) {
            result[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
        return result;
    }

    private int bytesToInt (byte[] value) {
        int result = 0;
        for (int i = 0; i < value.length; ++i) {
            result = result << 8;
            result = result | (value[i] & 0xff);
        }
        return result;
    }
}
