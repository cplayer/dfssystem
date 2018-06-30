/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.File;
import java.net.Socket;
import java.util.Arrays;
import java.util.ArrayList;

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
    void upload (String path, String dfsPath) {
        try {
            File fileUpload = new File(path);
            FileInputStream stream = new FileInputStream(fileUpload);
            byte[] buffer = new byte[chunkLen + headerLen];
            int readLen;
            long fileLen = fileUpload.length();
            int fileId = 0;
            long totIndex, curIndex;
            logger.trace("待读取文件总长度：" + fileLen + " Bytes.");
            // 发送命令
            this.send("upload  ".getBytes());
            // 发送文件总长度
            this.send(longToBytes(fileLen));
            // 发送文件路径
            this.send(dfsPath.getBytes());
            // 发送文件名
            this.send(fileUpload.getName().getBytes());
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
                System.arraycopy("upload..".getBytes(), 0, buffer, 0, 8);
                // 第二部分
                byte[] tmpBytes = longToBytes(readLen);
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
                sendChunk(buffer, headerLen, chunkLen);
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
        System.out.println("上传成功！");
    }

    void list (String path) {
        // 进入列举模式
        this.send("list    ".getBytes());
        if (path == null) { path = "/"; }
        this.send(path.getBytes());
        try {
            String status = new String(this.receive(8), "UTF-8");
            if (status.contains("Accepted")) {
                int resultNumber = bytesToInt(this.receive(4));
                ArrayList<String> fileNames = new ArrayList<>();
                ArrayList<String> filePaths = new ArrayList<>();
                ArrayList<Long> fileLens = new ArrayList<>();
                ArrayList<String> fileIDs = new ArrayList<>();
                fileLens.clear(); fileNames.clear(); filePaths.clear(); fileIDs.clear();
                for (int i = 0; i < resultNumber; ++i) {
                    String fileName = new String(this.receive(255), "UTF-8").trim();
                    String filePath = new String(this.receive(255), "UTF-8").trim();
                    long fileLen = bytesToLong(this.receive(8));
                    String fileID = new String(this.receive(255), "UTF-8").trim();
                    fileNames.add(fileName);
                    filePaths.add(filePath);
                    fileLens.add(fileLen);
                    fileIDs.add(fileID);
                }
                for (int i = 0; i < 120; ++i) {
                    System.out.print('-');
                }
                System.out.println();
                for (int i = 0; i < resultNumber; ++i) {
                    System.out.println(String.format("序号：%d", i + 1));
                    System.out.println(String.format("文件名：%s", fileNames.get(i)));
                    System.out.println(String.format("文件路径：%s", filePaths.get(i)));
                    System.out.println(String.format("文件大小：%d字节", fileLens.get(i)));
                    System.out.println(String.format("文件id：%s", fileIDs.get(i)));
                    for (int j = 0; j < 120; ++j) {
                        System.out.print('-');
                    }
                }
                System.out.println();
            } else if (status.contains("Denied")) {
                logger.error("请输入正确的路径！");
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("NameServer发送的结果编码错误！");
            e.printStackTrace();
        }
    }

    void getOffsetById (long offset, String id) {
        String path = getPath(id).trim();
        getOffsetByPath(offset, path);
    }

    void getOffsetByPath (long offset, String path) {
        this.send("offset  ".getBytes());
        int chunkNum = (int)(offset / (2 * 1024 * 1024)) + 1;
        this.send(intToBytes(chunkNum));
        this.send(path.getBytes());
        this.send(longToBytes(offset));
        try {
            String status = new String(this.receive(8), "UTF-8").trim();
            if (status.contains("Denied")) {
                System.out.println("输入的文件路径不正确！");
            } else {
                byte[] result = new byte[1];
                result = this.receive(1);
                System.out.println("目标字节为：0b" + Integer.toBinaryString(result[0] & 0xff));
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("client给出的下载文件路径不正确！");
            e.printStackTrace();
        }
    }

    void getMD5byID (int chunkNum, String id) {
        String path = getPath(id).trim();
        getMD5byPath(chunkNum, path);
    }

    void getMD5byPath (int chunkNum, String path) {
        this.send("md5     ".getBytes());
        this.send(path.getBytes());
        this.send(intToBytes(chunkNum));
        try {
            String status = new String(this.receive(255), "UTF-8").trim();
            int totalMd5 = bytesToInt(this.receive(4));
            ArrayList<String> md5 = new ArrayList<>();
            md5.clear();
            for (int i = 0; i < totalMd5; ++i) {
                String md5value = new String(this.receive(255)).trim();
                md5.add(md5value);
            }
            System.out.println(status);
            String goodResult = "Accepted";
            if (status.equals(goodResult)) {
                System.out.println("相同！");
            } else {
                System.out.println("不同！");
            }
            int i = 0;
            for (String element : md5) {
                System.out.println("第" + i + "个md5值为：" + element);
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("client给出的下载文件路径不正确！");
            e.printStackTrace();
        }
    }

    void checkExistId (String id) {
        String path = getPath(id);
        checkExistPath(path);
    }

    void checkExistPath (String path) {
        if (path == null) {
            System.out.println("文件不存在！");
            return;
        }
        logger.trace("开始发送文件路径...");
        this.send("checkFth".getBytes());
        this.send(path.getBytes());
        String result = new String(this.receive(255)).trim();
        String allowedResult = "Accepted";
        if (result.contains(allowedResult)) {
            System.out.println("文件存在！");
        } else {
            System.out.println("文件不存在！");
        }
    }

    private String getPath (String id) {
        this.send("checkID ".getBytes());
        int fileId = Integer.parseInt(id);
        this.send(intToBytes(fileId));
        String result = new String(this.receive(255));
        String notAllowedResult = "File Not Found";
        if (result.contains(notAllowedResult)) { result = null; }
        return result;
    }

    void getFileById (String id) {
        logger.trace("准备向NameServer请求文件路径...");
        String path = getPath(id);
        if (path != null) {
            logger.trace(String.format("获取的文件路径为：%s", path));
            getFileByPath(path);
        } else {
            logger.error("请输入正确的文件ID！");
        }
        // System.out.println("根据id获取文件成功！");
    }

    void getFileByPath (String path) {
        try {
            logger.trace("开始获取文件...");
            this.send("download".getBytes());
            this.send(path.getBytes());
            String fileName = new String(this.receive(255)).trim();
            logger.trace(String.format("获取到的文件名为：%s", fileName));
            File fileOutput = new File(fileName);
            FileOutputStream outstream = new FileOutputStream(fileOutput);
            int fileChunkNum = bytesToInt(this.receive(4));
            for (int i = 1; i <= fileChunkNum; ++i) {
                logger.trace(String.format("目前获取第%d个chunk，共有%d个chunk", i, fileChunkNum));
                byte[] chunkData = this.receiveChunk();
                int chunkLen = bytesToInt(chunkData, 8, 24);
                logger.trace(String.format("写入了%dBytes数据。", chunkLen));
                outstream.write(chunkData, 64, chunkLen);
            }
            outstream.flush();
            outstream.close();
            System.out.println("下载完成！文件名为：" + fileName);
        } catch (FileNotFoundException e) {
            logger.error("文件未找到！");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("client读写文件错误！");
            e.printStackTrace();
        }
    }

    // 向dataserver传送数据
    private int portServer = 36000;
    private String addressServer = "127.0.0.1";
    private void sendChunk (byte[] sendData, int headerlen, int chunklen) {
        try {
            socket = new Socket(this.addressServer, this.portServer);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData, 0, headerlen);
            outputStream.flush();
            for (int i = 0; i < chunklen; i += ipLen) {
                outputStream.write(sendData, headerlen + i, ipLen);
                outputStream.flush();
            }
            logger.trace("Client发送了" + chunklen + "Bytes数据。");
            outputStream.close();
            socket.close();
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
        } catch (IOException e) {
            logger.error("发送数据错误！");
            e.printStackTrace();
        }
    }

    private byte[] receive (int length) {
        byte[] result = new byte[length];
        try {
            socket = new Socket(this.addressServer, this.portServer);
            InputStream instream = socket.getInputStream();
            int readLen;
            readLen = instream.read(result);
            if (readLen < length) {
                logger.warn(String.format("client接收长度与给定长度不符合！接收长度为：%d，给定长度为：%d", readLen, length));
            }
            socket.close();
        } catch (IOException e) {
            logger.error("socket连接错误！");
            e.printStackTrace();
        }
        return result;
    }

    private byte[] receiveChunk () {
        byte[] chunkData = new byte[chunkLen + headerLen];
        try {
            socket = new Socket(this.addressServer, this.portServer);
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

    private long bytesToLong (byte[] value) {
        long result = 0;
        for (int i = 0; i < value.length; ++i) {
            result = result << 8;
            result = result | (value[i] & 0xff);
        }
        return result;
    }

    // byte数组转long
    private static int bytesToInt (byte[] arr, int start, int end) {
        int ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret << 8;
            ret |= (arr[i] & 0xFF);
        }
        return ret;
    }
}
