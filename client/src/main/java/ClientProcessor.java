/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.net.Socket;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class ClientProcessor {
    private static final Logger logger = LogManager.getLogger("clientLogger");
    ClientProcessor () {
        logger.info("处理过程启动！");
    }

    /** package private */
    void upload (String path) {
        try {
            File fileUpload = new File(path);
            FileInputStream stream = new FileInputStream(fileUpload);
            int bufferLen = 2 * 1024 * 1024;
            byte[] buffer = new byte[bufferLen];
            int readLen;
            long fileLen = fileUpload.length();
            int fileId = 0;
            logger.trace("待读取文件总长度：" + fileLen + " Bytes.");

            while (true) {
                // 读取给定buffer长度的数据
                readLen = stream.read(buffer);
                logger.trace("读取了" + readLen + "长度的数据，从" + path + "中.");
                if (readLen < bufferLen) { break; }
            }
            logger.trace("待上传文件的fileId为：" + fileId + ".");
            stream.close();
        } catch (FileNotFoundException e) {
            logger.error("请检查上传文件路径是否正确！");
        } catch (IOException e) {
            logger.error("读取文件出现错误，请检查是否有足够的权限进行读取！");
        } catch (Exception e) {
            logger.error("出现未知错误！");
            e.printStackTrace();
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
    public void send (byte[] sendData) {
        try {
            Socket socket = new Socket(this.addressServer, this.portServer);
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sendData);
            outputStream.close();
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
