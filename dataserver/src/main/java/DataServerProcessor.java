/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class DataServerProcessor {
    private static final Logger logger = LogManager.getLogger("dataServerLogger");

    // 监听函数
    void listen () {
        try {
            ServerSocket serverSocket = new ServerSocket(36000);
            Socket socket = serverSocket.accept();
            InputStream inputStream = socket.getInputStream();
            int bufferLen = 2 * 1024 * 1024 + 16 + 8;
            byte[] bytes = new byte[bufferLen];
            int len;
            StringBuilder sb = new StringBuilder();
            while ((len = inputStream.read(bytes)) != -1) {
                sb.append(new String(bytes, 0, len, "UTF-8"));
            }
            System.out.println("Message from client:" + sb);
            inputStream.close();
            socket.close();
            serverSocket.close();
        } catch (Exception e) {
            logger.error("监听出错！");
            e.printStackTrace();
        }
    }
}
