/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.OutputStream;
import java.net.Socket;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class ClientProcessor {
    private int portServer = 36000;
    private String addressServer = "127.0.0.1";
    private static final Logger logger = LogManager.getLogger(ClientProcessor.class);
    public void send () {
        try {
            Socket socket = new Socket(this.addressServer, this.portServer);
            OutputStream outputStream = socket.getOutputStream();
            String message = "Test Send from Client!";
            outputStream.write(message.getBytes("UTF-8"));
            outputStream.close();
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    ClientProcessor () {
        logger.info("处理过程启动！");
    }

    // package private
    void upload (String path) {
        System.out.println("上传成功！");
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
}
