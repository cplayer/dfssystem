import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

public class DataServerProcessor {
    public void listen () {
        try {
            ServerSocket serverSocket = new ServerSocket(36000);
            Socket socket = serverSocket.accept();
            InputStream inputStream = socket.getInputStream();
            byte[] bytes = new byte[1024];
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
            e.printStackTrace();
        }
    }
}
