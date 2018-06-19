/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import java.io.OutputStream;
import java.net.Socket;

public class ClientProcessor {
    private int portServer = 36000;
    private String addressServer = "127.0.0.1";
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
}
