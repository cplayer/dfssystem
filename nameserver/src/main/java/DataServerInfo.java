import java.net.InetAddress;

// 用于IP注册的类
class DataServerInfo {
    InetAddress address;
    int port;
    int load;
    DataServerInfo () {
        address = null;
        port = 0;
        load = 0;
    }

    DataServerInfo (InetAddress address, int port) {
        this.address = address;
        this.port = port;
        load = 0;
    }
}