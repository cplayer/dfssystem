// 用于服务sqlServerList的私有类
class SqlServerListNode {
    int fileID;
    String fileName;
    String filePath;
    SqlServerListNode () {
        fileID = 0; 
        fileName = null; 
        filePath = null;
    }

    SqlServerListNode (int fileID, String fileName, String filePath) {
        this.fileID = fileID;
        this.fileName = fileName;
        this.filePath = filePath;
    }
}