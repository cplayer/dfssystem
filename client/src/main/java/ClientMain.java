/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ClientMain {

    private static final Logger logger = LogManager.getLogger("clientLogger");

    public static void main(String[] args) {
        if (args.length > 0) {
            // 初始化处理类
            ClientProcessor processor = new ClientProcessor();
            // 判断是否有参数传入
            String[] commandList = {"upload", "list", "download"};
            String command = args[0];
            int index;
            // 判断是哪条命令
            for (index = 0; index < commandList.length; ++index) {
                if (command.equals(commandList[index])) { break; }
            }
            switch (index) {
                case 0:
                    // 上传
                    if (args.length < 2) {
                        logger.error("上传文件需要给定一个合法的路径！");
                    } else {
                        String path = args[1];
                        processor.upload(path);
                    }
                    break;
                case 1:
                    // 列举所有对应文件信息
                    processor.list();
                    break;
                case 2:
                    // 下载对应的文件
                    if (args.length < 3) {
                        logger.error("下载对应的参数不正确，请检查标志和参数是否正常！");
                    } else {
                        String flag = args[1];
                        String target = args[2];
                        String flag_ID = "-id";
                        String flag_NAME = "-name";
                        if (flag_ID.equals(flag)) {
                            processor.getFileById(target);
                        } else if (flag_NAME.equals(flag)) {
                            processor.getFileByName(target);
                        } else {
                            logger.error("系统支持根据文件或者id获取，请检查是否选择两者之一！");
                        }
                    }
                    break;
                default:
                    logger.error("请检查输入参数是否正确或者命令是否被支持！");
                    break;
            }
        } else {
            logger.error("无命令，请输入正确的命令以执行！");
        }

    }
}