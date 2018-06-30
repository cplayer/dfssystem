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
            String[] commandList = {"upload", "list", "download", "offset", "check", "md5"};
            String command = args[0];
            int index;
            // 判断是哪条命令
            for (index = 0; index < commandList.length; ++index) {
                if (command.equals(commandList[index])) { break; }
            }
            switch (index) {
                case 0:
                    // 上传
                    if (args.length < 3) {
                        logger.error("上传文件需要给定一个合法的路径和一个dfs中的路径！");
                    } else {
                        String path = args[1];
                        String dfsPath = args[2];
                        processor.upload(path, dfsPath);
                    }
                    break;
                case 1:
                    // 列举所有对应文件信息
                    if (args.length == 2) {
                        processor.list(args[1]);
                    } else if (args.length == 1) {
                        processor.list(null);
                    } else {
                        logger.error("请输入正确的列举路径！");
                    }
                    break;
                case 2:
                    // 下载对应的文件
                    if (args.length < 3) {
                        logger.error("下载对应的参数不正确，请检查标志和参数是否正常！");
                    } else {
                        String flag = args[1];
                        String target = args[2];
                        String flag_ID = "-id";
                        String flag_PATH = "-path";
                        if (flag_ID.equals(flag)) {
                            processor.getFileById(target);
                        } else if (flag_PATH.equals(flag)) {
                            processor.getFileByPath(target);
                        } else {
                            logger.error("系统支持根据文件或者id获取，请检查是否选择两者之一！");
                        }
                    }
                    break;
                case 3:
                    // 根据offset读取数据
                    // command + flag + content + offset
                    if (args.length != 4) {
                        logger.error("请输入正确的参数格式！");
                    } else {
                        String flag = args[1];
                        String content = args[2];
                        long offset = Long.parseLong(args[3]);
                        if (flag.equals("-id")) {
                            processor.getOffsetById(offset, content);
                        } else if (flag.equals("-path")) {
                            processor.getOffsetByPath(offset, content);
                        } else {
                            logger.error("请检查输入的标识是否正确！");
                        }
                    }
                    break;
                case 4:
                    // check文件是否在dfs里
                    // command + flag + content
                    if (args.length != 3) {
                        logger.error("请输入正确的参数格式！");
                    } else {
                        String flag = args[1];
                        String content = args[2];
                        if (flag.equals("-id")) {
                            processor.checkExistId(content);
                        } else if (flag.equals("-path")) {
                            processor.checkExistPath(content);
                        } else {
                            logger.error("请检查输入的标识是否正确！");
                        }
                    }
                    break;
                case 5:
                    // 检查不同chunk的md5值
                    // command + flag + content + chunkNumber
                    if (args.length != 4) {
                        logger.error("请输入正确的参数格式！");
                    } else {
                        String flag = args[1];
                        String content = args[2];
                        int chunkNum = Integer.parseInt(args[3]);
                        if (flag.equals("-id")) {
                            processor.getMD5byID(chunkNum, content);
                        } else if (flag.equals("-path")) {
                            processor.getMD5byPath(chunkNum, content);
                        } else {
                            logger.error("请检查输入的标识是否正确！");
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