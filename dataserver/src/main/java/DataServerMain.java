/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class DataServerMain {

    private static final Logger logger = LogManager.getLogger("dataServerLogger");

    public static void main (String[] args) {
        logger.info("DataServer启动。");
        if (args.length < 1) {
            logger.error("请输入正确的启动端口号！");
            return;
        }
        // dataserver启动之后，通过死循环进行监听NameServer发来的指令。
        DataServerProcessor processor = new DataServerProcessor(Integer.parseInt(args[0]));
        processor.listen();
    }
}
