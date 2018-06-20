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
        DataServerProcessor processor = new DataServerProcessor();
        // 启动之后不停的接收消息
        try {
            while (true) {
                processor.listen();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
