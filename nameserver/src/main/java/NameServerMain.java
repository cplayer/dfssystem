/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class NameServerMain {
    private static final Logger logger = LogManager.getLogger("nameServerLogger");

    public static void main (String[] args) {
        logger.trace("NameServer启动。");
        // 创建处理类进入处理循环
        NameServerProcessor processor = new NameServerProcessor();
        processor.run();
    }
}
