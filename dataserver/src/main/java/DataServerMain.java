/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

public class DataServerMain {
    public static void main(String[] args) {
        DataServerProcessor processor = new DataServerProcessor();
        processor.listen();
    }
}
