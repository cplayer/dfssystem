/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

public class Convert {
    public static void copyTo (byte[] target, int begin, int end, byte[] source) {
        for (int i = begin; i < end; ++i) { target[i] = source[i - begin]; }
    }

    public static int byteToInt (byte[] arr, int start, int end) {
        int ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret << 8;
            ret |= (arr[i] & 0xff);
        }
        return ret;
    }

    public static long byteToLong (byte[] arr, int start, int end) {
        long ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret << 8;
            ret |= (arr[i] & 0xff);
        }
        return ret;
    }

    // int转bytes数组
    public static byte[] intToBytes (int value) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; --i) {
            result[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
        return result;
    }
 }