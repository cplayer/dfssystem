/**
 * @author cplayer on 2018/6/16.
 * @version 1.0
 */

class Convert {
    // int转bytes数组
    public static byte[] intToBytes (int value) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; --i) {
            result[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
        return result;
    }

    // int放入bytes数组
    public static void intIntoBytes (int value, byte[] target, int start, int end) {
        for (int i = end - 1; i >= start; --i) {
            target[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
    }

    // long放入bytes数组
    public static void longIntoBytes (long value, byte[] target, int start, int end) {
        for (int i = end - 1; i >= start; --i) {
            target[i] = (byte)(value & 0xFF);
            value = value >> 8;
        }
    }
    
    // byte数组转int
    public static int byteToInt (byte[] arr, int start, int end) {
        int ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret << 8;
            ret |= (arr[i] & 0xFF);
        }
        return ret;
    }

    // byte数组转long
    public static long byteToLong (byte[] arr, int start, int end) {
        long ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret << 8;
            ret |= (arr[i] & 0xFF);
        }
        return ret;
    }
}
