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
    
    // byte数组转int
    public static int byteToInt(byte[] arr, int start, int end) {
        int ret = 0;
        for (int i = start; i < end; ++i) {
            ret = ret * 256 + arr[i];
        }
        return ret;
    }
}
