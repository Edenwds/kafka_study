package util;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 16:59 2019/10/14
 */
public class StringUtils {


    public static boolean isEmpty(String str) {
        return  str == null || "".equals(str);
    }

    public static boolean isInteger(String str) {
        try {
            Integer.valueOf(str);
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
