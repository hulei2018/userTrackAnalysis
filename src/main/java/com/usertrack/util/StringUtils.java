package com.usertrack.util;

public class StringUtils {
    /**
     * 对字符串是否为空进行判断
     * @param str
     */
    public static boolean isEmpty(String str){
        return str==null||"".equals(str.trim());
    }

    //判断字符串不为空
    public static boolean isNotEmpty(String str){
        return !isEmpty(str);
    }

}
