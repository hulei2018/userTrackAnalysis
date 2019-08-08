package com.usertrack.util;

import java.text.NumberFormat;

public class NumberUtils {
    /**
     * 将数字不以科学计算法的形式显示
     */
    public static String formatDoubleOfNotUseGrouping(Double totalSessionSum){
        Double value=Double.valueOf(totalSessionSum);
        NumberFormat inst = NumberFormat.getInstance();
        inst.setGroupingUsed(false);
        return inst.format(value);
    }
}
