package com.usertrack.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * create by jeremy hu 20190721
 */
public class DateUtils {
    public static final int dayOfMillis = 86400000;
    public static final String DATE_FORMAT="yyyy-MM-dd";
    public static final String TIME_FORMAT="yyyy-MM-dd HH:mm:ss";

    /**
     * 获取当前时间格式
     * @return "yyyy-MM-dd"
     */
    public static String getTodayDate(){
        return new SimpleDateFormat(DATE_FORMAT).format(new Date());
    }

    /**
     *
     * @param time
     * @return   返回当前时间格式 "yyyy-MM-dd HH:mm:ss"
     */
    public static String parseLong2String(long time){
        return parseLong2String(time,TIME_FORMAT);
    }

    public static String parseLong2String(long time,String pattern){
        return parseLong2String(time,new SimpleDateFormat(pattern));
    }

    public static String parseLong2String(long time,SimpleDateFormat sdf){
        Calendar cld = Calendar.getInstance();
        cld.setTimeInMillis(time);
        return sdf.format(cld.getTime());
    }
    /**
     * @param date
     * @return time
     */
    public static long parseDate2Long(String date){
        return parseString2Long(date,TIME_FORMAT);
    }

    public static long parseString2Long(String date,String dateFormat){
        return parseString2Long(date,new SimpleDateFormat(dateFormat));
    }
    public static long parseString2Long(String date,SimpleDateFormat sdf){
        try {
            return sdf.parse(date).getTime();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取一个随机的当天的毫秒级时间戳值，根据给定的随机对象
     *
     * @param random
     * @return
     */
    public static long getRandomTodayTimeOfMillis(Random random) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        if (random.nextDouble() <= 0.7) {
            // [0-21] => 70%
            int millis = dayOfMillis / 8 * 7;  //75600000
            cal.add(Calendar.MILLISECOND, 1 + random.nextInt(millis));
        } else {
            // [1-23] => 30%
            int millis = dayOfMillis / 24;
            cal.add(Calendar.MILLISECOND, millis + random.nextInt(millis * 23));
        }
        return cal.getTimeInMillis();

    }

    /**
     * 获取时间类型(类型)
     */
    public static enum DateTypeEnum{
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND
    }
    public static int getSpecificDateValueOfDateTypeEnum(long mills,DateTypeEnum type){
        Calendar inst = Calendar.getInstance();
        inst.setTimeInMillis(mills);
        switch (type){
            case YEAR:
                return inst.get(Calendar.YEAR);
            case MONTH:
                return inst.get(Calendar.MONTH)+1;
            case DAY:
                return inst.get(Calendar.DAY_OF_MONTH);
            case HOUR:
                return inst.get(Calendar.HOUR_OF_DAY);
            case MINUTE:
                return inst.get(Calendar.MINUTE);
            case SECOND:
                return inst.get(Calendar.SECOND);
            case MILLISECOND:
                return inst.get(Calendar.MILLISECOND);
        }
       throw new IllegalArgumentException("参数异常<这个异常不应该产生的....>");
    }
}
