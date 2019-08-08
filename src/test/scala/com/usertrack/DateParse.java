package com.usertrack;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateParse {
    public static String DATE_FORMAT="yyyy-MM-dd HH:mm:ss";
    public static String getDate(Long ms){
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        Date dt = new Date(ms);
        String format = sdf.format(dt);
        return format;
    }

   //对日期格式进行转换
    public static Long getCureentTime(String time){
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        try {
            Date parse = sdf.parse(time);
            long t1 = parse.getTime();
            return t1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
