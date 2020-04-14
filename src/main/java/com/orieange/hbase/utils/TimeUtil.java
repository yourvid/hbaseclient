package com.orieange.hbase.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    public static int getTimeDifDay(Date begin, Date end) {
        int day = (int) ((end.getTime() - begin.getTime()) / (1000 * 3600 * 24));
        return day;
    }

    public static int getTimeDifHour(Date begin, Date end) {
        int day = (int) ((end.getTime() - begin.getTime()) / (1000 * 3600));
        return day;
    }

    public static long getTimeDifMils(Date begin, Date end) {
        return ((end.getTime() - begin.getTime()) / 1000 );
    }

    public static Date getAfterDay(Date begin, Long millis) {
        Long afterMillis = begin.getTime() + millis;
        Date afterDate = new Date(afterMillis);
        return afterDate;
    }

    public static Date getDayAfter(Date date, Integer day) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String a = sdf.format(date);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DAY_OF_MONTH, day);
        Date nextDay = c.getTime();
        String b = sdf.format(nextDay);
        return nextDay;
    }

    public static String getDayAfterStr(Date date, Integer day) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DAY_OF_MONTH, day);
        Date nextDay = c.getTime();
        return sdf.format(nextDay);
    }

    public static Date getDayHour(Date date, Integer hour) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String a = sdf.format(date);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.HOUR, hour);
        Date nextDay = c.getTime();
        String b = sdf.format(nextDay);
        return nextDay;
    }

    public static String getAfterDayStr(Date begin, Long millis) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long afterMillis = begin.getTime() + millis;
        String afterDateStr = sdf.format(new Date(afterMillis));
        return afterDateStr;
    }

    public static String getTimeFormat(Date date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String str = sdf.format(date);
        return str;
    }

    public static String getTimeFormatDefault(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String str = sdf.format(date);
        return str;
    }

    public static Date getDateFromStringDefault(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(dateStr);
        return date;
    }

    public static Date getDateFromString(String dateStr,String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date date = sdf.parse(dateStr);
        return date;
    }

}
