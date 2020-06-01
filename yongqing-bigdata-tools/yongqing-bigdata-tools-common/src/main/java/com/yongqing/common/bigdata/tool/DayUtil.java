package com.yongqing.common.bigdata.tool;

import lombok.extern.log4j.Log4j2;


import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Log4j2
public class DayUtil {

    //日期格式
    public static DateFormat dateFormat(){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
    //日期格式-日
    public static DateFormat dateFormatDay(){
        return new SimpleDateFormat("yyyy-MM-dd");
    }
    //日期格式-日短格式
    public static DateFormat dateFormatDayShort(){
        return new SimpleDateFormat("yyyyMMdd");
    }
    //日期格式带小时
    public static DateFormat dateFormatDayHour(){
        return new SimpleDateFormat("yyyyMMddHH");
    }
    //日期格式-月
    public static DateFormat dateFormatMonthShort(){
        return new SimpleDateFormat("yyyyMM");
    }
    //日期格式-年
    public static DateFormat dateFormatYearShort(){
        return new SimpleDateFormat("yyyy");
    }
    //日期格式-小时
    public static DateFormat dateFormatHour(){
        return new SimpleDateFormat("HH");
    }
    //日期格式-日-长格式
    public static DateFormat dateFormatLong(){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    private DayUtil(){}


    //Srting转date
    public static Date changeStringToDate(String dateStr) {
        Date date = new Date();
        try {
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr);
        } catch (ParseException e) {
            log.error(e.getMessage());
        }
        return date;

    }

    //date转String
    public static String changeDateToString(Date date) {
        String dateStr = "";
        try {
            dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        } catch (Exception e) {
            log.error("DayUtil causes Exception", e);
        }
        return dateStr;
    }

    //String转Timestamp
    public static Timestamp changeStringToTimestamp(String tsStr) {
        Timestamp ts = Timestamp.valueOf(tsStr);
        return ts;
    }

    //Timestamp转String
    public static String changeTimestampToString(Timestamp ts) {
        return ts.toString();
    }

    //Date转Timestamp
    public static Timestamp changeDateToTimestamp(Date date) {
        Date dateNew = null;
        Timestamp ts = null;
        try {
            dateNew = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
            ts = new Timestamp(dateNew.getTime());
        } catch (ParseException e) {
            log.error("DayUtil causes ParseException", e);
        }
        return ts;
    }

    //Timestamp转Date
    public static Date changeTimestampToDate(Timestamp ts) {
        return new Date(ts.getTime());

    }


    //获取某天的0点时间
    public static Date getMoningTimeOfDay(Date date) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.HOUR_OF_DAY, 0);
        day.set(Calendar.MINUTE, 0);
        day.set(Calendar.SECOND, 0);
        day.set(Calendar.MILLISECOND, 0);
        return day.getTime();
    }

    //获取某天的末点时间
    public static Date getEndTimeOfDay(Date date) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.HOUR_OF_DAY, 23);
        day.set(Calendar.MINUTE, 59);
        day.set(Calendar.SECOND, 59);
        day.set(Calendar.MILLISECOND, 999);
        return day.getTime();
    }

    //获取某小时开始时间
    public static Date getBeginTimeOfHour(Date date) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.MINUTE, 0);
        day.set(Calendar.SECOND, 0);
        day.set(Calendar.MILLISECOND, 0);
        return day.getTime();
    }

    //获取某小时的末点时间
    public static Date getEndTimeOfHour(Date date) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.MINUTE, 59);
        day.set(Calendar.SECOND, 59);
        day.set(Calendar.MILLISECOND, 999);
        return day.getTime();
    }


    /**
     * 小时+1
     *
     * @param date 日期
     * @return 时间
     */
    public static Date hourInc(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);   //设置当前日期
        c.add(Calendar.HOUR, 1); //日期分钟加1,Calendar.DATE(天),Calendar.HOUR(小时)
        return c.getTime(); //结果
    }

    /**
     * 日期+1
     *
     * @param date 日期
     * @return 时间
     */
    public static Date dayInc(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);   //设置当前日期
        c.add(Calendar.DATE, 1); //日期分钟加1,Calendar.DATE(天),Calendar.HOUR(小时)
        return c.getTime(); //结果
    }

    /**
     * 获取昨天日期
     * @param date 日期
     * @return 时间
     */
    public static Date getYesterDay(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);   //设置当前日期
        c.add(Calendar.DATE, -1);
        return c.getTime(); //结果
    }

    /**
     * 小时-1
     *
     * @param date 日期
     * @return 时间
     */
    public static Date hourDec(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);   //设置当前日期
        c.add(Calendar.HOUR, -1); //日期分钟加1,Calendar.DATE(天),Calendar.HOUR(小时)
        return c.getTime(); //结果
    }

    /**
     * 日是期类型格式转换
     *
     * @param srcFormat    源格式
     * @param targetFormat 目标格式
     * @param dateStr      要转换的日期字符串
     * @return 转换后字符串
     */
    public static String convert(DateFormat srcFormat, DateFormat targetFormat, String dateStr) {
        try {
            Date date = srcFormat.parse(dateStr);
            return targetFormat.format(date);
        } catch (ParseException e) {
            log.error("DayUtil causes ParseException", e);
        }
        return null;
    }

    /**
     * 获取某天的指定时间点时间类型(精确到秒)
     *
     * @param date      日期类型Date
     * @param hour      小时
     * @param minute    分钟
     * @param second    秒
     * @return
     */
    public static Date getCustomTimeOfDay(Date date, int hour, int minute, int second) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.HOUR_OF_DAY, hour);
        day.set(Calendar.MINUTE, minute);
        day.set(Calendar.SECOND, second);
        day.set(Calendar.MILLISECOND, 0);
        return day.getTime();
    }

    /**
     * 获取随机日期
     *
     * @param start 起始日期
     * @param end 结束日期  结束日期 > 起始日期
     * @return
     */
    public static long randomUnixTime(Date start, Date end){
        try {
            if(start.getTime() >= end.getTime()){
                return 0;
            }
            return random(start.getTime(), end.getTime()) / 1000;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return 0;
    }

    /**
     * 随机生成指定区间的值
     *
     * @param begin
     * @param end
     * @return
     */
    private static long random(long begin, long end){
        long rtn = begin + (long)(Math.random() * (end - begin));
        if(rtn == begin || rtn == end){
            return random(begin,end);
        }
        return rtn;
    }

    /**
     * 获取日期的unixTime
     *
     * @param date 时间
     * @return unixTime值
     */
    public static long getUnixTime(Date date){
        if(null == date){
            throw new NullPointerException("date is null");
        }
        return date.getTime() / 1000;
    }

    /**
     * 获取现在时间的前多少天的最小时间
     * interval 时间间隔
     * @return
     */
    public static String dayIntervalMin(int interval){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,1);
        calendar.set(Calendar.MILLISECOND,1);
        calendar.add(Calendar.DAY_OF_YEAR,-interval);
        Date date = calendar.getTime();
        return sdf.format(date);
    }

    /**
     * 获取现在时间的前一天的最大时间
     * @return
     */
    public static String yesterdayIntervalMax(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY,23);
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,59);
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        Date date = calendar.getTime();
        return sdf.format(date);
    }

    /**
     * 获取时间的前多少天的0点
     * interval 时间间隔
     * @return
     */
    public static Date dayIntervalMin(Date date, int interval) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.HOUR_OF_DAY,0);
        day.set(Calendar.MINUTE,0);
        day.set(Calendar.SECOND,0);
        day.add(Calendar.DATE,-interval);
        return day.getTime();
    }

    /**
     * 获取时间的前多少天的23:59:59点
     * interval 时间间隔
     * @return
     */
    public static Date dayIntervalMax(Date date, int interval) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
        day.set(Calendar.HOUR_OF_DAY,23);
        day.set(Calendar.MINUTE,59);
        day.set(Calendar.SECOND,59);
        day.add(Calendar.DATE,-interval);
        return day.getTime();
    }

    /**
     * 获取现在时间的前多少天
     * interval 时间间隔
     * @return
     */
    public static String dayIntervalNow(int interval){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal=Calendar.getInstance();
        cal.add(Calendar.DATE,-interval);
        Date time=cal.getTime();
        return sdf.format(time);
    }

    /**
     *
     * @param startDate
     * @return
     */
    public static String lastMonth(String startDate){
        SimpleDateFormat sformat = new SimpleDateFormat("yyyy-MM");
        //解析日期
        Date date = null;
        String time = null;
        try {
            date = sformat.parse(startDate);
//            Calendar c = Calendar.getInstance();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            //calendar.add(Calendar.YEAR, -1);//当前时间减去一年，即一年前的时间
            calendar.add(Calendar.MONTH, -1);//当前时间前去一个月，即一个月前的时间
            time = sformat.format(calendar.getTime());//获取一年前的时间，或者一个月前的时间
        }catch (ParseException e){
            log.error(e.getMessage(),e);
        }
        return time;
    }

    /**
     *  去年的当月
     * @param startDate
     * @return
     */
    public static String lastYearAndMonth(String startDate){
        SimpleDateFormat sformat = new SimpleDateFormat("yyyy-MM");
        //解析日期
        Date date = null;
        String time = null;
        try {
            date = sformat.parse(startDate);
//            Calendar c = Calendar.getInstance();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.YEAR, -1);//当前时间减去一年，即一年前的时间
           // calendar.add(Calendar.MONTH, -1);//当前时间前去一个月，即一个月前的时间
            time = sformat.format(calendar.getTime());//获取一年前的时间，或者一个月前的时间
        }catch (ParseException e){
            log.error(e.getMessage(),e);
        }
        return time;
    }
}
