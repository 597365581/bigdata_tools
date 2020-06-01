package com.yongqing.common.bigdata.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 *  日期时间工具类
 */
@Log4j2
public class DateCommonUtil {

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
    //日期格式-时区
    public static DateFormat dateFormatTimeZoe(){
        return  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000+08:00'");
    }
    //日期格式-时区
    public static DateFormat dateFormatMonth (){
        return  new SimpleDateFormat("MMddHHmmss");
    }
    //日期格式-月日小时分钟
    public static DateFormat dateFormatMinute (){
        return  new SimpleDateFormat("MMddHHmm");
    }

    public static String getNormalTimeStr(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return simpleDateFormat.format(new Date());
    }

    //Srting转date
    public static Date changeStringToDate(String dateStr) {
        Date date = new Date();
        try {
            if (StringUtils.isNotBlank(dateStr)){
                date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr);
            } else {
                date = null;
            }
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
        }
        return date;

    }

    /**
     * 判断时间是否为正点
     *
     * @param date 时间
     * @return
     */
    public static boolean isIntegralClock(Date date) {
        Calendar gc = Calendar.getInstance();
        gc.setTime(date);

        if ((gc.get(Calendar.MINUTE) == 0) && (gc.get(Calendar.SECOND) == 0)) {
            return true;
        } else {
            return false;
        }
    }
    //date转String精确到毫秒
    public static String changeDateToMillisecondString(Date date) {
        String dateStr = "";
        try {
            dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
        } catch (Exception e) {
            log.error("changeDateToMillisecondString={},{}", e.getMessage(), e);
        }
        return dateStr;
    }


    //date转String
    public static String changeDateToString(Date date) {
        String dateStr = "";
        try {
            if (null != date){
                dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
            }
        } catch (Exception e) {
            log.error("date to String cause Exception",e);
        }
        return dateStr;
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
     * 获取当前的整5分钟 00：05：00 00:10:00
     *
     * @param date 日期
     * @return 时间
     */
    public static Date realFiveMinute(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);   //设置当前日期
        int fiveMinute =  c.get(Calendar.MINUTE)/5*5;
        c.set(Calendar.MINUTE, fiveMinute);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        return c.getTime(); //结果
    }

    /**
     * 获取上个整5分钟 00：05：00 00:10:00
     *
     * @param date 日期
     * @return 时间
     */
    public static Date lastFiveMinute(Date date) {
        Date realFiveDate =  realFiveMinute(date);
        Calendar c = Calendar.getInstance();
        c.setTime(realFiveDate);   //设置当前日期
        c.add(Calendar.MINUTE,-5);
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
            log.error(e.getMessage());
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
     * 获取时间的前多少天
     * interval 时间间隔
     * @return
     */
    public static Date dayInterval(Date date, int interval) {
        Calendar day = Calendar.getInstance();
        day.setTime(date);
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
        Calendar cal= Calendar.getInstance();
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
            // Calendar c = Calendar.getInstance();
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
            // Calendar c = Calendar.getInstance();
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

    /**
     * 获取去年
     * @param date
     * @return
     */
    public static Date lastYear(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.YEAR, -1);
        return c.getTime();
    }
    public static Date getAfterHour(Date dateTime, Integer afterHour){
        Calendar c = Calendar.getInstance();
        c.setTime(dateTime);
        c.add(Calendar.HOUR, +afterHour);
        return c.getTime();
    }
    public static Date getBeforeMonth(Date dateTime, Integer beforeMonth){
        Calendar c = Calendar.getInstance();
        c.setTime(dateTime);
        c.add(Calendar.MONTH, -beforeMonth);
        return c.getTime();
    }
    /**
     * 判断失效时间
     * @param requestUnixTimestamp unix时间戳
     * @return 是否通过
     */
    public static boolean checkRequestTimestamp(long requestUnixTimestamp  ,long scopeTime){
        boolean isVaildTimestamp = false;
        long currentTime = System.currentTimeMillis();
        long minVaildTime = currentTime - scopeTime;
        long maxVaildTime = currentTime + scopeTime;
        if (requestUnixTimestamp >= minVaildTime && requestUnixTimestamp <= maxVaildTime){
            isVaildTimestamp = true;
        }
        return isVaildTimestamp;

    }
    /**
     * 获取两个日期相差的月数
     */
    public static int getMonthDiff(Date d1, Date d2) {
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(d1);
        c2.setTime(d2);
        int year1 = c1.get(Calendar.YEAR);
        int year2 = c2.get(Calendar.YEAR);
        int month1 = c1.get(Calendar.MONTH);
        int month2 = c2.get(Calendar.MONTH);
        int day1 = c1.get(Calendar.DAY_OF_MONTH);
        int day2 = c2.get(Calendar.DAY_OF_MONTH);
        // 获取年的差值 
        int yearInterval = year1 - year2;
        // 如果 d1的 月-日 小于 d2的 月-日 那么 yearInterval-- 这样就得到了相差的年数
        if (month1 < month2 || month1 == month2 && day1 < day2) {
            yearInterval--;
        }
        // 获取月数差值
        int monthInterval = (month1 + 12) - month2;
        if (day1 < day2) {
            monthInterval--;
        }
        monthInterval %= 12;
        int monthsDiff = Math.abs(yearInterval * 12 + monthInterval);
        return monthsDiff;
    }
}
