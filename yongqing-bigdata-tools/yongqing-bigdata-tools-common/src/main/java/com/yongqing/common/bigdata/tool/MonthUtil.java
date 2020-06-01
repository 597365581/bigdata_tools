package com.yongqing.common.bigdata.tool;


import lombok.extern.log4j.Log4j2;


import java.text.SimpleDateFormat;
import java.util.*;

@Log4j2
public class MonthUtil {

    public static SimpleDateFormat dateFormatLong(){
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
    /**
     * 指定日期所在月的开始时间
     *
     * @return 时间
     */
    public static Date getMonthStartTime(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        Date now = null;
        try {
            c.set(Calendar.DATE, 1);
            now = DayUtil.dateFormatDay().parse(DayUtil.dateFormatDay().format(c.getTime()));
        } catch (Exception e) {
            log.error("getMonthStartTime error:", e);
        }
        return now;
    }

    /**
     * 指定日期所在月的结束时间
     *
     * @return 时间
     */
    public static Date getMonthEndTime(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        Date now = null;
        try {
            c.set(Calendar.DATE, 1);
            c.add(Calendar.MONTH, 1);
            c.add(Calendar.DATE, -1);
            now = DayUtil.dateFormat().parse(DayUtil.dateFormatDay().format(c.getTime()) + " 23:59:59");
        } catch (Exception e) {
            log.error("getMonthEndTime error:", e);
        }
        return now;
    }

    /**
     * 指定月所在的日期
     *
     * @return 时间
     */
    public static Date getEndTimeOfDayOfMonth(Date date,int dayOfMonth) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        Date now = null;
        try {
            c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
            now = DayUtil.dateFormat().parse(DayUtil.dateFormatDay().format(c.getTime()) + " 23:59:59");
        } catch (Exception e) {
            log.error("getEndTimeOfDayOfMonth error:",e);
        }
        return now;
    }



    /**
     * 指定日期所在年的开始时间
     *
     * @return 时间
     */
    public static Date getYearStartTime(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        Date now = null;
        try {
            c.set(Calendar.DAY_OF_YEAR, 1);
            now = DayUtil.dateFormatDay().parse(DayUtil.dateFormatDay().format(c.getTime()));
        } catch (Exception e) {
            log.error("getYearStartTime error:", e);
        }
        return now;
    }

    // 获取下一月的日期
    public static Date nextMonth(Date curDate) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(curDate);
        cal.add(GregorianCalendar.MONTH, 1);// 在月份上加1
        return cal.getTime();
    }

    // 获取上一月的日期
    public static Date lastMonth(Date curDate) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(curDate);
        cal.add(GregorianCalendar.MONTH, -1);
        return cal.getTime();
    }

    /**
     * 获取月内日期列表
     *
     * @param startDate 月开始日期
     * @param endDate   月结束日期
     * @return 月内日期列表
     */
    public static List<String> getDateMonthList(Date startDate, Date endDate) {
        List<String> results = new ArrayList<String>();
        Date tmpStartTime = DayUtil.getMoningTimeOfDay(startDate);
        while (tmpStartTime.before(endDate)) {
            results.add(DayUtil.dateFormatDayShort().format(tmpStartTime));
            tmpStartTime = DayUtil.dayInc(tmpStartTime);//日期递增
        }
        return results;
    }
}
