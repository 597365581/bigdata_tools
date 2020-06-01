package com.yongqing.common.bigdata.tool;




import lombok.extern.log4j.Log4j2;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

@Log4j2
public class WeekUtil {


    /**
     * 获得日期所在周开始时间(周一为开始时间)
     *
     * @return 周开始时间
     */
    public static Date getWeekStartTime(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        try {
            //判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了
            int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
            if(1 == dayWeek) {
                cal.add(Calendar.DAY_OF_MONTH, -1);
            }
            cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
            int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
            cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
            cal.setTime(DayUtil.dateFormat().parse(DayUtil.dateFormatDay().format(cal.getTime()) + " 00:00:00"));
        } catch (Exception e) {
            log.error("getWeekStartTime error:", e);
        }
        return cal.getTime();
    }

    /**
     * 获得日期所在周结束时间（周日为结束时间）
     *
     * @return 周结束时间
     */
    public static Date getWeekEndTime(Date date) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        try {
            //判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了
            int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
            if(1 == dayWeek) {
                cal.add(Calendar.DAY_OF_MONTH, -1);
            }
            cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
            int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
            cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
            cal.add(Calendar.DATE, 6);
            cal.setTime(DayUtil.dateFormat().parse(DayUtil.dateFormatDay().format(cal.getTime()) + " 23:59:59"));
        } catch (Exception e) {
            log.error("getWeekEndTime error:",e);
        }
        return cal.getTime();
    }

    /**
     * 获取上一周的日期
     * @param curDate 当前日期
     * @return 上一周的日期
     */
    public static Date lastWeek(Date curDate) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(curDate);
        cal.add(GregorianCalendar.DATE, -7);// 在日期上加-7天
        return cal.getTime();
    }

    /**
     * 获取下一周的日期
     * @param curDate 当前日期
     * @return 上一周的日期
     */
    public static Date nextWeek(Date curDate) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(curDate);
        cal.add(GregorianCalendar.DATE, 7);// 在日期上加7天
        return cal.getTime();
    }

    /**
     * 判断两时间是否为同一周
     * @param dateOne 日期1
     * @param dateTwo 日期2
     * @return 是否为同一周
     */
    public static boolean isInSameWeek(Date dateOne,Date dateTwo) {
        return  getWeekStartTime(dateOne).equals(getWeekStartTime(dateTwo));
    }
}
