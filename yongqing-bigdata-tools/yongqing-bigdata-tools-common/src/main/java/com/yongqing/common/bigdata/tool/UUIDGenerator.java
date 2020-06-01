package com.yongqing.common.bigdata.tool;

import java.text.SimpleDateFormat;
import java.util.UUID;

/**
 * 生成唯一ID
 */
public class UUIDGenerator {
    /**
     *
     * 功能描述: <br>
     * 〈UUID获取〉
     *
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    private static final ThreadLocal<String> threadRequestID = new ThreadLocal<String>();
    private static final ThreadLocal<String> threadNumID = new ThreadLocal<String>();
    private static  int guid=100;
    public static String getRequestID() {
        String requestID = (String) threadRequestID.get();
        if (requestID == null) {
            requestID = UUIDGenerator.getUUID();
            threadRequestID.set(requestID);
        }
        return requestID;
    }
    public static String getNumID() {
        String numID = (String) threadNumID.get();
        if (numID == null) {
            numID = UUIDGenerator.getGuid();
            threadNumID.set(numID);
        }
        return numID;
    }
    public static void destroyRequestID() {
        threadRequestID.set(null);
    }
    public static void destroyNumID() {
        threadNumID.set(null);
    }
    public static String getUUID() {
        // 取两组UUID拼接字符串
        String s = UUID.randomUUID().toString() + UUID.randomUUID().toString();
        // 去除UUID中的'-',并截取成48位
        return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18) + s.substring(19, 23)+s.substring(24,36) ;

    }

    public static String getGuid(){
        guid+=1;
        long now = System.currentTimeMillis();
        //获取4位年份数字
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy");
        //获取时间戳
        String time=dateFormat.format(now);
        String info=Long.toString(now)+"";
        int ran=0;
        if(guid>999){
            guid=100;
        }
        ran=guid;

        return time+info.substring(2, info.length())+ran;
    }


}