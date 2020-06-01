package com.yongqing.common.bigdata.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Base64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 */
@Log4j2
public class MD5Utils {
    private static MessageDigest md5 =null;
    static {
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            log.error("MessageDigest.getInstance(\"MD5\") cause Exception",e);
        }
    }
    //24位
    public static String getMD5StrByBase64String(String strValue) throws Exception {
        return Base64.encodeBase64String(md5.digest(strValue.getBytes("UTF-8")));
    }
    //32位
    public static String getMD5Str(String strValue) throws Exception {
        String result = "";
        md5.update((strValue).getBytes("UTF-8"));
        int i;
        StringBuffer buf = new StringBuffer("");
        byte b[] = md5.digest();
        for(int offset=0; offset<b.length; offset++){
           i = b[offset];
            if(i<0){
                i+=256;
            }
            if(i<16){
                buf.append("0");
            }
            buf.append(Integer.toHexString(i));
        }
        return buf.toString();
    }
}
