package com.yongqing.common.bigdata.tool;



import lombok.extern.log4j.Log4j2;
import sun.misc.BASE64Decoder;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
@Log4j2
public class Base64Utils {
    
    final static String baseTable = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    private Base64Utils(){
        super();
    }

    /** 
     * Encode a byte array.  
     *  
     * @param bytes a byte array to be encoded.  
     * @return encoded object as a String object.  
     */  
    public static String encode(byte[] bytes) {
  
        StringBuffer tmp = new StringBuffer();
        int i = 0;  
        byte pos;   
  
        for(i=0; i < (bytes.length - bytes.length%3); i+=3) {  
  
            pos = (byte) ((bytes[i] >> 2) & 63);   
            tmp.append(baseTable.charAt(pos));   
  
            pos = (byte) (((bytes[i] & 3) << 4) + ((bytes[i+1] >> 4) & 15));   
            tmp.append(baseTable.charAt( pos ));  
                      
            pos = (byte) (((bytes[i+1] & 15) << 2) + ((bytes[i+2]  >> 6) & 3));  
            tmp.append(baseTable.charAt(pos));  
          
            pos = (byte) (((bytes[i+2]) & 63));  
            tmp.append(baseTable.charAt(pos));  

        }  
  
        if(bytes.length % 3 != 0) {  
  
            if(bytes.length % 3 == 2) {  
  
                pos = (byte) ((bytes[i] >> 2) & 63);   
                tmp.append(baseTable.charAt(pos));   
  
                pos = (byte) (((bytes[i] & 3) << 4) + ((bytes[i+1] >> 4) & 15));   
                tmp.append(baseTable.charAt( pos ));  
                          
                pos = (byte) ((bytes[i+1] & 15) << 2);  
                tmp.append(baseTable.charAt(pos));  
              
                tmp.append("=");  
  
            } else if(bytes.length % 3 == 1) {  
                  
                pos = (byte) ((bytes[i] >> 2) & 63);   
                tmp.append(baseTable.charAt(pos));   
  
                pos = (byte) ((bytes[i] & 3) << 4);   
                tmp.append(baseTable.charAt( pos ));  
                          
                tmp.append("==");  
            }  
        }  
        return tmp.toString();  
  
    }  
  
    /** 
     * Encode a String object.  
     *  
     * @param src a String object to be encoded with Base64 schema.  
     * @return encoded String object.  
     */  
    public static String encode(String src) {
          
        return encode(src.getBytes());    
    }  
  
    public static byte[] decode(String src) {
  
        byte[] bytes = null;  
  
        StringBuffer buf = new StringBuffer(src);
  

        int i = 0;  
        char c = ' ';
        while( i < buf.length()) {
            c = buf.charAt(i);
            if( c == '\r') {
                buf.deleteCharAt(i);  
                i --;  
            } else if( c == '\n') {  
                buf.deleteCharAt(i);  
                i --;  
            } else if( c == '\t') {  
                buf.deleteCharAt(i);  
                i --;  
            } else if( c == ' ') { 
                buf.deleteCharAt(i);
                i --;  
            }  
            i++;  
        }  
  
        // The source should consists groups with length of 4 chars.   
        if(buf.length() % 4 != 0) {
        	log.error("Base64Utils decode failed! Base64 decoding invalid length!");
            throw new RuntimeException("Base64Utils decode failed! Base64 decoding invalid length!");
        }  
  

        bytes = new byte[3 * (buf.length() / 4)];
        int index = 0;
        int blankBytes = 0;
        // Now decode each group
        for(i = 0; i < buf.length(); i+=4) {  
  
            byte data = 0;  
            int nGroup = 0;  
  
            for(int j = 0; j < 4; j++) {  
  
                char theChar = buf.charAt(i + j);   
  
                if(theChar == '=') { 
                	blankBytes = blankBytes + 1;
                    data = 0;
                } else {  
                    data = getBaseTableIndex(theChar);   
                }  
  
                if(data == -1) {  
                	log.error("Base64Utils decode failed! Base64 decoding bad character!");
                	throw new RuntimeException("Base64Utils decode failed! Base64 decoding bad character!");
                }  
  
                nGroup = 64*nGroup + data;  
            }
  
            bytes[index] = (byte) (255 & (nGroup >> 16));  
            //bytes.put(index, (byte) (255 & (nGroup >> 16)));
            index ++;  
  
            bytes[index] = (byte) (255 & (nGroup >> 8));  
            //bytes.put(index, (byte) (255 & (nGroup >> 8)));
            index ++;  
  
            bytes[index] = (byte) (255 & (nGroup)); 
            //bytes.put(index, (byte) (255 & (nGroup)));
            index ++;  
        }  
          
        //byte[] newBytes = new byte[index];
        byte[] newBytes = new byte[index-blankBytes];
        for(i = 0; i < index-blankBytes; i++) {
        	newBytes[i] = bytes[i];  
        }
  
        return newBytes;  
    }  
  
    /** 
     * Find index number in base table for a given character.  
     *  
     */  
    protected static byte getBaseTableIndex(char c) {  
          
        byte index = -1;  
  
        for(byte i = 0; i < baseTable.length(); i ++) {  
          
            if(baseTable.charAt(i) == c) {  
                index = i;  
                break;  
            }  
        }  
  
        return index;  
    }

    /**
     *  Base64字符串转输入流
     * @param base64String
     * @return
     * @throws RuntimeException
     */
    public static InputStream Base64StringCovertImage(String base64String) {
        if(null==base64String || base64String.length()==0){
            return null;
        }
        BASE64Decoder decoder = new BASE64Decoder();
        try {
            // 解密
            byte[] b = decoder.decodeBuffer(base64String);
            // 处理数据
            for (int i = 0; i < b.length; ++i) {
                if (b[i] < 0) {
                    b[i] += 256;
                }
            }
            return new ByteArrayInputStream(b);
        }
        catch (Exception e){
            log.error("base64 string covert to Image error",e);
            throw new RuntimeException("base64 string covert to Image error"+e.getMessage());
        }
    }
} 