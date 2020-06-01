package com.yongqing.common.bigdata.tool;

import lombok.extern.log4j.Log4j2;

import java.io.*;

@Log4j2
public class FileStreamUtils {


    /*
     * 将byte数组转换为InputStream
     * @param src
     * @return java.io.InputStream
     */
    public static InputStream bytes2InputStream(byte[] bytes) {
        return new ByteArrayInputStream(bytes);
    }

    /*
     * InputStream转换为byte[]
     * @param src
     * @return  byte[]
     */
    public static byte[] inputStream2Bytes(InputStream is) throws IOException{
        ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
        byte[] buff = new byte[100];
        int rc = 0;
        while ((rc = is.read(buff, 0, 100)) > 0) {
            swapStream.write(buff, 0, rc);
        }
        byte[] in2b = swapStream.toByteArray();
        return in2b;
    }

    public static InputStream copyInputStream(InputStream src) throws IOException {
        byte[] srcBytes = inputStream2Bytes(src);
        return bytes2InputStream(srcBytes);
    }

    /*
     *  将File 转换为byte数组
     * @param src
     * @return  byte[]
     */
    public static byte[] file2Bytes(File file) throws IOException{
        FileInputStream fis = null;
        ByteArrayOutputStream bos = null;
        try {
            fis = new FileInputStream(file);
            bos = new ByteArrayOutputStream((int)file.length());
            int buf_size = 1024;
            byte[] b = new byte[buf_size];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e1) {
                    log.error("IOException", e1);
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e1) {
                    log.error("IOException", e1);
                }
            }
        }

        return bos.toByteArray();
    }

    /*
     *  将byte数组转换为File
     * @param bytes
     * @param filePath
     * @param fileName
     * @return  void
     */
    public static void bytes2File(byte[] bytes, String filePath, String fileName) throws IOException {
        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        File file = null;
        try {
            File dir = new File(filePath);
            if(!dir.exists()&&dir.isDirectory()){//判断文件目录是否存在
                dir.mkdirs();
            }
            file = new File(filePath+"\\"+fileName);
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            bos.write(bytes);
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e1) {
                    log.error("IOException", e1);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e1) {
                    log.error("IOException", e1);
                }
            }
        }
    }

}
