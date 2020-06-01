package com.yongqing.common.bigdata.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * excel 工具类
 */
@Log4j2
public class ExcelUtil {
    private ExcelUtil(){

    }
    public static boolean isExcel(File file) {
        return file.getName().toLowerCase().endsWith("xlsx") || file.getName().toLowerCase().endsWith("xls");
    }

    public static Workbook getReadWorkbook(File excelFile) throws Exception {
        InputStream inputStream = null;
        try {
            if(!excelFile.exists()){
                throw new RuntimeException("excelFile:"+excelFile+" is not exist");
            }
            inputStream = new FileInputStream(excelFile);
            if (excelFile.getName().toLowerCase().endsWith("xlsx")) {
                return new XSSFWorkbook(inputStream);
            } else if (excelFile.getName().toLowerCase().endsWith("xls")) {
                return new HSSFWorkbook(inputStream);
            } else {
                throw new RuntimeException("Invalid excel file");
            }
        } catch (IOException e) {
            log.error("getReadWorkbook cause Exception",e);
            throw new RuntimeException(e.getMessage());
        } finally {
            if(null!=inputStream){
                inputStream.close();
            }
        }
    }
}
