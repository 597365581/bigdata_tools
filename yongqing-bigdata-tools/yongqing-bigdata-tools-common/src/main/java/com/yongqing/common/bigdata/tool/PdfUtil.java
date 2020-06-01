package com.yongqing.common.bigdata.tool;

import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfReaderContentParser;
import com.itextpdf.text.pdf.parser.SimpleTextExtractionStrategy;
import com.itextpdf.text.pdf.parser.TextExtractionStrategy;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import technology.tabula.*;

import java.io.File;
import java.io.IOException;


/**
 * PDF 解析工具类
 */
public class PdfUtil {

    public static String getPdfTableContent(String filepath) throws ParseException {
        return getPdfTableContent(filepath, null, null);
    }

    public static String getPdfTableContent(String filepath, String page) throws ParseException {
        return getPdfTableContent(filepath, page, null);
    }

    public static String getPdfTableContent(File file) throws ParseException {
        return getPdfTableContent(file, null);
    }

    public static String getPdfTableContent(File file, String page) throws ParseException {
        return getPdfTableContent(file, page, null);
    }

    public static String getPdfTableContent(File file, Integer page, String password) throws ParseException {
        return getPdfTableContent(file.getPath(), page == null ? null : String.valueOf(page), password);
    }

    public static String getPdfTableContent(File file, String page, String password) throws ParseException {
        return getPdfTableContent(file.getPath(), page, password);
    }

    public static String getPdfTableContent(String filepath, String page, String password) throws ParseException {
        String[] args;
        if (StringUtils.isBlank(page) && StringUtils.isBlank(password)) {
            args = new String[]{"-f=JSON", "-p=all", filepath};
        } else if (StringUtils.isNotBlank(page) && StringUtils.isBlank(password)) {
            args = new String[]{"-f=JSON", "-p=" + page, filepath};
        } else if (StringUtils.isNotBlank(page) && StringUtils.isNotBlank(password)) {
            args = new String[]{"-f=JSON", "-p=" + page, "-s=" + password, filepath};
        } else if (StringUtils.isBlank(page) && StringUtils.isNotBlank(password)) {
            args = new String[]{"-f=JSON", "-p=all", "-s=" + password, filepath};
        } else {
            args = new String[]{"-f=JSON", "-p=all", filepath};
        }
        return getPdfTableContent(args);
    }

    public static String getPdfTableContent(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(CommandLineApp.buildOptions(), args);
        StringBuilder stringBuilder = new StringBuilder();
        new CommandLineApp(stringBuilder, cmd).extractTables(cmd);
        return stringBuilder.toString();
    }

    public static String getPdfFileText(String fileName, Integer page) throws Exception {
        return getPrivatePdfFileText(fileName, page);
    }

    private static <T> String getPrivatePdfFileText(T object, Integer page) throws Exception {
        PdfReader reader = null;
        if (object instanceof byte[]) {
            reader = new PdfReader((byte[]) object);
        } else if (object instanceof String) {
            reader = new PdfReader((String) object);
        } else {
            throw new RuntimeException("object only support String or byte[]");
        }
        if (null == page || 0 == page) {
            page = 1;
        }
        if (page > reader.getNumberOfPages()) {
            throw new RuntimeException("page Nummber>" + object + "'s total pages，please check...");
        }
        PdfReaderContentParser parser = new PdfReaderContentParser(reader);
        StringBuffer buff = new StringBuffer();
        TextExtractionStrategy strategy = parser.processContent(page,
                new SimpleTextExtractionStrategy());
        ;
        buff.append(strategy.getResultantText());
        return buff.toString();
    }

    public static String getPdfFileText(File file, Integer page) throws Exception {
        return getPdfFileText(file.getPath(), page);
    }

    public static String getPdfFileText(File file) throws Exception {
        return getPdfFileText(file.getPath());
    }

    public static String getPdfFileText(String fileName) throws IOException {
        return getPrivatePdfFileText(fileName);
    }

    public static String getPdfFileText(byte[] bytes) throws IOException {

        return getPrivatePdfFileText(bytes);
    }

    public static String getPdfFileText(byte[] bytes, Integer page) throws Exception {

        return getPrivatePdfFileText(bytes, page);
    }

    private static <T> String getPrivatePdfFileText(T object) throws IOException {
        PdfReader reader = null;
        if (object instanceof byte[]) {
            reader = new PdfReader((byte[]) object);
        } else if (object instanceof String) {
            reader = new PdfReader((String) object);
        } else {
            throw new RuntimeException("object only support String or byte[]");
        }
        PdfReaderContentParser parser = new PdfReaderContentParser(reader);
        StringBuffer buff = new StringBuffer();
        TextExtractionStrategy strategy;
        for (int i = 1; i <= reader.getNumberOfPages(); i++) {
            strategy = parser.processContent(i,
                    new SimpleTextExtractionStrategy());
            buff.append(strategy.getResultantText());
        }
        return buff.toString();
    }

}
