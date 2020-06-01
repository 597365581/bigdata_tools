package com.yongqing.common.bigdata.tool;

import com.itextpdf.text.Document;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.tool.xml.XMLWorkerHelper;

import java.io.*;
import java.nio.charset.Charset;

/**
 *
 */
public class HtmlToPdf {
    public static byte[] htmlToPDF(String htmlString, InputStream inCssFileStream) throws FileNotFoundException, UnsupportedEncodingException {
        ByteArrayInputStream byteArrayInputStream=null;
        try{
             byteArrayInputStream = new ByteArrayInputStream(htmlString.getBytes("UTF-8"));
        }
        finally {
            if(null!=byteArrayInputStream){
                try {
                    byteArrayInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return htmlToPDF(byteArrayInputStream, inCssFileStream);
    }
    public static byte[] htmlToPDF(String htmlString) throws FileNotFoundException, UnsupportedEncodingException {
        return htmlToPDF(htmlString, null);
    }

    public static byte[] htmlToPDF(File htmlFile, InputStream inCssFileStream) throws FileNotFoundException {
        FileInputStream fileInputStream=null;
        try{
            fileInputStream = new FileInputStream(htmlFile);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return htmlToPDF(fileInputStream, inCssFileStream);
    }

    public static byte[] htmlToPDF(InputStream htmlFileStream, InputStream inCssFileStream) {
        Document document = new Document(PageSize.A4);
        OutputStream outputStream = null;
        String outPath = System.getProperty("java.io.tmpdir") + "/" + UUIDGenerator.getUUID() + ".pdf";
        File outFile = new File(outPath);
        try {
            outputStream = new FileOutputStream(outFile);
            PdfWriter pdfWriter = PdfWriter.getInstance(document, outputStream);
            document.open();
            XMLWorkerHelper worker = XMLWorkerHelper.getInstance();
            worker.parseXHtml(pdfWriter, document, htmlFileStream, inCssFileStream, Charset.forName("UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            document.close();
            if (null != outputStream) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            return FileStreamUtils.file2Bytes(outFile);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != outFile && outFile.exists()) {
                outFile.delete();
            }
        }
        return null;
    }

    public static byte[] htmlToPDF(File htmlFile) throws FileNotFoundException {
        return htmlToPDF(htmlFile, null);
    }
}
