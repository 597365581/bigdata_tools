package com.yongqing.hdfs.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 */
@Log4j2
public class HdfsClient extends AbstractHdfsClient {
    protected HdfsClient() {
        super();
    }

    protected HdfsClient(Properties properties) {
        super(properties);
    }

    @Override
    public boolean createHdfspath(String path) {
        try {
            Path hdfsPath = new Path(path);
            if (getFileSystem().exists(hdfsPath)) {
                log.info("hdfsPath:{} is exist", path);
                return true;
            } else {
                return getFileSystem().mkdirs(hdfsPath);
            }
        } catch (Exception e) {
            log.error("createHdfspath cause Exception", e);
        }
        return false;
    }

    public FSDataInputStream getFSDataInputStream(String filePath, String fileName) throws IOException {
        return getFSDataInputStream(filePath, fileName, null);
    }

    public String getFSDataInputStreamByReadLine(String filePath, String fileName) throws IOException {
        StringBuilder result = new StringBuilder();
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = getFSDataInputStream(filePath, fileName);

            String tmp = null;
            while (null != (tmp = fsDataInputStream.readLine())) {
                result.append(tmp);
                result.append("\n");
            }

        } catch (Exception e) {
            log.error("getFSDataInputStreamToString cause Exception", e);
        } finally {
            if (null != fsDataInputStream) {
                fsDataInputStream.close();
            }
        }
        return result.toString();
    }

    public FSDataInputStream getFSDataInputStream(String filePath, String fileName, Integer bufferSize) throws IOException {
        FSDataInputStream fsDataInputStream = null;
        if (filePath.endsWith("/")) {
            fileName = filePath + fileName;
        } else {
            fileName = filePath + "/" + fileName;
        }
        Path hdfsFilePath = new Path(fileName);
        if (getFileSystem().exists(hdfsFilePath)) {
            if (null != bufferSize && bufferSize > 0) {
                return getFileSystem().open(hdfsFilePath, bufferSize);
            } else {
                return getFileSystem().open(hdfsFilePath);
            }
        } else {
            throw new RuntimeException("fileName " + fileName + " is not exist");
        }
    }

    public List<String> listFiles(String filePath, boolean recursive) throws IOException {
        List<String> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = getFileSystem().listFiles(new Path(filePath), recursive);
        while (iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            list.add(locatedFileStatus.getPath().getName());
        }
        return list;
    }

    public FSDataOutputStream getFSDataOutputStream(String filePath, String fileName, int bufferSize) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        if (filePath.endsWith("/")) {
            fileName = filePath + fileName;
        } else {
            fileName = filePath + "/" + fileName;
        }
        log.info("getFSDataOutputStream fileName :{}", fileName);
        Path hdfsFilePath = new Path(fileName);
        if (!getFileSystem().exists(hdfsFilePath)) {
            getFileSystem().createNewFile(hdfsFilePath);
        }
        fsDataOutputStream = getFileSystem().append(hdfsFilePath, bufferSize);
        return fsDataOutputStream;
    }

    public void appendToFileByWrite(String filePath, String fileName, int bufferSize, byte[] bytes) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.write(bytes);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteBytes(String filePath, String fileName, int bufferSize, String inputStr) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeBytes(inputStr);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteChars(String filePath, String fileName, int bufferSize, String inputStr) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeChars(inputStr);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteBoolean(String filePath, String fileName, int bufferSize, Boolean input) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeBoolean(input);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteDouble(String filePath, String fileName, int bufferSize, Double input) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeDouble(input);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteLong(String filePath, String fileName, int bufferSize, Long input) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeLong(input);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteUTF(String filePath, String fileName, int bufferSize, String input) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeUTF(input);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public void appendToFileByWriteFloat(String filePath, String fileName, int bufferSize, Float input) throws IOException {
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = getFSDataOutputStream(filePath, fileName, bufferSize);
            fsDataOutputStream.writeFloat(input);
        } catch (Exception e) {
            log.error("appendToFile cause error", e);
        } finally {
            if (null != fsDataOutputStream) {
                fsDataOutputStream.close();
            }
        }
    }

    public boolean delete(String filePath, String fileName) throws IOException {
        if (filePath.endsWith("/")) {
            fileName = filePath + fileName;
        } else {
            fileName = filePath + "/" + fileName;
        }
        Path hdfsFilePath = new Path(fileName);
        return getFileSystem().deleteOnExit(hdfsFilePath);
    }

}
