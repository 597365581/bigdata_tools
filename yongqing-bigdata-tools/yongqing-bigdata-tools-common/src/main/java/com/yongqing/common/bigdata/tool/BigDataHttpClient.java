package com.yongqing.common.bigdata.tool;

import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 *
 */
@Log4j2
public abstract class BigDataHttpClient {
    //http 连接管理器
    private static PoolingHttpClientConnectionManager cm;
    //空字符串
    private static String EMPTY_STR = "";
    private static final  String PROTOCOL_HTTP = "http";
    private static final  String PROTOCOL_HTTPS = "https";
    private static HttpClientBuilder httpBulder = null;

    private static HttpClientBuilder init() {
        if (cm == null) {
            cm = new PoolingHttpClientConnectionManager();
            cm.setMaxTotal(50);// 整个连接池最大连接数
            cm.setDefaultMaxPerRoute(5);// 每路由最大连接数，默认值是2
        }
        if(null == httpBulder){
            httpBulder = HttpClients.custom().setConnectionManager(cm);
        }
        return httpBulder;
    }
    public synchronized static CloseableHttpClient getHttpClient() {
        return init().build();
    }

    /**
     *  http 请求解析
     * @param request
     * @return
     */
    public static String getResult(HttpRequestBase request) {
        //获取HTTP连接
        CloseableHttpClient httpClient = getHttpClient();
        try {
            CloseableHttpResponse response = httpClient.execute(request);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String result = EntityUtils.toString(entity);
                    response.close();
                    return result;
                } else {
                    log.error("request failed,return is null,please check...");
                }
            } else {
                response.close();
                log.error("request failed! " + response.getStatusLine().getStatusCode());
                return EMPTY_STR;
            }
        } catch (ClientProtocolException e) {
            log.error("getResult ClientProtocolException error:", e);
        } catch (IOException e) {
            log.error("getResult IOException error:", e);
        }
        return EMPTY_STR;
    }
    public static String post(String url, Map<String, Object> params) {
        List<NameValuePair> formData = new ArrayList<NameValuePair>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            formData.add(new BasicNameValuePair(entry.getKey(), String.valueOf(entry.getValue())));
        }
        //获取HTTP连接
        CloseableHttpClient httpClient = getHttpClient();
        HttpPost post = new HttpPost(url);
        try {
            UrlEncodedFormEntity requestEntity = new UrlEncodedFormEntity(formData, "utf-8");
            post.setEntity(requestEntity);
            HttpResponse response = httpClient.execute(post);
            HttpEntity entity = response.getEntity();
            //如果请求成功
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String responseStr = EntityUtils.toString(entity, "utf-8");
                EntityUtils.consume(entity);
                return responseStr;
            } else { //如果请求失败
                // 调用HttpGet的abort，这样就会直接中止这次连接，
                post.abort();
                log.error("execute httpClient post request failed! response code is：" + response.getStatusLine().getStatusCode() );
                return EMPTY_STR;
            }
        } catch (Exception e) {
            post.abort();
            log.error( "execute httpClient post request failed!" + e.getMessage(),e);
            return EMPTY_STR;
        }
    }
    public static String doGet(String url) {
        //获取HTTP连接
        CloseableHttpClient httpClient = getHttpClient();
        HttpGet httpGet = new HttpGet(url);
        try {
            // 为httpGet实例设置配置
            /*RequestConfig requestConfig = RequestConfig.custom().
                setConnectTimeout(180 * 1000) //设置连接超时时间
                .setConnectionRequestTimeout(180 * 1000) // 设置请求超时时间
                .setSocketTimeout(180 * 1000)
                .setRedirectsEnabled(true) //默认允许自动重定向
                .build();
            httpPost.setConfig(requestConfig);*/
            //为httpGet设置请求头
            httpGet.setHeader("Accept", "application/json;charset=UTF-8");
            httpGet.setHeader("Accept-Encoding", "gzip, deflate, sdch");
            httpGet.setHeader("Connection", "Close");
            // 执行get请求得到返回对象
            HttpResponse response = httpClient.execute(httpGet);
            // 通过返回对象获取返回数据
            HttpEntity entity = response.getEntity();
            int stateCode = response.getStatusLine().getStatusCode();
            if (stateCode ==  HttpStatus.SC_OK && entity != null) {
                String responseStr = EntityUtils.toString(entity, "utf-8");
                EntityUtils.consume(entity);
                return responseStr;
            }  else {
                // 调用HttpPost的abort，这样就会直接中止这次连接，
                httpGet.abort();
                log.error("execute httpClient get request failed! response code is：{}",  stateCode);
                return EMPTY_STR;
            }
        } catch (Exception e) {
            httpGet.abort();
            log.error( "execute httpClient get request failed!" + e.getMessage(),e);
            return EMPTY_STR;
        }
    }
    public static String postJsonData(String url, String jsonParams){
        //获取HTTP连接
        CloseableHttpClient httpClient = getHttpClient();
        HttpPost httpPost = new HttpPost(url);
        /*RequestConfig requestConfig = RequestConfig.custom().
                setConnectTimeout(180 * 1000) //设置连接超时时间
                .setConnectionRequestTimeout(180 * 1000) // 设置请求超时时间
                .setSocketTimeout(180 * 1000)
                .setRedirectsEnabled(true) //默认允许自动重定向
                .build();
        httpPost.setConfig(requestConfig);*/
        httpPost.setHeader("Content-Type", ContentType.APPLICATION_JSON.toString());

        try {
            StringEntity stringEntity = new StringEntity(jsonParams, Charset.forName("UTF-8"));
            stringEntity.setContentEncoding("UTF-8");
            // 发送Json格式的数据请求
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
//            System.out.println("request parameters" + EntityUtils.toString(httpPost.getEntity()));
            HttpResponse httpResponse = httpClient.execute(httpPost);
            HttpEntity entity = httpResponse.getEntity();
            //如果请求成功
//            System.out.println("status code:"+response.getStatusLine().getStatusCode());
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String responseStr = EntityUtils.toString(entity, "utf-8");
                EntityUtils.consume(entity);
                return responseStr;
            } else {
                // 调用HttpPost的abort，这样就会直接中止这次连接，
                httpPost.abort();
                log.error("execute httpClient post request failed! response code is：" + httpResponse.getStatusLine().getStatusCode() );
                return EMPTY_STR;
            }
        } catch (Exception e) {
            httpPost.abort();
            log.error( "execute httpClient post request failed!" + e.getMessage(),e);
            return EMPTY_STR;
        }
        finally {
//            if(null != httpClient){
//                try {
//                    httpClient.close();
//                } catch (IOException e) {
//                    log.error("close httpclient cause error",e);
//                }
//            }
        }
    }
    public static byte[] getImageFromNetByUrl(String imgUrl) {
        // 最大重试次数; 重试时间间隔
        long retryTimes = 2L, retryIntervals = 800L;
        int redo = 0; // 重试次数
        HttpURLConnection conn = null;
        InputStream inStream = null;
        while(redo < retryTimes + 1){
            try{
                try {
                    log.info("HttpUtils getImageFromNetByUrl {} , retry {}", imgUrl, redo);
                    //设置代理访问
//                    System.setProperty("proxyType", "4");
//                    System.setProperty("http.proxyHost", "10.19.110.55");
//                    System.setProperty("http.proxyPort", "8080");
//                    System.setProperty("http.proxyHost", PassengerFlowConfig.getInstance().getItem("http.proxyHost"));
//                    System.setProperty("http.proxyPort", PassengerFlowConfig.getInstance().getItem("http.proxyPort"));
//                    System.setProperty("proxySet", "true");
                    URL url = new URL(imgUrl);
                    String host = url.getHost();//获取域名
                    String protocol = url.getProtocol();//获取协议类型
                    if(protocol.equals(PROTOCOL_HTTPS)){ //判断协议类型是否是HTTPS
                        conn = (HttpsURLConnection) url.openConnection();// 打开 HTTPS 连接
                    }else{
                        conn = (HttpURLConnection) url.openConnection(); // 打开 HTTP 连接
                    }
                    conn.setRequestMethod("GET");
                    conn.setConnectTimeout(5 * 1000);
                    conn.setReadTimeout(10 * 1000);
                    inStream = conn.getInputStream();//通过输入流获取图片数据
                    return readInputStream(inStream);//得到图片的二进制数据
                } catch (Exception e) {
                    log.error(e.getMessage(),e);
                } finally {
                    try {
                        if (inStream != null) {
                            inStream.close();
                            inStream = null;
                        }
                    } catch (Exception e) {

                        log.error("图片" + imgUrl + "下载出错" ,e);

                    }
                    if (conn != null) {
                        conn.disconnect();
                        conn = null;
                    }
                }
            }
            catch(Exception e){
                redo++; // 异常时, 重试次数增加
                log.error("HttpUtils getImageFromNetByUrl occur error, will retry {}", redo, e);
                if(redo < retryTimes + 1) {
                    try {
                        Thread.sleep(retryIntervals);
                    } catch (InterruptedException ie) {
                        log.error("Thread sleep occur InterruptedException.", ie);
                    }
                    continue; // 结束本次循环
                }
                else{
                    return new byte[]{};
                }
            }
        }
        return new byte[]{};
    }
    /**
     * 从输入流中获取数据
     *
     * @param inStream 输入流
     * @return 字节数组
     * @throws Exception 异常
     */
    public static byte[] readInputStream(InputStream inStream) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = inStream.read(buffer)) != -1) {
            outStream.write(buffer, 0, len);
        }
        if(null!=inStream){
            inStream.close();
        }
        byte[] picBytes = outStream.toByteArray();
        if(null!=outStream){
            outStream.close();
        }
        return picBytes;
    }
}
