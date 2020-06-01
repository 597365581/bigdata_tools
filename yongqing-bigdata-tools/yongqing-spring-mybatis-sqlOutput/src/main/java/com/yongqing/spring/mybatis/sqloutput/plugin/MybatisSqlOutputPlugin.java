package com.yongqing.spring.mybatis.sqloutput.plugin;

import com.yongqing.log.output.utils.LogPrint;
import com.yongqing.spring.mybatis.sqloutput.util.SqlUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.util.*;

/**
 *
 */
@Intercepts({
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        @Signature(type = Executor.class, method = "queryCursor", args = {MappedStatement.class, Object.class, RowBounds.class}),
})
@Log4j2
@Transactional
@ConfigurationProperties(prefix = "sqlOutput")
public class MybatisSqlOutputPlugin implements Interceptor {
    @Value("${sqlOutput.sqlBusinessId}")
    private String sqlBusinessId;
    @Value("${sqlOutput.sqlBusinessType}")
    private String sqlBusinessType;
    @Value("${sqlOutput.sqlBusinessSystem}")
    private String sqlBusinessSystem;
    @Value("${sqlOutput.sqlBusinessName}")
    private String sqlBusinessName;
    // 1 mybarisPlug 2 切点的方式
    @Value("${sqlOutput.type}")
    private String sqlOutputType;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object returnVal = null;
        if ("1".equals(sqlOutputType)) {
            MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
            Object parameter = null;
            if (invocation.getArgs().length > 1) {
                parameter = invocation.getArgs()[1];
            }
            BoundSql boundSql = mappedStatement.getBoundSql(parameter);
            Configuration configuration = mappedStatement.getConfiguration();
            returnVal = invocation.proceed();
            //获取sql语句
            String sql = SqlUtil.getSql(configuration, boundSql);
            log.info("start to exec MybatisSqlOutputPlugin...");
            log.info("MybatisSqlOutputPlugin拦截器获取SQL,ID: " + mappedStatement.getId() + ",sql: " + sql);
            String sqlType = "0";
            if (invocation.getArgs().length == 4 || invocation.getArgs().length == 6) {
                sqlType = "1";
            }
            Map<String, Object> customizeField = new HashMap<String, Object>();
            customizeField.put("sqlMethodId", mappedStatement.getId());
            Connection connection = configuration.getEnvironment().getDataSource().getConnection();
            try {
                customizeField.put("sqlDataBaseUrl", connection.getMetaData().getURL());
                customizeField.put("sqlDataBaseUserName", connection.getMetaData().getUserName());
                customizeField.put("sqlDataBaseDriver", connection.getMetaData().getDriverName());
                customizeField.put("sql", sql);
            } catch (Throwable e) {
                log.error("MybatisSqlOutputPlugin cause Exception", e);
            } finally {
                connection.close();
            }
            LogPrint.printSqlLog(sqlBusinessId, sqlBusinessType, sqlBusinessSystem, sqlBusinessName, sqlType, Arrays.asList(sql.split(";")), customizeField);
//        Object target = invocation.getTarget();
//        Object result = null;
//        if (target instanceof Executor) {
//            long start = System.currentTimeMillis();
//            Method method = invocation.getMethod();
//            /**执行方法*/
//            result = invocation.proceed();
//            long end = System.currentTimeMillis();
//            log.info("[" + method.getName() + "] 耗时 [" + (end - start) + "] ms");
//        }
            log.info("end to exec MybatisSqlOutputPlugin...");
        } else {
            returnVal = invocation.proceed();
        }
        return returnVal;

    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
    }
}
