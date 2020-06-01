package com.yongqing.mybatis.sqloutput.plugin;

import com.yongqing.log.output.utils.LogPrint;
import lombok.extern.log4j.Log4j2;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.sql.Connection;
import java.text.DateFormat;
import java.util.*;

/**
 *
 */
@Intercepts({
        @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
        @Signature(type = Executor.class, method = "query",  args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class }),
        @Signature(type = Executor.class, method = "query",  args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class,BoundSql.class}),
        @Signature(type = Executor.class, method = "queryCursor",  args = { MappedStatement.class, Object.class, RowBounds.class}),
})
@Log4j2
public class MybatisSqlOutputPlugin implements Interceptor {
    private Properties properties;
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        Object parameter = null;
        if (invocation.getArgs().length > 1) {
            parameter = invocation.getArgs()[1];
        }
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Configuration configuration = mappedStatement.getConfiguration();
        Object returnVal = invocation.proceed();
        //获取sql语句
        String sql = getSql(configuration, boundSql);
        log.info("start to exec MybatisSqlOutputPlugin...");
        log.info("MybatisSqlOutputPlugin拦截器获取SQL,ID: "+mappedStatement.getId()+",sql: "+sql);
        String sqlType="0";
        if(invocation.getArgs().length==4||invocation.getArgs().length==6){
            sqlType="1";
        }
        Map<String,Object> customizeField =  new HashMap<String, Object>();
        customizeField.put("sqlMethodId",mappedStatement.getId());
        Connection connection = configuration.getEnvironment().getDataSource().getConnection();
        try{
            customizeField.put("sqlDataBaseUrl",connection.getMetaData().getURL());
            customizeField.put("sqlDataBaseUserName",connection.getMetaData().getUserName());
            customizeField.put("sqlDataBaseDriver",connection.getMetaData().getDriverName());
        }
        catch (Throwable e){
            log.error("MybatisSqlOutputPlugin cause Exception", e);
        }
        finally {
            connection.close();
        }
        LogPrint.printSqlLog(properties.getProperty("sqlBusinessId"),properties.getProperty("sqlBusinessType"),properties.getProperty("sqlBusinessSystem"),properties.getProperty("sqlBusinessName"),sqlType, Arrays.asList(sql.split(";")),customizeField);
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
        return returnVal;

    }
    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }
    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    private String getSql(Configuration configuration, BoundSql boundSql) {
        Object parameterObject = boundSql.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        String sql = boundSql.getSql().replaceAll("[\\s]+", " ");
        if (parameterObject == null || parameterMappings.size() == 0) {
            return sql;
        }
        TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
        if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
            sql = sql.replaceFirst("\\?", getParameterValue(parameterObject));
        } else {
            MetaObject metaObject = configuration.newMetaObject(parameterObject);
            for (ParameterMapping parameterMapping : parameterMappings) {
                String propertyName = parameterMapping.getProperty();
                if (metaObject.hasGetter(propertyName)) {
                    Object obj = metaObject.getValue(propertyName);
                    sql = sql.replaceFirst("\\?", getParameterValue(obj));
                } else if (boundSql.hasAdditionalParameter(propertyName)) {
                    Object obj = boundSql.getAdditionalParameter(propertyName);
                    sql = sql.replaceFirst("\\?", getParameterValue(obj));
                }
            }
        }
        return sql;
    }

    private String getParameterValue(Object obj) {
        String value = null;
        if (obj instanceof String) {
            value = "'" + obj.toString() + "'";
        } else if (obj instanceof Date) {
            DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.CHINA);
            value = "'" + formatter.format(obj) + "'";
        } else {
            if (obj != null) {
                value = obj.toString();
            } else {
                value = "";
            }
        }
        return value;
    }
}
