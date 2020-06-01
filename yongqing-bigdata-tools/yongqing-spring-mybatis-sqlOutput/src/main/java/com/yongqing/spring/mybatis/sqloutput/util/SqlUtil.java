package com.yongqing.spring.mybatis.sqloutput.util;

import lombok.extern.log4j.Log4j2;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.*;


/**
 *
 */
@Log4j2
public class SqlUtil {


    public static Map<String, String> getMybatisSql(ProceedingJoinPoint pjp, SqlSessionFactory sqlSessionFactory)  {
        Map<String, Object> map = new HashMap<>();
        //1.获取namespace+methdoName
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Method method = signature.getMethod();
        String namespace = method.getDeclaringClass().getName();
        String methodName = method.getName();
        //2.根据namespace+methdoName获取相对应的MappedStatement
        Configuration configuration = sqlSessionFactory.getConfiguration();
        MappedStatement mappedStatement = configuration.getMappedStatement(namespace + "." + methodName);
//        //3.获取方法参数列表名
//        Parameter[] parameters = method.getParameters();
        //4.形参和实参的映射
        Object[] objects = pjp.getArgs(); //获取实参
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < parameterAnnotations.length; i++) {
            Object object = objects[i];
            if (parameterAnnotations[i].length == 0) { //说明该参数没有注解，此时该参数可能是实体类，也可能是Map，也可能只是单参数
                if (object.getClass().getClassLoader() == null && object instanceof Map) {
                    map.putAll((Map<? extends String, ?>) object);
                }
                else if(object.getClass().getClassLoader() == null && object instanceof List){
                    map.put("list",object);
                }
                else {//形参为自定义实体类
                    try {
                        map.putAll(objectToMap(object));
                    } catch (Exception e) {
                        log.info("getMybatisSql objectToMap cause Exception",e);
                    }
                }
            } else {//说明该参数有注解，且必须为@Param
                for (Annotation annotation : parameterAnnotations[i]) {
                    if (annotation instanceof Param) {
                        map.put(((Param) annotation).value(), object);
                    }
                }
            }
        }
        //5.获取boundSql
        BoundSql boundSql = mappedStatement.getBoundSql(map);
        Map<String, String> result = new HashMap<String, String>();
        result.put("sql", getSql(configuration, boundSql));
        result.put("sqlMethodId", mappedStatement.getId());
        Connection connection = null;
        try {
            connection = configuration.getEnvironment().getDataSource().getConnection();
            result.put("sqlDataBaseUrl", connection.getMetaData().getURL());
            result.put("sqlDataBaseUserName", connection.getMetaData().getUserName());
            result.put("sqlDataBaseDriver", connection.getMetaData().getDriverName());
        } catch (Throwable e) {
            log.error("getMybatisSql cause Exception", e);
        }
        finally {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("getMybatisSql cause Exception", e);
            }
        }
        return result;
    }


    public static String getSql(Configuration configuration, BoundSql boundSql) {
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

    private static String getParameterValue(Object obj) {
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

    private static Map<String, Object> objectToMap(Object obj) throws IllegalAccessException {
        Map<String, Object> map = new HashMap<>();
        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object value = field.get(obj);
            map.put(fieldName, value);
        }
        return map;
    }
}
