package com.yongqing.spring.mybatis.sqloutput.aspect;

import com.yongqing.log.output.utils.LogPrint;
import com.yongqing.spring.mybatis.sqloutput.util.SqlUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.ibatis.session.SqlSessionFactory;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 *
 */
@Aspect
@Component
@Log4j2
@ConfigurationProperties(
        prefix = "sqlOutput"
)
public class SqlOutputAdvice {
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
    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    @Pointcut("execution(* com.yongqing.spring.mybatis.sqloutput.mapper..SqlBaseMapper+.*(..))||@within(com.yongqing.spring.mybatis.sqloutput.annotation.SqlOutput) || @annotation(com.yongqing.spring.mybatis.sqloutput.annotation.SqlOutput) ")
    private void sql() {

    }

    @Before("com.yongqing.spring.mybatis.sqloutput.aspect.SqlOutputAdvice.sql()")
    public void before(JoinPoint joinPoint) {

    }

    @AfterReturning("com.yongqing.spring.mybatis.sqloutput.aspect.SqlOutputAdvice.sql()")
    public void afterReturning(JoinPoint joinPoint) {

    }

    @Around("com.yongqing.spring.mybatis.sqloutput.aspect.SqlOutputAdvice.sql()")
    public Object around(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        if ("2".equals(sqlOutputType)) {
            log.info("start to exec SqlOutputAdvice...Kind:{},sqlBusinessId:{},sqlBusinessType:{},sqlBusinessSystem:{},sqlBusinessName:{}", proceedingJoinPoint.getKind(), sqlBusinessId, sqlBusinessType, sqlBusinessSystem, sqlBusinessName);
            Object proceed = null;
            try {
                proceed = proceedingJoinPoint.proceed(proceedingJoinPoint.getArgs());
            } catch (Throwable throwable) {
                log.error("ProceedingJoinPoint exec cause Exception", throwable);
            }
            try {
                Map<String, String> sqlreturn = SqlUtil.getMybatisSql(proceedingJoinPoint, sqlSessionFactory);
                String sql = sqlreturn.get("sql");
                log.info("SqlOutputAdvice获取SQL,sqlMethodId:{},SQL:{}", sqlreturn.get("sqlMethodId"), sql);
                String sqlType = "0";
//        if (proceedingJoinPoint.getArgs().length == 4) {
//            sqlType = "1";
//        }
                Map<String, Object> customizeField = new HashMap<String, Object>();
                customizeField.putAll(sqlreturn);
                LogPrint.printSqlLog(sqlBusinessId, sqlBusinessType, sqlBusinessSystem, sqlBusinessName, sqlType, Arrays.asList(sql.split(";")), customizeField);
                log.info("end to exec SqlOutputAdvice...");
            } catch (Throwable e) {
                log.error("SqlOutputAdvice获取SQL时，Dao中的sqlMap 接口中方法的参数除了实体类和Map外，参数必须有@Param注解标注...");
                log.error("SqlOutputAdvice获取SQL cause Exception", e);
            }
            return proceed;
        } else {
            return proceedingJoinPoint.proceed(proceedingJoinPoint.getArgs());
        }
    }

    @AfterThrowing("com.yongqing.spring.mybatis.sqloutput.aspect.SqlOutputAdvice.sql()")
    public void afterException(JoinPoint joinPoint) {

    }

    @After("com.yongqing.spring.mybatis.sqloutput.aspect.SqlOutputAdvice.sql()")
    public void after(JoinPoint joinPoint) {

    }
}
