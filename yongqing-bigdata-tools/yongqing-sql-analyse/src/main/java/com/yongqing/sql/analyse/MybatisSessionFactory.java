package com.yongqing.sql.analyse;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 *
 *  sqlsession factory 单例  事务设置为手动提交
 */
public class MybatisSessionFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MybatisSessionFactory.class);
    private static SqlSessionFactory sqlSessionFactory;
    private MybatisSessionFactory(){
        super();
    }
    public synchronized static SqlSessionFactory getSqlSessionFactory(){
        if(null==sqlSessionFactory){
            InputStream inputStream=null;
            try{
                inputStream = MybatisSessionFactory.class.getClassLoader().getResourceAsStream("mybatis-config.xml");
                sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            }
            catch (Exception e){
                LOG.error("create MybatisSessionFactory read mybatis-config.xml cause Exception",e);
            }
            if(null!=sqlSessionFactory){
                LOG.info("get Mybatis sqlsession sucessed....");
            }
            else {
                LOG.info("get Mybatis sqlsession failed....");
            }
        }
        return sqlSessionFactory;
    }
}
