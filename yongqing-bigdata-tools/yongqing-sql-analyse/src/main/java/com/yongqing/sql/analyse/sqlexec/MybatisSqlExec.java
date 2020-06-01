package com.yongqing.sql.analyse.sqlexec;

import com.yongqing.sql.analyse.MybatisSessionFactory;
import com.yongqing.sql.analyse.bean.SqlOutput;
import lombok.extern.log4j.Log4j2;
import org.apache.ibatis.session.SqlSession;

import java.util.List;


/**
 *
 */
@Log4j2
public class MybatisSqlExec extends DefaultSqlExec {
    public SqlOutput executeQuery(String sql) {
        SqlOutput result = null;
        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        try {
            result = super.executeQuery(sqlSession.getConnection(), sql);
        } catch (Exception e) {
            log.error("executeQuery cause Exception", e);
        } finally {
            sqlSession.close();
        }
        return result;
    }

    public SqlOutput executeQuery(String sql, List<String> columnList) {
        SqlOutput result = null;
        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        try {
            result = super.executeQuery(sqlSession.getConnection(), sql,columnList);
        } catch (Exception e) {
            log.error("executeQuery cause Exception", e);
        } finally {
            sqlSession.close();
        }
        return result;
    }

    public String exportQuery(String sql, String separator, int start, int length) {
        String result = null;
        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        try {
            result = super.exportQuery(sqlSession.getConnection(), sql, separator, start, length);
        } catch (Exception e) {
            log.error("exportQuery cause Exception", e);
        } finally {
            sqlSession.close();
        }
        return result;
    }

    public SqlOutput executeUpdate(String sql) {
        SqlOutput result = null;
        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        try {
            result = super.executeUpdate(sqlSession.getConnection(), sql);
        } catch (Exception e) {
            log.error("executeUpdate cause Exception", e);
        } finally {
            sqlSession.commit();
            sqlSession.close();
        }
        return result;
    }

    public SqlOutput execute(String sql) {
        SqlOutput result = null;
        SqlSession sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
        try {
            result = super.execute(sqlSession.getConnection(), sql);
        } catch (Exception e) {
            log.error("execute cause Exception", e);
        } finally {
            sqlSession.commit();
            sqlSession.close();
        }
        return result;
    }
}
