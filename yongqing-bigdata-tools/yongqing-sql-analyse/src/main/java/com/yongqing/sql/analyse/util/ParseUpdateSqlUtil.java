package com.yongqing.sql.analyse.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.yongqing.sql.analyse.bean.SqlOutput;
import com.yongqing.sql.analyse.sqlexec.MybatisSqlExec;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Log4j2
public class ParseUpdateSqlUtil {
    /**
     * 解析更新语句
     *
     * @param sql
     * @return
     */
    public static SqlOutput parseUpdateSql(String sql) {
        SqlOutput sqlOutput = new SqlOutput();
        sqlOutput.setSql(sql);
        sqlOutput.setSqlType("update");
        try {
            SQLStatementParser parser = new SQLStatementParser(sql);
            SQLUpdateStatement sqlUpdateStatement = parser.parseUpdateStatement();
            List<SQLUpdateSetItem> listValue = sqlUpdateStatement.getItems();
            Map<String, Object> map = new HashMap<>();
            List<Map<String, Object>> list = new ArrayList<>();
            for (SQLUpdateSetItem sqlUpdateSetItem : listValue) {
                String columnValue=String.valueOf(sqlUpdateSetItem.getValue());
                if(columnValue.contains("'")){
                    columnValue=columnValue.replace("'","");
                }
                map.put(String.valueOf(sqlUpdateSetItem.getColumn()), columnValue);
            }
            list.add(map);
            sqlOutput.setSqlResult(list);
            sqlOutput.setExecStatus("1");
        } catch (Exception e) {
            log.error("parse update Sql error", e);
            sqlOutput.setExecStatus("0");
            sqlOutput.setExecMessage(e.getMessage());
        }
        return sqlOutput;
    }

    /**
     * 解析插入语句
     *
     * @param sql
     * @return
     */
    public static SqlOutput parseInsertSql(String sql) {
        SqlOutput sqlOutput = new SqlOutput();
        sqlOutput.setSql(sql);
        sqlOutput.setSqlType("insert");
        try {
            SQLStatementParser parser = new SQLStatementParser(sql);
            SQLInsertStatement insertStmt = (SQLInsertStatement) parser.parseInsert();
            List<SQLExpr> columnList = insertStmt.getColumns();
            if (null != columnList && columnList.size() > 0) {
                List<String> columnStrList = new ArrayList<>();
                columnList.forEach(column -> {
                    columnStrList.add(String.valueOf(column));
                });
                SQLSelect sqlSelect = insertStmt.getQuery();
                if (null != sqlSelect) {//包含select语句
                    String selectSql = String.valueOf(sqlSelect);
                    MybatisSqlExec mybatisSqlExec = new MybatisSqlExec();
                    sqlOutput = mybatisSqlExec.executeQuery(selectSql, columnStrList);
                } else if (null != insertStmt.getValuesList() && insertStmt.getValuesList().size() > 0) {//values类型语句
                    List<Map<String, Object>> list = new ArrayList<>();
                    for (SQLInsertStatement.ValuesClause valuesClause : insertStmt.getValuesList()) {
                        Map<String, Object> map = new HashMap<>();
                        for (int i = 0; i < valuesClause.getValues().size(); i++) {
                            String columnName = columnStrList.get(i);
                            String columnValue = String.valueOf(valuesClause.getValues().get(i));
                            if (columnValue.contains("'")) {
                                columnValue = columnValue.replace("'", "");
                            }
                            map.put(columnName, columnValue);
                        }
                        list.add(map);
                        sqlOutput.setSqlResult(list);
                        sqlOutput.setExecStatus("1");
                        sqlOutput.setEffectRowNum(list.size());
                    }
                }else{
                    log.info("update sql without data is not need to deal");
                }
            } else {
                //TODO find all column
            }
        } catch (Exception e) {
            log.error("parse insert sql error", e);
            sqlOutput.setExecStatus("0");
            sqlOutput.setExecMessage(e.getMessage());
        }
        return sqlOutput;
    }
}
