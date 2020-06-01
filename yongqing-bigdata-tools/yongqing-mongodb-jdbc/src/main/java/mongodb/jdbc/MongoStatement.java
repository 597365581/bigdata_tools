package mongodb.jdbc;

import com.mongodb.BasicDBObject;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import mongodb.conn.ServerConnection;
import mongodb.query.MongoBuilder;
import mongodb.query.MongoBuilderUpstreamException;
import mongodb.query.MongoInsertQuery;
import mongodb.query.MongoQuery;
import mongodb.query.MongoSelectQuery;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceField;
import unity.engine.IServerConnection;
import unity.generic.jdbc.StatementImpl;
import unity.jdbc.LocalResultSet;
import unity.jdbc.UnityDriver;
import unity.parser.GlobalParser;
import unity.query.GlobalQuery;
import unity.query.GlobalUpdate;
import unity.query.LQTree;
import unity.query.LimitInfo;
import unity.util.StringFunc;

public class MongoStatement extends StatementImpl
{
    protected static ResourceBundle resources = ResourceBundle.getBundle("resources/mongo/MongoStatement", locale);
    private MongoQuery query;

    public MongoStatement()
    {
        super(null, null, 1003, 1007);
    }

    public MongoStatement(MongoConnection cnt, IServerConnection con, int resultSetType, int resultSetConcurrency)
    {
        super(cnt, con, resultSetType, resultSetConcurrency);
    }

    public ResultSet executeQuery(String sql)
            throws SQLException
    {
        String trimmedText = sql.toLowerCase().trim();
        if (trimmedText.startsWith("explain"))
        {
            return executeExplain(sql);
        }
        if (!trimmedText.startsWith("select")) {
            throw new SQLException(resources.getString("errorExecuteQuery") + sql);
        }
        String tempSql = sql;
        if (this._results != null) {
            this._results.close();
        }

        this.queryActive = false;
        this._results = null;
        this.query = null;

        if (this._maxRows > 0)
        {
            LimitInfo li = LimitInfo.parse(sql);
            if (li.hasLimit)
                li.rowCount = Math.min(li.rowCount, this._maxRows);
            else
                li.rowCount = this._maxRows;
            li.hasLimit = true;
            tempSql = StringFunc.replaceLimit(sql, li);
        }

        this._results = ((ServerConnection)this.con).executeQuery(tempSql, this._resultSetType, this);

        this.queryActive = true;
        return this._results;
    }

    public ResultSet executeExplain(String sql)
            throws SQLException
    {
        this._results = ((ServerConnection)this.con).executeExplain(sql, this);
        this.queryActive = true;
        return this._results;
    }

    public int executeUpdate(String sql)
            throws SQLException
    {
        if (sql.toLowerCase().trim().startsWith("select")) {
            throw new SQLException(resources.getString("errorExecuteUpdate") + sql);
        }
        if (sql.toLowerCase().trim().contains("create table"))
        {
            return 1;
        }
        String sqlQuery = StringFunc.verifyTerminator(sql);
        if (UnityDriver.DEBUG)
            System.out.println("Executing: " + sqlQuery);
        return ((ServerConnection)this.con).executeUpdate(sqlQuery, this);
    }

    public String getQueryString()
    {
        if (this.query == null) {
            return "";
        }
        try
        {
            return this.query.toMongoString();
        }
        catch (Exception e) {
        }
        return "";
    }

    public void setQuery(MongoQuery mq)
    {
        this.query = mq;
    }

    public GlobalQuery parseQuery(String query, boolean schemaValidation)
            throws SQLException
    {
        if ((query == null) || (query.equals(""))) {
            return null;
        }
        String sql = StringFunc.verifyTerminator(query);

        GlobalSchema schema = ((ServerConnection)this.con).getSchema();

        GlobalParser kingParser = new GlobalParser(false, schemaValidation);
        GlobalQuery gq = kingParser.parse(sql, schema);
        gq.setQueryString(sql);
        try
        {
            MongoBuilder m = new MongoBuilder(gq.getLogicalQueryTree().getRoot());
            this.query = ((MongoSelectQuery)m.toMongoQuery());
        }
        catch (MongoBuilderUpstreamException e)
        {
            return ((ServerConnection)this.con).processMongoWithUnityPrepared(sql, schema);
        }
        catch (Exception e)
        {
            return ((ServerConnection)this.con).processMongoWithUnityPrepared(sql, schema);
        }

        return gq;
    }

    public GlobalQuery translateQuery(String query, boolean schemaValidation, GlobalSchema schema)
            throws SQLException
    {
        if ((query == null) || (query.equals(""))) {
            return null;
        }
        String sql = StringFunc.verifyTerminator(query);

        GlobalParser kingParser = new GlobalParser(false, schemaValidation);
        GlobalQuery gq = kingParser.parse(sql, schema);
        gq.setQueryString(sql);
        try
        {
            MongoBuilder m = new MongoBuilder(gq.getLogicalQueryTree().getRoot());
            this.query = ((MongoSelectQuery)m.toMongoQuery());
        }
        catch (MongoBuilderUpstreamException e)
        {
            return ((ServerConnection)this.con).processMongoWithUnityPrepared(sql, schema);
        }
        catch (Exception e)
        {
            return ((ServerConnection)this.con).processMongoWithUnityPrepared(sql, schema);
        }

        return gq;
    }

    public GlobalUpdate parseUpdate(String query, boolean schemaValidation)
            throws SQLException
    {
        if ((query == null) || (query.equals(""))) {
            return null;
        }
        String sql = StringFunc.verifyTerminator(query);

        GlobalSchema schema = ((ServerConnection)this.con).getSchema();

        GlobalParser kingParser = new GlobalParser(false, schemaValidation);
        GlobalUpdate gu = kingParser.parseUpdate(sql, schema);

        return gu;
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        return execute(sql);
    }

    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException
    {
        return execute(sql, 1);
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException
    {
        return execute(sql, 1);
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        return executeUpdate(sql);
    }

    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        return executeUpdate(sql, 1);
    }

    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        return executeUpdate(sql, 1);
    }

    public ResultSet getGeneratedKeys()
            throws SQLException
    {
        String[] columns = new String[1];
        ArrayList metadata = new ArrayList(1);
        metadata.add(new SourceField(null, null, null, "_id", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 1, "YES"));
        for (int i = 0; i < 1; i++)
            columns[i] = ((SourceField)metadata.get(i)).getColumnName();
        ArrayList data = new ArrayList();

        if ((this.query != null) && ((this.query instanceof MongoInsertQuery)))
        {
            ArrayList row = new ArrayList(1);
            String key = ((MongoInsertQuery)this.query).insertFields.getString("_id");
            row.add(key);
            data.add(row);
        }
        return new LocalResultSet(data, columns, metadata);
    }
}