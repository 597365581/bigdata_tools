package mongodb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ResourceBundle;
import mongodb.conn.ServerConnection;
import unity.engine.Attribute;
import unity.engine.IServerConnection;
import unity.query.GlobalCommand;
import unity.query.GlobalQuery;
import unity.query.GlobalUpdate;
import unity.query.LQTree;
import unity.query.Optimizer;
import unity.util.Convert;
import unity.util.StringFunc;

public class MongoPreparedStatement extends MongoStatement
        implements PreparedStatement
{
    protected static ResourceBundle resources = ResourceBundle.getBundle("resources/mongo/MongoPreparedStatement", locale);
    protected String originalQueryString;
    protected boolean hasBlob = false;
    protected ArrayList<Object> streams;
    protected GlobalCommand command;
    protected boolean isExplain = false;

    public MongoPreparedStatement(MongoConnection cnt, IServerConnection con, int resultSetType, int resultSetConcurrency, String orgQuerySt)
            throws SQLException
    {
        super(cnt, con, resultSetType, resultSetConcurrency);

        this.originalQueryString = orgQuerySt;
        this.streams = new ArrayList();

        String lowerquery = orgQuerySt.toLowerCase().trim();
        if (lowerquery.startsWith("select")) {
            this.command = parseQuery(orgQuerySt, false);
        } else if (lowerquery.startsWith("explain"))
        {
            this.isExplain = true;
            this.command = parseQuery(orgQuerySt.substring(8), false);
        }
        else {
            this.command = parseUpdate(orgQuerySt, false);
        }
    }

    public void addBatch()
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void clearParameters()
            throws SQLException
    {
        this.command.clearParameters();
    }

    private String combineString()
    {
        String executeQueryString = this.originalQueryString;
        if (this.command != null)
        {
            if ((this.command instanceof GlobalQuery)) {
                return Optimizer.buildSQL(((GlobalQuery)this.command).getLogicalQueryTree().getRoot());
            }
            return ((GlobalUpdate)this.command).getStatement();
        }

        return executeQueryString;
    }

    public boolean execute()
            throws SQLException
    {
        if (this.command != null)
        {
            if ((this.command instanceof GlobalQuery)) {
                executeQuery();
                return true;
            }

            executeUpdate();
            return false;
        }

        return false;
    }

    public ResultSet executeQuery()
            throws SQLException
    {
        if (this.isExplain) {
            this._results = ((ServerConnection)this.con).executeExplain((GlobalQuery)this.command, this);
        }
        else
        {
            this._results = ((ServerConnection)this.con).executePreparedQuery((GlobalQuery)this.command, this._resultSetType, this, combineString());
        }
        return this._results;
    }

    public int executeUpdate()
            throws SQLException
    {
        int count = ((ServerConnection)this.con).executeUpdate((GlobalUpdate)this.command, this);
        return count;
    }

    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        ResultSet rst = executeQuery();
        return rst.getMetaData();
    }

    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        return null;
    }

    public String getSQLStatement()
            throws SQLException
    {
        return combineString();
    }

    public ArrayList<Object> getStreams()
    {
        return this.streams;
    }

    public boolean isHasBlob()
    {
        return this.hasBlob;
    }

    public void setArray(int index, Array x)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setAsciiStream(int index, InputStream stream, int arg2)
            throws SQLException
    {
        if (stream == null)
        {
            setNull(index, -3);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, stream, -3);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setBigDecimal(int index, BigDecimal x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, x, 3);
    }

    public void setBinaryStream(int index, InputStream stream)
            throws SQLException
    {
        setHasBlob(true);
        if (stream == null)
        {
            setNull(index, -3);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, stream, -3);
    }

    public void setBinaryStream(int index, InputStream stream, int arg2)
            throws SQLException
    {
        if (stream == null)
        {
            setNull(index, -3);
            return;
        }

        this.hasBlob = true;
        if (this.command != null)
            this.command.setParameter(index, stream, -3);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setBlob(int index, Blob value)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setBoolean(int index, boolean x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Boolean.valueOf(x), 16);
    }

    public void setByte(int index, byte x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Byte.valueOf(x), 1002);
    }

    public void setBytes(int index, byte[] x)
            throws SQLException
    {
        if (x == null)
        {
            setNull(index, -2);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, x, -2);
    }

    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setCharacterStream(int index, Reader value, int offset)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setClob(int index, Clob value)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setDate(int index, java.sql.Date x)
            throws SQLException
    {
        if (x == null)
        {
            setNull(index, 91);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, x, 91);
    }

    public void setDate(int index, java.sql.Date x, Calendar cal)
            throws SQLException
    {
        setDate(index, x);
    }

    public void setDouble(int index, double x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Double.valueOf(x), 8);
    }

    public void setFloat(int index, float x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Float.valueOf(x), 6);
    }

    public void setHasBlob(boolean hasBlob)
    {
        this.hasBlob = hasBlob;
    }

    public void setInt(int index, int x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Integer.valueOf(x), 4);
    }

    public void setLong(int index, long x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Long.valueOf(x), -5);
    }

    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setNull(int index, int sqlType)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameterNull(index, sqlType);
    }

    public void setNull(int index, int sqlType, String typeName)
            throws SQLException
    {
        setNull(index, sqlType);
    }

    public void setObject(int index, Object obj)
            throws SQLException
    {
        if (obj == null)
        {
            setNull(index, 1111);
            return;
        }

        if ((obj instanceof Boolean))
            setBoolean(index, ((Boolean)obj).booleanValue());
        else if ((obj instanceof Byte))
            setByte(index, ((Byte)obj).byteValue());
        else if ((obj instanceof Short))
            setShort(index, ((Short)obj).shortValue());
        else if ((obj instanceof Integer))
            setInt(index, ((Integer)obj).intValue());
        else if ((obj instanceof Long))
            setLong(index, ((Long)obj).longValue());
        else if ((obj instanceof Float))
            setFloat(index, ((Float)obj).floatValue());
        else if ((obj instanceof Double))
            setDouble(index, ((Double)obj).doubleValue());
        else if ((obj instanceof BigDecimal))
            setBigDecimal(index, (BigDecimal)obj);
        else if ((obj instanceof String))
            setString(index, (String)obj);
        else if ((obj instanceof byte[])) {
            setBytes(index, (byte[])obj);
        }
        else if ((obj instanceof java.sql.Date)) {
            setDate(index, (java.sql.Date)obj);
        }
        else if ((obj instanceof Time)) {
            setTime(index, (Time)obj);
        }
        else if ((obj instanceof Timestamp)) {
            setTimestamp(index, (Timestamp)obj);
        }
        else if ((obj instanceof java.util.Date)) {
            setDate(index, new java.sql.Date(((java.util.Date)obj).getTime()));
        }
        else if ((obj instanceof Serializable))
        {
            setSerializableObject(index, obj);
        }
    }

    public void setObject(int index, Object obj, int targetSqlType)
            throws SQLException
    {
        if (obj == null)
        {
            setNull(index, targetSqlType);
            return;
        }

        int type = Attribute.getType(obj);
        if (type == targetSqlType)
        {
            setObject(index, obj);
        }
        else
        {
            try
            {
                Object newObj = Convert.convertObject(obj, targetSqlType);
                setObject(index, newObj);
            }
            catch (Exception e)
            {
                throw new SQLException(resources.getString("objectConvertError") + " Exception: " + e);
            }
        }
    }

    public void setObject(int index, Object obj, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        setObject(index, obj, targetSqlType);
    }

    public void setRef(int index, Ref value)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    private void setSerializableObject(int index, Object obj)
            throws SQLException
    {
        throw new SQLException(resources.getString("serializationError"));
    }

    public void setShort(int index, short x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, Short.valueOf(x), 5);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLException(resources.getString("notsupportedError"));
    }

    public void setString(int index, String x)
            throws SQLException
    {
        if (x == null)
        {
            setNull(index, 12);
            return;
        }

        String escapedString = StringFunc.safeMongo(x);
        if (this.command != null)
            this.command.setParameter(index, escapedString, 12);
    }

    public void setStringNQ(int index, String x)
            throws SQLException
    {
        if (this.command != null)
            this.command.setParameter(index, x, 1001);
    }

    public void setTime(int index, Time x)
            throws SQLException
    {
        if (x == null)
        {
            setNull(index, 92);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, x, 92);
    }

    public void setTime(int index, Time x, Calendar cal)
            throws SQLException
    {
        setTime(index, x);
    }

    public void setTimestamp(int index, Timestamp x)
            throws SQLException
    {
        if (x == null)
        {
            setNull(index, 93);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, x, 93);
    }

    public void setTimestamp(int index, Timestamp x, Calendar cal)
            throws SQLException
    {
        setTimestamp(index, x);
    }

    @Deprecated
    public void setUnicodeStream(int index, InputStream stream, int arg2)
            throws SQLException
    {
        if (stream == null)
        {
            setNull(index, -3);
            return;
        }

        if (this.command != null)
            this.command.setParameter(index, stream, -3);
    }

    public void setURL(int index, URL x)
            throws SQLException
    {
        setString(index, x.toString());
    }
}