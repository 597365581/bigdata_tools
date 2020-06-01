package mongodb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

public class MongoPooledConnection
        implements PooledConnection
{
    protected Connection conn;

    public MongoPooledConnection(Connection c)
    {
        this.conn = c;
    }

    public void close()
            throws SQLException
    {
        this.conn.close();
    }

    public Connection getConnection() throws SQLException
    {
        return this.conn;
    }

    public void addConnectionEventListener(ConnectionEventListener listener)
    {
    }

    public void removeConnectionEventListener(ConnectionEventListener listener)
    {
    }

    public void addStatementEventListener(StatementEventListener listener)
    {
    }

    public void removeStatementEventListener(StatementEventListener listener)
    {
    }
}