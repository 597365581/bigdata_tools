package mongodb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

public class MongoXADataSource extends MongoDataSource
        implements XADataSource
{
    private static final long serialVersionUID = 1L;

    public XAConnection getXAConnection()
            throws SQLException
    {
        Connection c = super.getConnection();
        MongoXAConnection xaconn = new MongoXAConnection(c);
        return xaconn;
    }

    public XAConnection getXAConnection(String user, String password)
            throws SQLException
    {
        Connection c = super.getConnection(user, password);
        MongoXAConnection xaconn = new MongoXAConnection(c);
        return xaconn;
    }
}