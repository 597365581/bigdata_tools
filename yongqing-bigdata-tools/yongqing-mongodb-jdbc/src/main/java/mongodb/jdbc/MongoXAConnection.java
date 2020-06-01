package mongodb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

public class MongoXAConnection extends MongoPooledConnection
        implements XAConnection
{
    private XAResource resource;

    public MongoXAConnection(Connection c)
    {
        super(c);
        this.resource = new MongoXAResource(this);
    }

    public XAResource getXAResource()
            throws SQLException
    {
        return this.resource;
    }
}