package mongodb.jdbc;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class MongoXAResource
        implements XAResource
{
    private MongoXAConnection connection;
    public static final int DEFAULT_TIMEOUT = 100;
    private int transactionTimeout = 100;

    public MongoXAResource(MongoXAConnection conn)
    {
        this.connection = conn;
    }

    public void commit(Xid xid, boolean onePhase)
            throws XAException
    {
    }

    public void end(Xid xid, int flags)
            throws XAException
    {
    }

    public void forget(Xid xid)
            throws XAException
    {
    }

    public int getTransactionTimeout()
            throws XAException
    {
        return this.transactionTimeout;
    }

    public boolean isSameRM(XAResource xares)
            throws XAException
    {
        return false;
    }

    public int prepare(Xid xid)
            throws XAException
    {
        return 0;
    }

    public Xid[] recover(int flag)
            throws XAException
    {
        return null;
    }

    public void rollback(Xid xid)
            throws XAException
    {
    }

    public boolean setTransactionTimeout(int seconds)
            throws XAException
    {
        if (seconds == 0)
        {
            this.transactionTimeout = 100;
        }
        this.transactionTimeout = seconds;
        return true;
    }

    public void start(Xid xid, int flags)
            throws XAException
    {
    }
}