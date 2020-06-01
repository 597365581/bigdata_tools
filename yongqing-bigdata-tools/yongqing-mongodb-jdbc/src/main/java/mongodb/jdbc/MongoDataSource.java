package mongodb.jdbc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;

public class MongoDataSource
        implements DataSource, Referenceable, Serializable, ConnectionPoolDataSource
{
    protected static MongoDriver driver = null;
    private static final long serialVersionUID = 1L;
    protected PrintWriter logWriter = null;

    protected String encoding = null;

    protected String url = null;

    protected int logintimeout = 0;

    protected boolean pooled = true;
    protected Properties props;

    public MongoDataSource()
    {
        this.props = new Properties();
        this.props.setProperty("password", "");
        this.props.setProperty("loginTimeout", String.valueOf(getLoginTimeout()));
    }

    public String getUrl()
    {
        return this.props.getProperty("url");
    }

    public void setUrl(String url)
    {
        this.props.setProperty("url", url);
    }

    public String getUser()
    {
        return this.props.getProperty("user");
    }

    public void setUser(String user)
    {
        this.props.setProperty("user", user);
    }

    public String getPassword()
    {
        return this.props.getProperty("password");
    }

    public void setPassword(String password)
    {
        this.props.setProperty("password", password);
    }

    public String getProperty(String name)
    {
        return this.props.getProperty(name);
    }

    public void setProperty(String name, String value)
    {
        this.props.setProperty(name, value);
    }

    public PrintWriter getLogWriter()
    {
        return this.logWriter;
    }

    public void setLogWriter(PrintWriter pw)
    {
        this.logWriter = pw;
    }

    public Reference getReference()
            throws NamingException
    {
        Reference ref = new Reference(getClass().getName(), "mongodb.jdbc.MongoDataSourceFactory", null);
        ref.add(new StringRefAddr("url", getUrl()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", getPassword()));
        return ref;
    }

    public Connection getConnection(String userID, String pass)
            throws SQLException
    {
        this.props.setProperty("user", userID);
        this.props.setProperty("password", pass);
        return getConnection(this.props);
    }

    public Connection getConnection(Properties props)
            throws SQLException
    {
        return driver.connect(props.getProperty("url"), props);
    }

    public Connection getConnection()
            throws SQLException
    {
        return driver.connect(this.props.getProperty("url"), this.props);
    }

    public void setLoginTimeout(int seconds)
            throws SQLException
    {
        this.logintimeout = seconds;
    }

    public int getLoginTimeout()
    {
        return this.logintimeout;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        try
        {
            return iface.cast(this);
        }
        catch (ClassCastException e) {
        }
        throw new SQLException("ERROR: Failed to wrap to " + iface.toString());
    }

    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("Feature not supported: getParentLogger()");
    }

    public PooledConnection getPooledConnection(Properties props)
            throws SQLException
    {
        return new MongoPooledConnection(driver.connect(props.getProperty("url"), props));
    }

    public PooledConnection getPooledConnection()
            throws SQLException
    {
        return new MongoPooledConnection(driver.connect(this.props.getProperty("url"), this.props));
    }

    public PooledConnection getPooledConnection(String user, String password)
            throws SQLException
    {
        this.props.put("user", user);
        this.props.put("password", password);
        return new MongoPooledConnection(driver.connect(this.props.getProperty("url"), this.props));
    }

    static
    {
        try
        {
            driver = (MongoDriver)Class.forName("mongodb.jdbc.MongoDriver").newInstance();
        }
        catch (Exception E)
        {
            throw new RuntimeException("Unable to load Mongo driver.");
        }
    }
}