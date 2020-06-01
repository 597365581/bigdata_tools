package mongodb.jdbc;

import com.mongodb.util.Util;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import mongodb.conn.ServerConnection;
import unity.generic.jdbc.DriverImpl;
import unity.jdbc.UnityDriver;

public class MongoDriver extends DriverImpl
{
    protected static ResourceBundle resources = ResourceBundle.getBundle("resources/mongo/MongoDriver", locale);

    public MongoDriver()
    {
        this._MAJORVERSION = 1;
        this._MINORVERSION = 0;
    }

    public boolean acceptsURL(String url)
            throws SQLException
    {
        int position = url.toLowerCase().indexOf("jdbc:mongo");
        if (position < 0)
        {
            return false;
        }

        return true;
    }

    protected String extractDBName(String url)
    {
        int qpos = url.indexOf("?");
        if (qpos < 0) {
            qpos = url.length();
        }

        int paramPos = url.lastIndexOf(47, qpos);
        if (paramPos > 0)
        {
            return url.substring(paramPos + 1, qpos);
        }
        return "unknown";
    }

    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url))
        {
            return null;
        }

        int positionURI = url.toLowerCase().indexOf("jdbc:mongodb://");
        boolean isURI = positionURI >= 0;
        String mongoURI = null;
        ArrayList servers = new ArrayList(1);
        ArrayList ports = new ArrayList(1);
        String dbname = "admin";
        String urlWithoutParam = null;

        Properties properties = info;
        if (info == null) {
            properties = new Properties();
        }

        if (isURI)
        {
            mongoURI = url.substring(5);
            int qpos = mongoURI.indexOf("?");
            if (qpos > 0)
                mongoURI = mongoURI.substring(0, qpos);
            dbname = extractDBName(url);
            urlWithoutParam = mongoURI;

            if (qpos > 0)
            {
                String tempUrl = mongoURI;
                String params = url.substring(qpos + 6);
                StringTokenizer myTokenizer = new StringTokenizer(params, "=&;");

                while (myTokenizer.hasMoreTokens())
                {
                    String key = myTokenizer.nextToken();

                    key = key.toLowerCase();

                    if (myTokenizer.hasMoreTokens())
                    {
                        String val = myTokenizer.nextToken();
                        properties.put(key, val);

                        if (key.equals("debug"))
                        {
                            if ((val.equals("1")) || (val.equalsIgnoreCase("true"))) {
                                UnityDriver.DEBUG = true;
                            }
                        }
                        if ((key.equals("log")) &&
                                (val != null) && (!val.equals(""))) {
                            UnityDriver.initializeLog(val);
                        }
                        if (key.equals("db")) {
                            dbname = val;
                        }

                    }

                }

            }

        }
        else
        {
            int position = url.toLowerCase().indexOf("jdbc:mongo://");
            String tempUrl = url.substring(position + 13);

            urlWithoutParam = tempUrl;
            int qpos = tempUrl.indexOf("?");
            int paramPos = tempUrl.indexOf("/");
            if (paramPos > 0) {
                if (qpos > 0)
                {
                    dbname = tempUrl.substring(paramPos + 1, qpos);
                }
                else
                {
                    dbname = tempUrl.substring(paramPos + 1);
                }
                urlWithoutParam = tempUrl.substring(0, paramPos);
            }

            if (qpos > 0)
            {
                String params = tempUrl.substring(qpos + 1);
                StringTokenizer myTokenizer = new StringTokenizer(params, "=&;");

                while (myTokenizer.hasMoreTokens())
                {
                    String key = myTokenizer.nextToken();

                    key = key.toLowerCase();

                    if (myTokenizer.hasMoreTokens())
                    {
                        String val = myTokenizer.nextToken();
                        properties.put(key, val);

                        if (key.equals("debug"))
                        {
                            if ((val.equals("1")) || (val.equalsIgnoreCase("true"))) {
                                UnityDriver.DEBUG = true;
                            }
                        }
                        if ((key.equals("log")) &&
                                (val != null) && (!val.equals(""))) {
                            UnityDriver.initializeLog(val);
                        }
                    }
                }
                if (paramPos < 0) {
                    urlWithoutParam = tempUrl.substring(0, qpos);
                }

            }

            StringTokenizer tokenizer = new StringTokenizer(urlWithoutParam, ",");
            while (tokenizer.hasMoreTokens())
            {
                String server = tokenizer.nextToken();

                int port = 27017;

                int idx = server.indexOf(58);
                if (idx > 0)
                {
                    try
                    {
                        port = Integer.parseInt(server.substring(idx + 1));
                        server = server.substring(0, idx);
                    }
                    catch (Exception e)
                    {
                    }
                }
                servers.add(server);
                ports.add(Integer.valueOf(port));
            }

        }

        if (!properties.containsKey("rebuildschema")) {
            properties.put("rebuildschema", "false");
        }
        if (!properties.containsKey("validation")) {
            properties.put("validation", "flex");
        }

        String schemaLocation = properties.getProperty("schema");
        if (schemaLocation == null)
        {
            schemaLocation = "mongo_" + dbname + ".xml";
            properties.put("schema", schemaLocation);
        }

        MongoConnection hconn = new MongoConnection(urlWithoutParam, servers, ports, dbname, properties, mongoURI);
        return hconn;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        String[] keys = { "debug", "user", "password", "cursor", "encoding", "dbname", "validation", "schema", "rebuildschema", "readpref", "readparam", "writeconcern", "writeparam", "ssl", "samplesize", "log" };
        String[] values = { "false", "", "", "client", "utf-8", "admin", "flex", "", "false", "primary", "", "ack", "", "false", "", "" };
        String[][] choices = { { "true", "false" }, new String[0], new String[0], { "client", "server" }, { "utf-8" }, new String[0], { "strict", "flex", "none" }, new String[0], { "true", "false" }, { "primary", "primarypref", "secondary", "secondarypref", "nearest" }, { "JSON string that will be converted into DBObject as ReadPreference parameters." }, { "unack", "ack", "replicaAck", "journaled" }, new String[0], { "true", "false" }, { "0", "1" }, new String[0] };

        String[] description = { "The debug property will cause the driver to print out debug information to the console during its operation.", "User name for connection.", "Password for connection.", "For scrollable ResultSets, should the cursor be on the client or server.", "Character encoding used.", "Database name to use.", "Schema validation performed.  Strict validation ensures all identifiers are in schema.  Flex validation will perform best effort validation against a schema (if present) but attempt to execute query in all cases.  None will never generate or use any schema information.", "Location of schema.  Either a file URI or location in MongoDB.", "If true, rebuilds schema for connection.  If false, uses existing cached schema if available.  Uses location provided in schema property.", "Specifies a MongoDB ReadPreference such as primary, secondary, or nearest.", "Used to pass parameters to configure ReadPreference in the form of JSON strings which are converted to DBObjects.", "Write concern controls how writing and updated are handled.  Settings such as unacknowledged or acknowledged are possible.", "Used to configure a write concern based on a tag name.  Calls WriteConcern(String w) constructor with value as w parameter.", "Use SSL connections to Mongo instance.", "When building schemas fraction of collection to sample in the range of 0 to 1.  Default is 0.001.", "File location of log file to store debugging information if debug is true." };

        boolean[] required = { false, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false };

        DriverPropertyInfo[] dpinfo = new DriverPropertyInfo[keys.length];

        for (int i = 0; i < keys.length; i++)
        {
            dpinfo[i] = new DriverPropertyInfo(keys[i], values[i]);
            if ((info != null) && (info.containsKey(keys[i])))
                dpinfo[i].value = info.getProperty(keys[i]);
            dpinfo[i].description = description[i];
            dpinfo[i].choices = choices[i];
            dpinfo[i].required = required[i];
        }

        return dpinfo;
    }

    public static String _hash(String username, String password)
    {
        char[] passwd = password.toCharArray();
        ByteArrayOutputStream bout = new ByteArrayOutputStream(username.length() + 20 + passwd.length);
        try
        {
            bout.write(username.getBytes());
            bout.write(":mongo:".getBytes());
            for (int i = 0; i < passwd.length; i++)
            {
                if (passwd[i] >= '')
                    throw new IllegalArgumentException("Only ASCII passwords supported.");
                bout.write((byte)passwd[i]);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Errir building password hash.", e);
        }
        return Util.hexMD5(bout.toByteArray());
    }

    public static int getNumMongoClients()
    {
        return ServerConnection.getNumMongoClients();
    }

    static
    {
        try
        {
            DriverManager.registerDriver(new MongoDriver());
        }
        catch (SQLException e)
        {
            throw new RuntimeException(resources.getString("registerError") + e);
        }
    }
}