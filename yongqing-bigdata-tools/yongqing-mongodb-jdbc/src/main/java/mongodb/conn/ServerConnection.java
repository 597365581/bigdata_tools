//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package mongodb.conn;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.util.JSON;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSocketFactory;
import mongodb.jdbc.MongoStatement;
import mongodb.query.MongoBuilder;
import mongodb.query.MongoBuilderUpstreamException;
import mongodb.query.MongoQuery;
import unity.annotation.AnnotatedSourceDatabase;
import unity.annotation.AnnotatedSourceField;
import unity.annotation.AnnotatedSourceKey;
import unity.annotation.AnnotatedSourceTable;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceDatabase;
import unity.annotation.SourceField;
import unity.annotation.SourceKey;
import unity.annotation.SourceTable;
import unity.engine.Attribute;
import unity.engine.IServerConnection;
import unity.engine.Tuple;
import unity.generic.jdbc.StatementImpl;
import unity.io.FileManager;
import unity.jdbc.LocalResultSet;
import unity.jdbc.UnityConnection;
import unity.jdbc.UnityDriver;
import unity.jdbc.UnityStatement;
import unity.operators.ResultSetScan;
import unity.parser.GlobalParser;
import unity.query.Evaluator;
import unity.query.GQTableRef;
import unity.query.GlobalCommand;
import unity.query.GlobalQuery;
import unity.query.GlobalUpdate;
import unity.query.LQNode;
import unity.query.LQProjNode;
import unity.query.LQTree;
import unity.query.LocalQuery;
import unity.util.StringFunc;

public class ServerConnection implements IServerConnection {
    public static final int MAX_STRING_SIZE = 16793600;
    protected static ResourceBundle resources;
    protected static final Locale locale = Locale.getDefault();
    private String url;
    private String mongoURI;
    private ArrayList<ServerAddress> servers;
    private int fetchSize;
    private HashMap<Long, MongoExecutor> executors;
    private String userName;
    private String password;
    private Properties properties;
    private static HashMap<String, MongoClientConnection> mongoClients;
    private static final Lock lock;
    private DB db;
    private String databaseName;
    private UnityConnection uconn;
    private SourceDatabase database;
    private GlobalSchema schema;
    private String validation;
    private String schemaLocation;
    private Connection connection;
    private boolean useSSL;

    public ServerConnection(ArrayList<String> servers, ArrayList<Integer> ports, String dbname, String url, Connection connection, String mongoURI) throws SQLException {
        this.mongoURI = mongoURI;
        this.servers = new ArrayList(servers.size());

        for(int i = 0; i < servers.size(); ++i) {
            try {
                this.servers.add(new ServerAddress((String)servers.get(i), ((Integer)ports.get(i)).intValue()));
            } catch (Exception var9) {
                throw new SQLException(var9.getMessage());
            }
        }

        this.url = url;
        this.databaseName = dbname;
        this.uconn = null;
        this.connection = connection;
        this.executors = new HashMap();
    }

    private static MongoClient getConnection(String url, boolean useSSL, Properties info, String dbname) throws UnknownHostException, SQLException {
        ArrayList<ServerAddress> servers = new ArrayList(1);
        servers.add(new ServerAddress(url));
        return getConnection(servers, useSSL, info, dbname);
    }

    private static MongoClient getConnection(ArrayList<ServerAddress> servers, boolean useSSL, Properties info, String dbName) throws UnknownHostException, SQLException {
        MongoClient mongoClient = null;
        String key = servers.toString();
        if(useSSL) {
            UnityDriver.debug("Creating new SQL connection with SSL: " + key);
            key = key + "ssl";
        } else {
            UnityDriver.debug("Creating new SQL connection: " + key);
        }

        boolean reused = false;
        lock.lock();
        boolean useCredential = false;
        MongoCredential credential = null;
        if(info.getProperty("user") != null && !info.getProperty("user").equals("")) {
            useCredential = true;
            String user = info.getProperty("user");
            String password = info.getProperty("password");
            if(password == null) {
                password = "";
            }

            String dbname = info.getProperty("dbname");
            if(dbname == null) {
                dbname = "admin";
            }

            String authdb = info.getProperty("authdb");
            if(authdb == null) {
                authdb = dbname;
            }

            String authMechanism = null;
            String authSource = null;
            authMechanism = info.getProperty("authMechanism");
            if(authMechanism == null) {
                authMechanism = info.getProperty("authmechanism");
            }

            authSource = info.getProperty("authSource");
            if(authSource == null) {
                authSource = info.getProperty("authsource");
            }

            if(authMechanism == null) {
                credential = MongoCredential.createCredential(user, authdb, password.toCharArray());
            } else if(authMechanism.toUpperCase().contains("SCRAM")) {
                UnityDriver.debug("Connecting with SCRAM-SHA-1 authentication.");
                credential = MongoCredential.createScramSha1Credential(user, authdb, password.toCharArray());
            } else if(authMechanism.toUpperCase().contains("MONGODB-CR")) {
                UnityDriver.debug("Connecting with MONGODB-CR authentication.");
                credential = MongoCredential.createMongoCRCredential(user, authdb, password.toCharArray());
            } else if(authMechanism.toUpperCase().contains("509")) {
                UnityDriver.debug("Connecting with X.509 authentication.");
                credential = MongoCredential.createMongoX509Credential(user);
            } else if(authMechanism.toUpperCase().contains("GSSAPI")) {
                UnityDriver.debug("Connecting with GSSAPI authentication.");
                credential = MongoCredential.createGSSAPICredential(user);
            } else if(authMechanism.toUpperCase().contains("PLAIN")) {
                UnityDriver.debug("Connecting with LDAP (PLAIN) authentication.");
                credential = MongoCredential.createPlainCredential(user, authSource, password.toCharArray());
            } else {
                UnityDriver.debug("Unrecognized authentication: " + authMechanism + ".  Using default.");
                credential = MongoCredential.createCredential(user, authdb, password.toCharArray());
            }
        }

        try {
            MongoClientConnection con = (MongoClientConnection)mongoClients.get(key);
            if(con != null) {
                ++con.count;
                mongoClient = con.client;
                reused = true;
            } else {
                reused = false;
                MongoClientOptions o;
                if(useSSL) {
                    o = (new Builder()).socketFactory(SSLSocketFactory.getDefault()).build();
                    if(servers.size() == 1) {
                        if(!useCredential) {
                            mongoClient = new MongoClient((ServerAddress)servers.get(0), o);
                        } else {
                            mongoClient = new MongoClient((ServerAddress)servers.get(0), Arrays.asList(new MongoCredential[]{credential}), o);
                        }
                    } else if(!useCredential) {
                        mongoClient = new MongoClient(servers, o);
                    } else {
                        mongoClient = new MongoClient(servers, Arrays.asList(new MongoCredential[]{credential}), o);
                    }
                } else {
                    o = (new Builder()).connectTimeout(500).build();
                    if(servers.size() == 1) {
                        if(!useCredential) {
                            mongoClient = new MongoClient((ServerAddress)servers.get(0), o);
                        } else {
                            mongoClient = new MongoClient((ServerAddress)servers.get(0), Arrays.asList(new MongoCredential[]{credential}), o);
                        }
                    } else if(!useCredential) {
                        mongoClient = new MongoClient((ServerAddress)servers.get(0), o);
                    } else {
                        mongoClient = new MongoClient((ServerAddress)servers.get(0), Arrays.asList(new MongoCredential[]{credential}), o);
                    }
                }

                DB db = mongoClient.getDB(dbName);
                db.getStats();
                con = new MongoClientConnection();
                con.count = 1;
                con.client = mongoClient;
                mongoClients.put(key, con);
            }
        } catch (Exception var19) {
            throw new UnknownHostException(var19.toString());
        } finally {
            lock.unlock();
        }

        if(!reused) {
            UnityDriver.debug("Created new MongoClient connection for URL: " + key);
        }

        return mongoClient;
    }

    private static MongoClient getConnection(String mongoURI) throws SQLException {
        MongoClient mongoClient = null;
        boolean reused = false;
        String key = mongoURI;
        lock.lock();

        try {
            MongoClientConnection con = (MongoClientConnection)mongoClients.get(key);
            if(con != null) {
                ++con.count;
                mongoClient = con.client;
                reused = true;
            } else {
                reused = false;
                mongoClient = new MongoClient(new MongoClientURI(mongoURI));
                con = new MongoClientConnection();
                con.count = 1;
                con.client = mongoClient;
                mongoClients.put(key, con);
            }
        } catch (Exception var9) {
            throw new SQLException(var9.toString());
        } finally {
            lock.unlock();
        }

        if(!reused) {
            UnityDriver.debug("Created new MongoClient connection for URL: " + mongoURI);
        }

        return mongoClient;
    }

    public static void addObjectToTagSet(DBObject obj, List<Tag> ltag) {
        Iterator i$ = obj.keySet().iterator();

        while(i$.hasNext()) {
            String k = (String)i$.next();
            Object val = obj.get(k);
            if(val instanceof DBObject) {
                addObjectToTagSet((DBObject)val, ltag);
            } else {
                ltag.add(new Tag(k, val.toString()));
            }
        }

    }

    public static TagSet convertObjectToTagSet(DBObject obj) {
        List<Tag> ltag = new ArrayList();
        if(obj != null) {
            addObjectToTagSet(obj, ltag);
            TagSet tset = new TagSet(ltag);
            return tset;
        } else {
            return new TagSet();
        }
    }

    public void connect(Properties info) throws SQLException {
        MongoClient mongoClient = null;

        try {
            this.properties = info;
            UnityDriver.debug("Properties: " + this.properties);
            if(this.mongoURI != null) {
                mongoClient = getConnection(this.mongoURI);
                this.db = mongoClient.getDB(this.databaseName);
            } else {
                String readparam;
                try {
                    readparam = info.getProperty("ssl");
                    this.useSSL = false;
                    if(readparam != null && (readparam.equals("true") || readparam.equals("1"))) {
                        this.useSSL = true;
                    }

                    mongoClient = getConnection(this.servers, this.useSSL, info, this.databaseName);
                } catch (UnknownHostException var10) {
                    throw new SQLException(var10);
                }

                this.db = mongoClient.getDB(this.databaseName);
                readparam = info.getProperty("readparam");
                DBObject rParam = null;
                if(readparam != null) {
                    try {
                        rParam = (DBObject)JSON.parse(readparam);
                    } catch (Exception var9) {
                        throw new SQLException("Invalid JSON value for read parameter: " + readparam + " JSON conversion error: " + var9.getMessage());
                    }
                }

                String readpref = info.getProperty("readpref");
                if(readpref != null) {
                    ReadPreference rpref = null;
                    if(!readpref.equalsIgnoreCase("0") && !readpref.equalsIgnoreCase("primary")) {
                        TagSet tset;
                        if(!readpref.equalsIgnoreCase("1") && !readpref.equalsIgnoreCase("primarypref")) {
                            if(!readpref.equalsIgnoreCase("2") && !readpref.equalsIgnoreCase("secondary")) {
                                if(!readpref.equalsIgnoreCase("3") && !readpref.equalsIgnoreCase("secondarypref")) {
                                    if(readpref.equalsIgnoreCase("4") || readpref.equalsIgnoreCase("nearest")) {
                                        if(rParam == null) {
                                            rpref = ReadPreference.nearest();
                                        } else {
                                            tset = convertObjectToTagSet(rParam);
                                            if(tset == null) {
                                                rpref = ReadPreference.nearest();
                                            } else {
                                                rpref = ReadPreference.nearest(tset);
                                            }
                                        }
                                    }
                                } else if(rParam == null) {
                                    rpref = ReadPreference.secondaryPreferred();
                                } else {
                                    tset = convertObjectToTagSet(rParam);
                                    if(tset == null) {
                                        rpref = ReadPreference.secondaryPreferred();
                                    } else {
                                        rpref = ReadPreference.secondaryPreferred(tset);
                                    }
                                }
                            } else if(rParam == null) {
                                rpref = ReadPreference.secondary();
                            } else {
                                tset = convertObjectToTagSet(rParam);
                                if(tset == null) {
                                    rpref = ReadPreference.secondary();
                                } else {
                                    rpref = ReadPreference.secondary(tset);
                                }
                            }
                        } else if(rParam == null) {
                            rpref = ReadPreference.primaryPreferred();
                        } else {
                            tset = convertObjectToTagSet(rParam);
                            if(tset == null) {
                                rpref = ReadPreference.primaryPreferred();
                            } else {
                                rpref = ReadPreference.primaryPreferred(tset);
                            }
                        }
                    } else {
                        rpref = ReadPreference.primary();
                    }

                    if(rpref != null) {
                        this.db.setReadPreference((ReadPreference)rpref);
                        if(UnityDriver.DEBUG) {
                            System.out.println("MongoDB read preference: " + this.db.getReadPreference());
                        }
                    }
                }

                String writeparam = info.getProperty("writeparam");
                String writeconcern = info.getProperty("writeconcern");
                WriteConcern wcon = WriteConcern.ACKNOWLEDGED;
                if(writeparam != null) {
                    wcon = new WriteConcern(writeparam);
                } else if(writeconcern != null) {
                    if(!writeconcern.equalsIgnoreCase("0") && !writeconcern.equalsIgnoreCase("unack")) {
                        if(!writeconcern.equalsIgnoreCase("1") && !writeconcern.equalsIgnoreCase("ack")) {
                            if(!writeconcern.equalsIgnoreCase("2") && !writeconcern.equalsIgnoreCase("replicaAck")) {
                                if(writeconcern.equalsIgnoreCase("3") || writeconcern.contains("journal")) {
                                    wcon = WriteConcern.JOURNALED;
                                }
                            } else {
                                wcon = WriteConcern.REPLICA_ACKNOWLEDGED;
                            }
                        } else {
                            wcon = WriteConcern.ACKNOWLEDGED;
                        }
                    } else {
                        wcon = WriteConcern.UNACKNOWLEDGED;
                    }
                }

                if(wcon != null) {
                    this.db.setWriteConcern(wcon);
                    if(UnityDriver.DEBUG) {
                        System.out.println("MongoDB write concern: " + this.db.getWriteConcern());
                    }
                }
            }

            Object rebuild = this.properties.get("rebuildschema");
            this.schemaLocation = (String)this.properties.get("schema");
            boolean rebuilt = false;
            if(rebuild != null && rebuild.toString().equalsIgnoreCase("true")) {
                this.buildSchema();
                this.saveSchema(this.schemaLocation);
                rebuilt = true;
            }

            Object validation = this.properties.get("validation");
            this.validation = (String)validation;
            if(validation != null && !rebuilt) {
                if(validation.toString().equalsIgnoreCase("strict")) {
                    if(!this.loadSchema(this.schemaLocation)) {
                        this.buildSchema();
                        this.saveSchema(this.schemaLocation);
                    }
                } else if(validation.toString().equalsIgnoreCase("flex")) {
                    this.loadSchema(this.schemaLocation);
                }
            }

        } catch (SQLException var11) {
            throw new SQLException(var11);
        }
    }

    public void saveSchema(String location) throws SQLException {
        if(location.contains("mongo:")) {
            HashMap<String, String> items = this.parseMongoLocation(location);
            String url = (String)items.get("url");
            String dbName = (String)items.get("dbname");
            String collectionName = (String)items.get("collection");
            Properties info = new Properties();
            if(this.userName != null) {
                info.put("user", this.userName);
                info.put("password", this.password);
                info.put("dbName", dbName);
            }

            MongoClient mongoClient = null;

            try {
                mongoClient = getConnection(url, this.useSSL, info, dbName);
                DB db = mongoClient.getDB(dbName);
                DBCollection collection = db.getCollection(collectionName);
                DBObject q = new BasicDBObject();
                BasicDBObject o = new BasicDBObject();
                o.append("schema", this.database.exportXML());
                collection.update(q, o, true, false);
            } catch (UnknownHostException var17) {
                throw new SQLException("Error while saving schema: " + var17);
            } finally {
                if(mongoClient != null) {
                    closeConnection("[" + url + "]", this.useSSL);
                }

            }
        } else {
            try {
                File f = new File(location);
                this.database.exportXML(f);
            } catch (IOException var16) {
                throw new SQLException("Error while saving schema to file: " + var16);
            }
        }

    }

    private HashMap<String, String> parseMongoLocation(String location) throws SQLException {
        HashMap<String, String> output = new HashMap();
        String loc = location.substring(6);
        int pos = loc.indexOf("/");
        if(pos < 0) {
            throw new SQLException("Invalid Mongo URL (bad URL): " + location);
        } else {
            String url = loc.substring(0, pos);
            output.put("url", url);
            loc = loc.substring(pos + 1);
            pos = loc.indexOf("/");
            if(pos < 0) {
                throw new SQLException("Invalid Mongo URL (expecting database name): " + location);
            } else {
                String dbname = loc.substring(0, pos);
                output.put("dbname", dbname);
                loc = loc.substring(pos + 1);
                output.put("collection", loc);
                return output;
            }
        }
    }

    public boolean loadSchema(String location) throws SQLException {
        if(!location.contains("mongo:")) {
            try {
                InputStream is = FileManager.openInputFile(location);
                if(this.schema == null) {
                    this.schema = new GlobalSchema();
                }

                this.database = this.schema.importSchema(is);
                this.schema.addDatabase(this.database);
                return true;
            } catch (Exception var17) {
                return false;
            }
        } else {
            HashMap<String, String> items = this.parseMongoLocation(location);
            String url = (String)items.get("url");
            String dbName = (String)items.get("dbname");
            String collectionName = (String)items.get("collection");
            Properties info = new Properties();
            if(this.userName != null) {
                info.put("user", this.userName);
                info.put("password", this.password);
                info.put("dbName", dbName);
            }

            MongoClient mongoClient = null;

            boolean var12;
            try {
                mongoClient = getConnection(url, this.useSSL, info, dbName);
                this.db = mongoClient.getDB(dbName);
                DBCollection collection = this.db.getCollection(collectionName);
                DBCursor cursor = collection.find();
                if(!cursor.hasNext()) {
                    boolean var21 = false;
                    return var21;
                }

                DBObject o = cursor.next();
                String schemaXML = (String)o.get("schema");
                if(this.schema == null) {
                    this.schema = new GlobalSchema();
                }

                this.database = this.schema.importSchema(new ByteArrayInputStream(schemaXML.getBytes()));
                this.schema.addDatabase(this.database);
                var12 = true;
            } catch (UnknownHostException var18) {
                throw new SQLException("Error while saving schema: " + var18);
            } finally {
                if(mongoClient != null) {
                    closeConnection("[" + url + "]", this.useSSL);
                }

            }

            return var12;
        }
    }

    public ResultSet getMoreResults(int resultSetType, MongoStatement stmt) throws SQLException {
        try {
            return null;
        } catch (Exception var4) {
            throw new SQLException(var4);
        }
    }

    private ResultSet processMongoWithUnity(String sql, GlobalSchema gs) throws SQLException {
        UnityDriver.debug("Processing query in UnityJDBC not handled by MongoDB.");
        this.setupUnityForQuery(gs);
        UnityStatement stmt = (UnityStatement)this.uconn.createStatement();
        GlobalQuery gq = stmt.parseQuery(sql, true);
        return stmt.executeQuery(gq);
    }

    private void setupUnityForQuery(GlobalSchema gs) throws SQLException {
        if(this.uconn == null) {
            try {
                Class.forName("unity.jdbc.UnityDriver");
            } catch (ClassNotFoundException var3) {
                throw new SQLException(var3);
            }

            Properties info = new Properties();
            this.uconn = (UnityConnection)DriverManager.getConnection("jdbc:unity://virtual", info);
        }

        if(gs == null) {
            throw new SQLException(resources.getString("noSchema"));
        } else {
            this.uconn.setGlobalSchema(gs);
            SourceDatabase db = gs.getDB(this.databaseName);
            if(db == null) {
                throw new SQLException("ERROR: No database defined: " + this.databaseName);
            } else {
                db.setProperty("usecatalog", "false");
                db.setProperty("useschema", "false");
                this.uconn.setConnection(this.databaseName, this.connection);
            }
        }
    }

    public GlobalQuery processMongoWithUnityPrepared(String sql, GlobalSchema gs) throws SQLException {
        UnityDriver.debug("Processing PreparedStatement query in UnityJDBC not handled by MongoDB.");
        this.setupUnityForQuery(gs);
        UnityStatement stmt = (UnityStatement)this.uconn.createStatement();
        GlobalQuery gq = stmt.parseQuery(sql, true);
        return gq;
    }

    protected synchronized MongoExecutor createExecutor(long stmtId) {
        MongoExecutor exec = (MongoExecutor)this.executors.get(Long.valueOf(stmtId));
        if(exec != null) {
            exec.close();
            this.removeExecutor(stmtId);
        }

        exec = new MongoExecutor(this.db, stmtId);
        this.executors.put(Long.valueOf(stmtId), exec);
        return exec;
    }

    protected void removeExecutor(long stmtId) {
        this.executors.remove(Long.valueOf(stmtId));
    }

    public ResultSet executeQuery(String sql, int resultSetType, MongoStatement stmt) throws SQLException {
        UnityDriver.debug("executeQuery for URL: " + this.url + " Query: " + sql);
        if(sql.trim().equals("SELECT 1")) {
            ArrayList<ArrayList<Object>> data = new ArrayList();
            ArrayList<Object> row = new ArrayList(1);
            row.add(Integer.valueOf(1));
            data.add(row);
            ArrayList<SourceField> metadata = new ArrayList(1);
            metadata.add(new SourceField((String)null, (String)null, (String)null, "Field1", 4, "INT", 4, 0, 0, 0, "", (String)null, 0, 1, "YES"));
            return new LocalResultSet(data, new String[]{"Field1"}, metadata);
        } else {
            MongoExecutor exec = this.createExecutor(stmt.getStatementId());
            boolean deleteExecutor = true;

            ResultSet var7;
            try {
                ResultSet rst = exec.execute(sql, resultSetType, stmt, this.schema, this);
                deleteExecutor = false;
                var7 = rst;
                return var7;
            } catch (MongoBuilderUpstreamException var13) {
                if(stmt.getStatus() == StatementImpl.UNITY_PROMOTED_QUERY) {
                    throw new SQLException(var13.getMessage());
                }

                var7 = this.processMongoWithUnity(sql, this.schema);
            } catch (SQLException var14) {
                if(stmt.getStatus() == StatementImpl.UNITY_PROMOTED_QUERY) {
                    throw var14;
                }

                var7 = this.processMongoWithUnity(sql, this.schema);
                return var7;
            } catch (Exception var15) {
                throw new SQLException(var15);
            } finally {
                if(deleteExecutor) {
                    this.removeExecutor(stmt.getStatementId());
                }

            }

            return var7;
        }
    }

    public ResultSet executeExplain(String sql, MongoStatement stmt) throws SQLException {
        try {
            String sqlExplain = sql.substring(8) + ";";
            if(sqlExplain.toLowerCase().trim().startsWith("select")) {
                GlobalQuery gq = null;

                GlobalParser kingParser;
                try {
                    kingParser = new GlobalParser(false, true);
                    gq = kingParser.parse(sqlExplain, this.schema);
                } catch (Exception var6) {
                    ;
                }

                if(gq == null) {
                    kingParser = new GlobalParser(false, false);
                    gq = kingParser.parse(sqlExplain, this.schema);
                }

                return this.executeExplain(gq, stmt);
            } else {
                throw new SQLException("EXPLAIN only supported for SELECT.");
            }
        } catch (Exception var7) {
            this.setupUnityForQuery(this.schema);
            UnityStatement st = (UnityStatement)this.uconn.createStatement();
            return st.executeExplain(sql);
        }
    }

    public ResultSet executeExplain(GlobalQuery gq, MongoStatement stmt) throws SQLException {
        try {
            LQTree lqtree = gq.getLogicalQueryTree();
            MongoBuilder m = new MongoBuilder(lqtree.getRoot());
            MongoQuery mq = m.toMongoQuery();
            LocalQuery lq = new LocalQuery((AnnotatedSourceDatabase)this.database);
            lq.setSQLQueryString(mq.toMongoString());
            LQNode node = lqtree.getRoot();
            if(node instanceof LQProjNode) {
                ((LQProjNode)node).buildOutputRelation(gq);
            }

            ResultSetScan op = new ResultSetScan(lq, node);
            op.setOutputRelation(node.getOutputRelation());
            gq.setExecutionTree(op);
            return StatementImpl.executeExplain(gq);
        } catch (Exception var9) {
            this.setupUnityForQuery(this.schema);
            UnityStatement st = (UnityStatement)this.uconn.createStatement();
            return st.executeExplain(stmt.getQueryString());
        }
    }

    public ResultSet executeQuery(GlobalQuery gq, int resultSetType, MongoStatement stmt) throws SQLException {
        try {
            MongoExecutor exec = this.createExecutor(stmt.getStatementId());

            try {
                return exec.execute(gq, resultSetType, stmt, this.schema, this);
            } catch (MongoBuilderUpstreamException var6) {
                if(stmt.getStatus() != StatementImpl.UNITY_PROMOTED_QUERY) {
                    return this.processMongoWithUnity(gq.getQueryString(), this.schema);
                } else {
                    throw var6;
                }
            } catch (SQLException var7) {
                if(stmt.getStatus() != StatementImpl.UNITY_PROMOTED_QUERY) {
                    return this.processMongoWithUnity(gq.getQueryString(), this.schema);
                } else {
                    throw var7;
                }
            }
        } catch (Exception var8) {
            throw new SQLException(var8);
        }
    }

    public ResultSet executePreparedQuery(GlobalQuery gq, int resultSetType, MongoStatement stmt, String fullSQLwithParam) throws SQLException {
        try {
            MongoExecutor exec = this.createExecutor(stmt.getStatementId());
            return exec.execute(gq, resultSetType, stmt, this.schema, this);
        } catch (MongoBuilderUpstreamException var6) {
            if(stmt.getStatus() != StatementImpl.UNITY_PROMOTED_QUERY) {
                return this.processMongoWithUnity(fullSQLwithParam, this.schema);
            } else {
                throw new SQLException(var6.getMessage());
            }
        } catch (SQLException var7) {
            if(stmt.getStatus() != StatementImpl.UNITY_PROMOTED_QUERY) {
                return this.processMongoWithUnity(fullSQLwithParam, this.schema);
            } else {
                throw var7;
            }
        } catch (Exception var8) {
            throw new SQLException(var8);
        }
    }

    public int executeUpdate(String sql, MongoStatement stmt) throws SQLException {
        GlobalParser kingParser = new GlobalParser(false, false);
        if(this.schema == null) {
            this.schema = new GlobalSchema();
        }

        AnnotatedSourceDatabase db = this.schema.getDB(this.databaseName);
        if(db == null) {
            db = new AnnotatedSourceDatabase(this.databaseName, "", "", "", "", "", '"');
            this.schema.addDatabase(db);
        }

        GlobalUpdate gu = kingParser.parseUpdate(sql, this.schema);
        gu.setStatement(sql);
        return this.executeUpdate(gu, stmt);
    }

    public int executeUpdate(GlobalUpdate gu, MongoStatement stmt) throws SQLException {
        try {
            UnityDriver.debug(gu.getSqlStmt());
            LQTree lqtree = gu.getPlan().getLogicalQueryTree();
            MongoBuilder m = new MongoBuilder(lqtree.getRoot(), this.schema);
            MongoQuery mq = m.toMongoQuery();
            stmt.setQuery(mq);
            UnityDriver.debug(mq.toString());
            Object result = mq.run(this.db);
            if(result instanceof WriteResult) {
                return ((WriteResult)result).getN();
            } else if(!(result instanceof Integer)) {
                return 0;
            } else {
                if(gu.getType() == 22) {
                    GQTableRef gqtref = gu.getPlan().getFirstTableRef();
                    String dbName = null;
                    if(gqtref != null) {
                        String tableName = gqtref.getTable().getTableName();
                        if(this.schema != null) {
                            ArrayList<AnnotatedSourceTable> res = this.schema.findTable((String)dbName, tableName);
                            if(res == null || res.size() == 0) {
                                this.buildSchema();
                            }
                        }
                    }
                }

                return ((Integer)result).intValue();
            }
        } catch (MongoBuilderUpstreamException var11) {
            if(stmt.getStatus() != StatementImpl.UNITY_PROMOTED_QUERY) {
                this.setupUnityForQuery(this.schema);
                UnityStatement stmt2 = (UnityStatement)this.uconn.createStatement();
                gu.setHasGlobalSubQuery(true);
                Evaluator eval = new Evaluator(gu);
                return eval.executeUpdate(this.uconn, stmt2);
            } else {
                throw new SQLException(var11);
            }
        } catch (Exception var12) {
            String message = var12.getLocalizedMessage();
            if(message != null) {
                if(!var12.getLocalizedMessage().contains("Function not supported: CURRENT_TIME") && !var12.getLocalizedMessage().contains("Function not supported: CURRENT_TIMESTAMP") && !var12.getLocalizedMessage().contains("Function not supported: CURRENT_DATE") && !var12.getLocalizedMessage().contains("Invalid reference: current_date") && !var12.getLocalizedMessage().contains("Invalid reference: current_time") && !var12.getLocalizedMessage().contains("Invalid reference: current_timestamp")) {
                    if(var12.getLocalizedMessage().contains("Delete only supports subqueries of the form: attr [not] in (subquery)")) {
                        throw new SQLException("JDBC for MongoDB Driver: FUNCTION in projections is not supported.");
                    } else {
                        throw new SQLException(var12.getLocalizedMessage());
                    }
                } else {
                    throw new SQLException("JDBC for MongoDB Driver: Mongo JDBC Driver only supports INSERT with constant values - no expressions or functions allowed.");
                }
            } else {
                var12.printStackTrace();
                throw new SQLException(var12.getStackTrace().toString());
            }
        }
    }

    public boolean next(long stmtId, Tuple t) throws SQLException {
        MongoExecutor exec = (MongoExecutor)this.executors.get(Long.valueOf(stmtId));
        return exec == null?false:exec.next(t);
    }

    public boolean get(long stmtId, int index, Tuple t) throws SQLException {
        try {
            return false;
        } catch (Exception var6) {
            throw new SQLException(var6);
        }
    }

    public int getLast(long stmtId) throws SQLException {
        try {
            return 0;
        } catch (Exception var4) {
            throw new SQLException(var4);
        }
    }

    public void setFetchSize(int fetchSize, int resultSetType) {
        this.fetchSize = fetchSize;
    }

    public void close() throws SQLException {
        boolean var9 = false;

        String key="";
        label116: {
            try {
                var9 = true;
                Iterator i$ = this.executors.values().iterator();

                while(i$.hasNext()) {
                    MongoExecutor e = (MongoExecutor)i$.next();

                    try {
                        e.close();
                    } catch (Exception var10) {
                        ;
                    }
                }

                var9 = false;
                break label116;
            } catch (Exception var11) {
                var9 = false;
            } finally {
                if(var9) {
                    if(this.servers != null) {
                        key = this.servers.toString();
                    } else {
                        key = "[" + this.url + "]";
                    }

                    closeConnection(key, this.useSSL);
                }
            }

            key = "";
            if(this.servers != null) {
                key = this.servers.toString();
            } else {
                key = "[" + this.url + "]";
            }

            closeConnection(key, this.useSSL);
            return;
        }

        key = "";
        if(this.servers != null) {
            key = this.servers.toString();
        } else {
            key = "[" + this.url + "]";
        }

        closeConnection(key, this.useSSL);
    }

    private static void closeConnection(String key, boolean useSSL) {
        if(useSSL) {
            key = key + "ssl";
        }

        UnityDriver.debug("Closing a connection.");
        MongoClientConnection con = null;
        boolean close = false;
        lock.lock();

        try {
            con = (MongoClientConnection)mongoClients.get(key);
            if(con != null) {
                --con.count;
                if(con.count <= 0) {
                    close = true;
                    mongoClients.remove(key);
                }
            }
        } finally {
            lock.unlock();
        }

        if(close && con != null && con.client != null) {
            String addr = con.client.getAddress().toString();
            con.client.close();
            UnityDriver.debug("Closed MongoClient: " + addr);
        }

    }

    public static int getNumMongoClients() {
        if(mongoClients == null) {
            return 0;
        } else {
            UnityDriver.debug("Mongo clients: " + mongoClients.size());
            Iterator it = mongoClients.entrySet().iterator();

            while(it.hasNext()) {
                Entry<String, MongoClientConnection> e = (Entry)it.next();
                UnityDriver.debug("Client: " + e.getValue());
            }

            return mongoClients.size();
        }
    }

    public void closeStatement(long stmtId) throws SQLException {
        MongoExecutor exec = (MongoExecutor)this.executors.get(Long.valueOf(stmtId));
        if(exec != null) {
            exec.close();
            this.removeExecutor(stmtId);
        }

    }

    public String getUserName() {
        return this.userName;
    }

    public SourceDatabase getDatabase() {
        if(this.database == null) {
            this.buildSchema();
        }

        return this.database;
    }

    public GlobalSchema getSchema() {
        UnityDriver.debug("getSchema() for URL: " + this.url);
        if(this.database == null) {
            this.buildSchema();
        }

        return this.schema;
    }

    public void buildSchema() {
        this.database = this.buildSourceDatabase();
        this.schema = new GlobalSchema();
        this.schema.addDatabase(this.database);
        if(this.validation != null && this.validation.equalsIgnoreCase("flex")) {
            try {
                this.saveSchema(this.schemaLocation);
            } catch (Exception var2) {
                ;
            }
        }

    }

    public static String buildUrlString(ArrayList<ServerAddress> servers) {
        StringBuilder buf = new StringBuilder(100);
        Iterator i$ = servers.iterator();

        while(i$.hasNext()) {
            ServerAddress a = (ServerAddress)i$.next();
            buf.append(a.getHost());
            buf.append(":");
            buf.append(a.getPort());
        }

        return buf.toString();
    }

    public SourceDatabase buildSourceDatabase() {
        String dbName = this.db.getName();
        if(StringFunc.delimitedIdentifier(dbName)) {
            dbName = StringFunc.delimitName(dbName, '"');
        }

        SourceDatabase ret = new AnnotatedSourceDatabase(dbName, dbName, "MongoDB", "", "jdbc:mongo://" + buildUrlString(this.servers) + "/" + this.databaseName, "mongodb.jdbc.MongoDriver", '"');
        ret.setJavaDriverClassName("mongodb.jdbc.MongoDriver");
        ret.setDatabaseId(80020202);
        MongoExecutor exec = new MongoExecutor(this.db, -1L);
        Object obj = this.properties.get("samplesize");
        if(obj != null) {
            try {
                double d = Double.parseDouble(obj.toString());
                exec.setSampleFraction(d);
            } catch (Exception var19) {
                ;
            }
        }

        LinkedHashMap<String, SourceTable> tables = new LinkedHashMap();
        ret.setSourceTables(tables);
        String schemaName = dbName;
        Iterator i$ = this.db.getCollectionNames().iterator();

        while(i$.hasNext()) {
            String collection = (String)i$.next();
            HashMap<String, SourceField> fields = null;
            String tblName = null;
            AnnotatedSourceTable table = null;
            HashMap<String, Subtable> subtables = null;
            DBCollection dbc = null;

            try {
                dbc = this.db.getCollection(collection);
                tblName = StringFunc.delimitName(collection);
                table = new AnnotatedSourceTable((String)null, schemaName, tblName, (String)null, (HashMap)null, (SourceKey)null);
                String semanticName = StringFunc.undelimitName(tblName, '"').toLowerCase();
                table.setSemanticTableName(semanticName);
                subtables = new HashMap();
                fields = this.getHashMapFromAttributes(exec.getSchema(ret, table, dbc, subtables), tblName);
            } catch (Exception var20) {
                System.out.println("Error while building schema for collection: " + collection);
                continue;
            }

            table.setSourceFields(fields);
            ArrayList<SourceField> pkFields = new ArrayList();
            SourceField sf = (SourceField)fields.get("\"_id\"");
            if(sf != null) {
                pkFields.add(sf);
                SourceKey pk = new AnnotatedSourceKey(pkFields, 1, "pk_" + StringFunc.undelimitName(tblName, '"'));
                pk.setTable(table);
                table.setPrimaryKey(pk);
            }

            table.setParentDatabase(ret);
            table.setCaseSensitive(true);
            table.setNumTuples((int)dbc.count());
            tables.put(collection, table);
            if(subtables.size() > 0) {
                Iterator it = subtables.entrySet().iterator();

                while(it.hasNext()) {
                    Entry<String, Subtable> e = (Entry)it.next();
                    SourceTable st = MongoExecutor.buildNestedTable((Subtable)e.getValue());
                    ret.addTable(st);
                }
            }
        }

        return ret;
    }

    private HashMap<String, SourceField> getHashMapFromAttributes(Attribute[] attributeList, String collection) {
        HashMap<String, SourceField> hm = new HashMap();
        int i = 1;
        if(attributeList != null) {
            Attribute[] arr$ = attributeList;
            int len$ = attributeList.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                Attribute a = arr$[i$];
                if(a == null) {
                    System.out.println("Issue with a NULL attribute.");
                } else {
                    AnnotatedSourceField f = new AnnotatedSourceField();
                    f.setColumnName(StringFunc.delimitName(a.getName()));
                    f.setDataTypeName(Attribute.getTypeName(a.getType()));
                    f.setTableName(collection);
                    f.setDataType(a.getType());
                    if(a.getName().equals("\"_id\"")) {
                        SourceField.setSizeByType(f, a.getLength());
                    } else {
                        SourceField.setSizeByType(f, 16793600);
                    }

                    f.setOrdinalPosition(i++);
                    f.setSemanticFieldName(StringFunc.undelimitName(a.getName(), '"').toLowerCase());
                    hm.put(a.getName(), f);
                }
            }
        }

        return hm;
    }

    public String getVersion() {
        CommandResult commandResult = this.db.command("buildInfo");
        return commandResult.getString("version");
    }

    public String getDriverVersion() {
        return "";
    }

    public List<DBObject> getIndexes(String colName) throws SQLException {
        try {
            DBCollection col = this.db.getCollection(colName);
            return col.getIndexInfo();
        } catch (Exception var3) {
            throw new SQLException("Collection not found: " + colName);
        }
    }

    public DB getDB() {
        return this.db;
    }

    public GlobalCommand getCommand(long stmtId) throws SQLException {
        MongoExecutor exec = (MongoExecutor)this.executors.get(Long.valueOf(stmtId));
        return exec == null?null:null;
    }

    static {
        resources = ResourceBundle.getBundle("resources/mongo/ServerConnection", locale);
        mongoClients = new HashMap();
        lock = new ReentrantLock();
    }
}
