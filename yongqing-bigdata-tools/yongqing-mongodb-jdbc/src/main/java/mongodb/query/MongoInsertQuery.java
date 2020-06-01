package mongodb.query;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoException;
import java.io.PrintStream;
import java.sql.SQLException;
import unity.jdbc.UnityDriver;

public class MongoInsertQuery extends MongoQuery
{
    public BasicDBObject insertFields;

    public MongoInsertQuery()
    {
        this.insertFields = new BasicDBObject();
    }

    public String toMongoString()
            throws MongoBuilderFatalException
    {
        return "db." + this.collectionName + ".insert(" + this.insertFields.toString() + ")";
    }

    public Object run(DB db)
            throws Exception
    {
        try
        {
            db.getCollection(this.collectionName).insert(new DBObject[] { this.insertFields });
            return Integer.valueOf(1);
        }
        catch (DuplicateKeyException e)
        {
            if (UnityDriver.DEBUG) {
                System.out.println("Mongo genererated the following exception: " + e);
            }

            String err = e.getMessage().substring(e.getMessage().lastIndexOf("duplicate key error index: ") + "duplicate key error index: ".length());
            String[] fields = err.substring(0, err.indexOf(" ")).split("\\.");
            String reason = "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by '" + fields[2] + "' defined on '" + fields[0] + "." + fields[1] + "'.";

            throw new SQLException(reason);
        }
        catch (MongoException e)
        {
            String msg = e.toString();
            if (msg.contains("not authorized"))
                throw new SQLException("INSERT failed on table " + this.collectionName + ".  User does not have write permission.  Error returned from Mongo: " + e.toString());
            throw new SQLException(e);
        }
    }
}