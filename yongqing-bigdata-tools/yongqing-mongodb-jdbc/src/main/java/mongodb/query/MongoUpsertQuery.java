package mongodb.query;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import java.io.PrintStream;
import java.sql.SQLException;
import unity.jdbc.UnityDriver;

public class MongoUpsertQuery extends MongoInsertQuery
{
    public String toMongoString()
            throws MongoBuilderFatalException
    {
        return "db." + this.collectionName + ".save(" + this.insertFields.toString() + ")";
    }

    public Object run(DB db)
            throws Exception
    {
        try
        {
            db.getCollection(this.collectionName).save(this.insertFields);
            return Integer.valueOf(1);
        }
        catch (MongoException e)
        {
            if (UnityDriver.DEBUG) {
                System.out.println("Mongo genererated the following exception: " + e);
            }
            throw new SQLException(e.getCause());
        }
    }
}