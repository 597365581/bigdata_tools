package mongodb.query;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoDeleteQuery extends MongoQuery
{
    public BasicDBObject query;

    public MongoDeleteQuery()
    {
        this.query = new BasicDBObject();
    }

    private String removeToString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("remove(");
        if (this.query.size() > 0)
            buf.append(this.query.toString());
        else {
            buf.append("{ }");
        }
        buf.append(")");
        return buf.toString();
    }

    public String toMongoString()
            throws MongoBuilderFatalException
    {
        StringBuffer buf = new StringBuffer();
        buf.append("db.");
        buf.append(this.collectionName);
        buf.append(".");
        buf.append(removeToString());
        return buf.toString();
    }

    public Object run(DB db)
            throws Exception
    {
        return db.getCollection(this.collectionName).remove(this.query);
    }
}