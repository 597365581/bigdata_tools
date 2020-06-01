package mongodb.query;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoCreateIndex extends MongoQuery
{
    private DBObject keys;
    private DBObject options;

    public String toMongoString()
            throws MongoBuilderFatalException
    {
        StringBuffer buf = new StringBuffer();
        buf.append("db.");
        buf.append(this.collectionName);
        buf.append(".");
        buf.append("ensureIndex(");
        buf.append(this.keys);
        buf.append(", ");
        buf.append(this.options);
        buf.append(")");
        return buf.toString();
    }

    public Object run(DB db)
            throws Exception
    {
        db.getCollection(this.collectionName).createIndex(this.keys, this.options);
        return null;
    }

    public void setIndex(DBObject keys, DBObject options)
    {
        this.keys = keys;
        this.options = options;
    }
}