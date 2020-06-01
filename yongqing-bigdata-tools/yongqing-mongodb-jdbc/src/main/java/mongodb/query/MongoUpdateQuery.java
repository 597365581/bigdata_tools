package mongodb.query;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoUpdateQuery extends MongoQuery
{
    public BasicDBObject query;
    public BasicDBObject update;

    public MongoUpdateQuery()
            throws MongoBuilderFatalException
    {
        this.update = new BasicDBObject();
        this.query = new BasicDBObject();
    }

    private String updateToString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("update(");
        if (this.query.size() > 0)
            buf.append(this.query.toString());
        else
            buf.append("{ }");
        buf.append(", ");
        buf.append(this.update.toString());
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
        buf.append(updateToString());
        return buf.toString();
    }

    public Object run(DB db)
            throws Exception
    {
        return db.getCollection(this.collectionName).update(this.query, this.update, false, true);
    }
}