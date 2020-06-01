package mongodb.query;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoDropTable extends MongoQuery
{
    public String toMongoString()
            throws MongoBuilderFatalException
    {
        StringBuffer buf = new StringBuffer();
        buf.append("db.");
        buf.append(this.collectionName);
        buf.append(".");
        buf.append("drop()");
        return buf.toString();
    }

    public Object run(DB db)
            throws Exception
    {
        db.getCollection(this.collectionName).drop();
        return null;
    }
}