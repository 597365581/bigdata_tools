package mongodb.query;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoDropIndex extends MongoQuery
{
    private String indexName;

    public MongoDropIndex(String indexName)
    {
        this.indexName = indexName;
    }

    public String toMongoString()
            throws MongoBuilderFatalException
    {
        StringBuffer buf = new StringBuffer();
        buf.append("db.");
        buf.append(this.collectionName);
        buf.append(".");
        buf.append("dropIndex(\"");
        buf.append(this.indexName);
        buf.append("\")");
        return buf.toString();
    }

    public Object run(DB db)
            throws Exception
    {
        db.getCollection(this.collectionName).dropIndex(this.indexName);
        return null;
    }
}