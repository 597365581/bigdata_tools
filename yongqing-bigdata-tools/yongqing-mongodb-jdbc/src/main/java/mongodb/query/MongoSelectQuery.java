package mongodb.query;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import unity.engine.Relation;
import unity.query.LQCondNode;
import unity.util.StringFunc;

public class MongoSelectQuery extends MongoQuery
{
    public BasicDBObject orderBy;
    public BasicDBObject groupBy;
    public BasicDBObject having;
    public BasicDBObject query;
    public BasicDBObject projections;
    public LinkedHashMap<String, String> fieldNames = new LinkedHashMap();

    public Integer limit = null;

    public Integer offset = null;

    public boolean count = false;

    public String distinctField = null;

    public boolean aggregateQuery = false;
    private Relation relation;
    private boolean isFlattening = false;

    private ArrayList<String> nestedFields = null;

    public MongoSelectQuery()
    {
        this.orderBy = new BasicDBObject();
        this.query = new BasicDBObject();
        this.projections = new BasicDBObject();
    }

    public boolean hasProjections()
    {
        return this.projections.size() > 0;
    }

    public void clearProjections()
    {
        this.projections = new BasicDBObject();
    }

    private Object runDistinct(DBCollection collection)
    {
        List results = collection.distinct(this.distinctField, this.query);

        if (this.count)
            return Integer.valueOf(results.size());
        return results;
    }

    private Object runFind(DBCollection collection)
    {
        DBCursor cursor = collection.find(this.query, this.projections);

        if (this.orderBy.size() > 0) {
            cursor.sort(this.orderBy);
        }
        if (!this.isFlattening)
        {
            if (this.limit != null)
                cursor.limit(this.limit.intValue());
            if (this.offset != null) {
                cursor.skip(this.offset.intValue());
            }
        }
        if (this.count)
        {
            int i = cursor.count();
            cursor.close();
            return Integer.valueOf(i);
        }

        return cursor;
    }

    private Object runAggregate(DBCollection collection)
    {
        List pipeline = buildAggregatePipeline();

        AggregationOutput cursor = collection.aggregate(pipeline);

        return cursor.results();
    }

    public List<DBObject> buildAggregatePipeline()
    {
        List pipeline = new ArrayList();

        if (this.query.size() > 0) {
            DBObject match = new BasicDBObject("$match", this.query);
            pipeline.add(match);
        }

        if ((this.nestedFields != null) && (this.nestedFields.size() > 0))
        {
            String[] components = StringFunc.divideId((String)this.nestedFields.get(0));

            for (int i = 0; i < components.length; i++)
            {
                DBObject unwind = new BasicDBObject("$unwind", "$" + StringFunc.combineComponents(components, 0, i + 1));
                pipeline.add(unwind);
            }

        }

        if (this.conditions != null)
        {
            for (LQCondNode c : this.conditions)
            {
                DBObject filter = new BasicDBObject("$match", c.getReference());
                pipeline.add(filter);
            }

        }

        pipeline.add(this.groupBy);

        if ((this.orderBy != null) && (this.orderBy.size() > 0))
        {
            DBObject orderby = new BasicDBObject("$sort", this.orderBy);
            pipeline.add(orderby);
        }

        if (this.having != null)
        {
            DBObject filter = new BasicDBObject("$match", this.having);
            pipeline.add(filter);
        }

        if (this.limit != null)
        {
            int offset = this.offset != null ? this.offset.intValue() : 0;
            DBObject limit = new BasicDBObject("$limit", Integer.valueOf(this.limit.intValue() + offset));
            pipeline.add(limit);
        }

        return pipeline;
    }

    public Object run(DB db)
            throws Exception
    {
        DBCollection collection = db.getCollection(this.collectionName);

        if (this.distinctField != null)
        {
            return runDistinct(collection);
        }

        if (this.aggregateQuery)
        {
            System.out.println("Running aggregate query.");
            return runAggregate(collection);
        }

        return runFind(collection);
    }

    private String findToString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("find(");
        buf.append(this.query.toString());
        buf.append(", ");
        buf.append(this.projections.toString());
        buf.append(")");
        return buf.toString();
    }

    private String distinctToString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("distinct(\"");
        buf.append(this.distinctField);
        buf.append("\", ");
        buf.append(this.query.toString());
        buf.append(")");
        return buf.toString();
    }

    public String toMongoString()
            throws MongoBuilderFatalException
    {
        if (this.collectionName == null) {
            throw new MongoBuilderFatalException("We never got a collection name! Did the FROM node go missing?");
        }
        StringBuffer buf = new StringBuffer();
        buf.append("db.");
        buf.append(this.collectionName);
        buf.append(".");

        if (this.aggregateQuery)
        {
            buf.append("aggregate(");
            buf.append(buildAggregatePipeline());
            buf.append(")");
        }
        else
        {
            if (this.distinctField != null)
                buf.append(distinctToString());
            else {
                buf.append(findToString());
            }
            if (this.orderBy.size() > 0)
            {
                buf.append(".sort( ");
                buf.append(this.orderBy.toString());
                buf.append(" )");
            }

            if ((this.limit != null) && (!this.isFlattening))
            {
                buf.append(".limit( ");
                buf.append(this.limit);
                buf.append(" )");
            }

            if ((this.offset != null) && (!this.isFlattening))
            {
                buf.append(".skip( ");
                buf.append(this.offset);
                buf.append(" )");
            }

            if (this.count)
            {
                buf.append(".count()");
            }
        }
        return buf.toString();
    }

    public Relation getRelation()
    {
        return this.relation;
    }

    public void setRelation(Relation relation)
    {
        this.relation = relation;
    }

    public boolean isFlattening()
    {
        return this.isFlattening;
    }

    public void setFlattening(boolean isFlattening)
    {
        this.isFlattening = isFlattening;
    }

    public ArrayList<String> getNestedFields()
    {
        return this.nestedFields;
    }

    public boolean isNestedField(String fld)
    {
        if (this.nestedFields != null)
        {
            for (String s : this.nestedFields)
            {
                if (s.equals(fld))
                    return true;
            }
        }
        return false;
    }

    public void setNestedFields(ArrayList<String> nestedFields)
    {
        this.nestedFields = nestedFields;
    }
}