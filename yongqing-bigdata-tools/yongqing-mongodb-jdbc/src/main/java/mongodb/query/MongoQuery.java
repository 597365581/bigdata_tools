package mongodb.query;

import com.mongodb.DB;
import java.util.ArrayList;
import unity.query.LQCondNode;

public abstract class MongoQuery
{
    public String collectionName = null;
    protected ArrayList<LQCondNode> conditions;

    public abstract String toMongoString()
            throws MongoBuilderFatalException;

    public abstract Object run(DB paramDB)
            throws Exception;

    public String toString()
    {
        try
        {
            return toMongoString();
        }
        catch (MongoBuilderFatalException e)
        {
            e.printStackTrace();
        }
        return "";
    }

    public void addCondition(LQCondNode cn)
    {
        if (this.conditions == null)
            this.conditions = new ArrayList(1);
        this.conditions.add(cn);
    }

    public ArrayList<LQCondNode> getConditions()
    {
        return this.conditions;
    }
}