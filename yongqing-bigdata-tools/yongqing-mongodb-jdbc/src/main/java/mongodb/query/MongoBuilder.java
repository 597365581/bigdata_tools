package mongodb.query;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.regex.Pattern;
import mongodb.jdbc.MongoDriver;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import unity.annotation.AnnotatedSourceField;
import unity.annotation.AnnotatedSourceTable;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.engine.Attribute;
import unity.engine.Relation;
import unity.engine.Tuple;
import unity.functions.Expression;
import unity.functions.F_Cast;
import unity.generic.jdbc.Parameter;
import unity.predicates.Predicate;
import unity.query.GQFieldRef;
import unity.query.GQTableRef;
import unity.query.GlobalQuery;
import unity.query.LQCondNode;
import unity.query.LQCreateIndexNode;
import unity.query.LQDeleteNode;
import unity.query.LQDropIndexNode;
import unity.query.LQDropNode;
import unity.query.LQDupElimNode;
import unity.query.LQExprNode;
import unity.query.LQGroupByNode;
import unity.query.LQInsertNode;
import unity.query.LQLimitNode;
import unity.query.LQNode;
import unity.query.LQOrderByNode;
import unity.query.LQProjNode;
import unity.query.LQSelNode;
import unity.query.LQUpdateNode;
import unity.query.LQUpsertNode;
import unity.query.SubQuery;
import unity.util.StringFunc;

public class MongoBuilder
{
    private LQNode startNode;
    private boolean distinct = false;
    private static final String OBJECT_ID = "ObjectID";
    private static final String ID_REGEX = "[0-9a-f]{24}";
    private GlobalSchema schema = null;

    protected LQProjNode firstProj = null;

    public MongoBuilder(LQNode startNode)
    {
        this.startNode = startNode;
    }

    public MongoBuilder(LQNode startNode, GlobalSchema schema)
    {
        this.startNode = startNode;
        this.schema = schema;
    }

    private void buildDelete(LQDeleteNode node, MongoDeleteQuery in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        BasicDBObject condition = new BasicDBObject();
        if (node.getCondition() != null) {
            buildCondition(node.getCondition(), condition, node, in);
        }
        in.query = condition;
        in.collectionName = StringFunc.removeAllQuotes(node.getSourceTable().getTable().getTableName());
    }

    private void buildUpdate(LQUpdateNode node, MongoUpdateQuery in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        BasicDBObject condition = new BasicDBObject();
        if (node.getCondition() != null) {
            buildCondition(node.getCondition(), condition, node, in);
        }
        in.query = condition;
        in.collectionName = StringFunc.removeAllQuotes(node.getTable().getTable().getTableName());
        boolean isSystemUsersCollection = in.collectionName.equals("system.users");
        String userName = null;

        if (isSystemUsersCollection)
        {
            userName = node.getCondition().containsEqualAttr("\"user\"");
            if (userName == null)
            {
                userName = node.getCondition().containsEqualAttr("user");
                if (userName == null) {
                    throw new MongoBuilderFatalException("When updating system.users, must specify in the WHERE user = 'username' to select particular user.");
                }
            }
        }
        BasicDBObject update = new BasicDBObject();
        for (int i = 0; i < node.getNumFields(); i++)
        {
            LQNode field = (LQExprNode)node.getField(i);
            Object ref = field.getContent();
            String name;
            if (((ref instanceof GQFieldRef)) && (ref != null))
            {
                name = StringFunc.removeAllQuotes(((GQFieldRef)ref).getField().getColumnName());
            }
            else
                throw new MongoBuilderFatalException("Invalid field: " + field.toString());
            Object val = node.getValue(i);
            Object value = val;

            if (val == null) {
                value = "null";
            } else if ((val instanceof LQExprNode))
            {
                int type = ((LQExprNode)val).getType();
                if (type == 131)
                {
                    value = null;
                }
                else if ((type == 102) || (type == 120) || (((LQExprNode)val).getContent().toString().equalsIgnoreCase("current_time")) || (((LQExprNode)val).getContent().toString().equalsIgnoreCase("current_date")) || (((LQExprNode)val).getContent().toString().equalsIgnoreCase("current_timestamp")))
                {
                    if (((LQExprNode)val).getContent().toString().equalsIgnoreCase("STR"))
                    {
                        LQExprNode child = (LQExprNode)((LQExprNode)val).getChild(0);
                        if ((child.getContent() instanceof java.util.Date))
                        {
                            child.setType(101);
                            child.setContent(child.getReference());

                            value = child.getContent();
                        }
                        else
                        {
                            child.setType(101);
                            value = child.getContent().toString();
                        }
                    }
                    else
                    {
                        throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Functions and expressions are not supported with INSERT, UPDATE or DELETE statements: " + val);
                    }
                }
                else if (((LQExprNode)val).getType() == 145)
                {
                    Object content = ((LQExprNode)val).getContent();
                    value = Parameter.retrieveParameterValue(content);
                    if ((value instanceof BigDecimal))
                    {
                        value = new Double(((BigDecimal)value).doubleValue());
                    }

                }
                else if ((type == 104) || (type == 150) || (type == 101) || (type == 140) || (type == 141) || (type == 142) || (type == 105) || (type == 151))
                {
                    value = ((LQExprNode)val).getContent();
                    Object v = buildBSON(value);
                    if ((v instanceof BasicDBList))
                        value = v;
                }
                else
                {
                    throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Functions and expressions are not supported with INSERT, UPDATE or DELETE statements: " + val);
                }

            }

            if ((value instanceof String))
            {
                value = StringFunc.undelimitName((String)value, '\'');
            }

            if (isSystemUsersCollection) {
                if (name.equals("pwd"))
                {
                    if (userName == null) {
                        throw new MongoBuilderFatalException("User name must be supplied.");
                    }
                    value = MongoDriver._hash(userName, value.toString());
                }
                else if (name.equals("roles"))
                {
                    Object v = buildBSON(value);
                    if (v != null) {
                        value = v;
                    }
                }
            }
            update.put(name, value);
        }

        BasicDBObject update2 = new BasicDBObject();
        update2.put("$set", update);
        in.update = update2;
    }

    private void buildInsert(LQInsertNode node, MongoInsertQuery in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        in.collectionName = StringFunc.undelimitName(node.getSourceTable().getTable().getTableName(), '"');

        boolean isJSONObject = false;
        boolean isSystemUsersCollection = in.collectionName.equals("system.users");
        String userName = null;

        ArrayList fields = node.getInsertFields();
        ArrayList values = node.getInsertValues();

        if (node.getNestedQuery() != null) {
            throw new MongoBuilderUpstreamException("INSERT INTO syntax not supported with a SELECT subquery.");
        }

        if (fields.size() == 0)
        {
            if (values.size() == 1) {
                if ( !(isJSONObject = buildBSON(values.get(0)) != null));
            } else
            {
                ArrayList tables = this.schema.findTable(new String[] { node.getSourceTable().getTable().getTableName() });

                if ((tables == null) || (tables.size() != 1))
                {
                    throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Unable to resolve key-value matching as a schema has not been predicted for collection " + node.getSourceTable().getTable().getTableName());
                }

                AnnotatedSourceTable ast = (AnnotatedSourceTable)tables.get(0);
                int numberOfUserFieldsInTable;
                if ((numberOfUserFieldsInTable = ast.getNumFields() - 1) <= 0)
                {
                    throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Cannot insert values into collection " + ast.getTableName() + " as it only contains _id.");
                }
                if (numberOfUserFieldsInTable < values.size())
                {
                    throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Unable to resolve key-value matching as there are more values than keys for the existing collection.");
                }
                if (numberOfUserFieldsInTable == values.size())
                {
                    ArrayList tempField = ast.getSourceFieldsByPosition();

                    Iterator i = tempField.iterator();
                    while (i.hasNext())
                    {
                        SourceField sField = (SourceField)i.next();

                        if (sField.getColumnName().compareTo("\"_id\"") != 0)
                            fields.add(new GQFieldRef((AnnotatedSourceField)sField, sField.getColumnName(), null));
                    }
                }
                else
                {
                    throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Column names must be specified for values.");
                }

            }

        }

        if (!isJSONObject)
        {
            if (fields.size() < values.size())
                throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Unable to resolve key-value matching as there are more values than keys for the existing collection.");
            if (fields.size() > values.size()) {
                throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Unable to resolve key-value matching as there are more keys (fields) than values for the existing collection.");
            }
            for (int i = 0; i < fields.size(); i++)
            {
                GQFieldRef field = (GQFieldRef)fields.get(i);
                field.setReferenceName(StringFunc.undelimitName(field.getName(), '"'));

                LQExprNode en = (LQExprNode)values.get(i);
                int type = en.getType();

                Object content = en.getContent();
                Object val = null;

                if (type == 145)
                {
                    val = Parameter.retrieveParameterValue(content);
                    if (val != null)
                    {
                        if ((val instanceof BigDecimal))
                        {
                            val = new Double(((BigDecimal)val).doubleValue());
                        }
                        else if ((val instanceof java.sql.Date))
                        {
                            val = new java.util.Date(((java.sql.Date)val).getTime());
                        }
                        else if ((val instanceof Timestamp))
                        {
                            val = new java.util.Date(((Timestamp)val).getTime());
                        }

                        Object v = buildBSON(val);
                        if (v != null)
                            val = v;
                    }
                }
                else if (type == 104)
                {
                    val = Integer.valueOf(Integer.parseInt(content.toString()));
                }
                else if (type == 151)
                {
                    val = content;
                }
                else if (type == 150)
                {
                    val = Long.valueOf(Long.parseLong(content.toString()));
                }
                else if (type == 105)
                {
                    val = Double.valueOf(Double.parseDouble(content.toString()));
                }
                else if ((type == 127) || (type == 101))
                {
                    val = content.toString();
                    if (StringFunc.isDelimited((String)val, '\'')) {
                        val = StringFunc.undelimitName((String)val, '\'');
                    }

                    if (val != null)
                        val = val.toString().replaceAll("''", "'");
                    Object v = buildBSON(val);
                    if (v != null) {
                        val = v;
                    }

                    if (isSystemUsersCollection) {
                        if (field.getName().equals("pwd"))
                        {
                            if (userName == null) {
                                throw new MongoBuilderFatalException("User name must be supplied.");
                            }
                            val = MongoDriver._hash(userName, val.toString());
                        }
                        else if (field.getName().equals("user"))
                        {
                            userName = val.toString();
                        }
                    }
                }
                else if ((type == 140) || (type == 141) || (type == 142))
                {
                    val = content;
                }
                else if (type == 131)
                {
                    val = null;
                }
                else if (type == 102)
                {
                    if (content.toString().equalsIgnoreCase("STR"))
                    {
                        LQExprNode child = (LQExprNode)en.getChild(0);
                        if ((child.getContent() instanceof java.util.Date))
                        {
                            child.setType(101);
                            child.setContent(child.getReference());
                            val = child.getContent();
                        }
                    }
                    else if (content.toString().equalsIgnoreCase("CAST"))
                    {
                        if (en.getNumChildren() >= 2)
                        {
                            LQExprNode child = (LQExprNode)en.getChild(0);
                            LQExprNode child2 = (LQExprNode)en.getChild(1);
                            int valueType = Attribute.getTypeBySQLName(child2.getContent().toString());
                            val = child.getContent();
                            if ((val instanceof Parameter)) {
                                val = ((Parameter)val).getValue();
                            }
                            if ((child2 instanceof LQExprNode))
                            {
                                val = evaluateExpression(en);
                            }
                            try
                            {
                                val = F_Cast.changeType(val, 12, valueType);
                            }
                            catch (SQLException e)
                            {
                            }

                        }

                    }
                    else if (content.toString().compareToIgnoreCase("ObjectID") == 0)
                    {
                        int numberOfChildren = en.getNumChildren();
                        if (numberOfChildren > 1)
                        {
                            throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Incorrect number of arguments for Mongo object ObjectID");
                        }
                        if (numberOfChildren == 1)
                        {
                            val = en.getChild().getContent().toString();

                            if (!Pattern.matches("'[0-9a-f]{24}'", (String)val))
                            {
                                throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Incorrectly formated ObjectId value.  Value must be formatted as a singled quoted 3-byte hexidecimal string");
                            }

                            val = new ObjectId(StringFunc.removeQuotes((String)val));
                        }
                        else
                        {
                            val = new ObjectId();
                        }

                    }
                    else
                    {
                        val = evaluateExpression(en);
                    }
                }
                else
                {
                    val = evaluateExpression(en);
                }
                in.insertFields.put(field.toString(), val);
            }

        }
        else
        {
            Object value = values.get(0);
            if (value != null)
                value = value.toString().replaceAll("''", "'");
            DBObject obj = (DBObject)buildBSON(value);
            in.insertFields.putAll(obj);
        }
    }

    private Object evaluateExpression(LQExprNode en) throws MongoBuilderFatalException
    {
        try
        {
            Attribute outputAttribute = new Attribute();
            Expression expr = en.buildExpression(new Relation(), outputAttribute, new GlobalQuery(), new SubQuery(), null);
            return expr.evaluate(new Tuple());
        }
        catch (Exception e) {
        }
        throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Mongo JDBC Driver only supports INSERT with constant values (no expressions)");
    }

    private void buildDrop(LQDropNode node, MongoDropTable in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        in.collectionName = StringFunc.undelimitName(node.getName(), '"');
    }

    private void buildDropIndex(LQDropIndexNode node, MongoDropIndex in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        in.collectionName = StringFunc.undelimitName(node.getTableName(), '"');
    }

    private void buildCreateIndex(LQCreateIndexNode node, MongoCreateIndex in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        DBObject keys = new BasicDBObject();
        DBObject options = new BasicDBObject();

        ArrayList expr = node.getExpressions();
        ArrayList ordering = node.getOrderings();
        for (int i = 0; i < expr.size(); i++)
        {
            String indexType = node.getIndexType();
            if ((indexType != null) && (indexType.toLowerCase().contains("hash"))) {
                keys.put(((LQExprNode)expr.get(i)).toString(), "hashed");
            }
            else {
                int sort = 1;
                if (((String)ordering.get(i)).equalsIgnoreCase("DESC")) {
                    sort = 1;
                }
                keys.put(((LQExprNode)expr.get(i)).toString(), Integer.valueOf(sort));
            }
        }
        if (node.isUnique())
            options.put("unique", Boolean.valueOf(true));
        options.put("name", node.getIndexName());
        in.setIndex(keys, options);
        in.collectionName = StringFunc.undelimitName(node.getTableName(), '"');
    }

    public static Object buildBSON(Object object)
            throws MongoBuilderFatalException
    {
        if ((!(object instanceof String)) && (!(object instanceof LQExprNode))) {
            return null;
        }
        String str = null;
        try
        {
            if ((object instanceof String))
            {
                str = (String)object;
            }
            else
            {
                str = ((LQExprNode)object).getContent().toString();
            }
            Object val = JSON.parse(StringFunc.removeQuotes(str));

            if (((val instanceof DBObject)) || ((val instanceof java.util.Date)))
                return val;
            return null;
        }
        catch (JSONParseException e)
        {
        }
        return null;
    }

    private void buildDistinct(LQDupElimNode node, MongoQuery in)
            throws MongoBuilderUpstreamException, MongoBuilderFatalException
    {
        this.distinct = true;
    }

    private void buildProjection(LQProjNode node, MongoSelectQuery in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        if (node != this.firstProj) {
            return;
        }
        boolean builtRelation = true;
        GlobalQuery gq = new GlobalQuery();
        BasicDBObject groupby = null;
        if ((in.aggregateQuery) && (in.groupBy != null))
        {
            Object obj = in.groupBy.get("$group");
            if ((obj != null) && ((obj instanceof DBObject)))
            {
                groupby = new BasicDBObject();
                groupby.putAll((BSONObject)obj);
            }
        }

        if ((node.isSelectAll()) && (!in.isFlattening()))
        {
            in.clearProjections();
            builtRelation = false;
        }
        else
        {
            ArrayList<LQExprNode> expressions = node.getExpressions();
            Attribute[] attr = new Attribute[expressions.size()];
            int pos = 0;

            for (LQExprNode field : expressions)
            {
                if (in.count) {
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: If a COUNT node is specified, only one projection is allowed.");
                }
                if (in.distinctField != null) {
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: DISTINCT with multiple projections is not allowed.");
                }
                String alias = null;
                if (field.getType() == 103)
                {
                    alias = field.getChild(1).toString();
                    field = (LQExprNode)field.getChild(0);
                }

                if (field.getType() == 133) {
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: CASE is not supported.");
                }
                if (field.getType() == 102) {
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Functions in projections are not supported.");
                }
                if (field.getType() == 126) {
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Expressions in projection are not supported.");
                }
                if (field.getType() == 120)
                {
                    field = (LQExprNode)field.getContent();
                    String funcName = (String)field.getContent();
                    if ((funcName.equals("COUNT")) && (!in.aggregateQuery))
                    {
                        field = (LQExprNode)field.getChild(0);
                        in.count = true;

                        if (alias == null)
                            alias = "Count";
                        in.fieldNames.put("Count(*)", alias);

                        if (in.hasProjections())
                            throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: If a COUNT node is specified, only one projection is allowed at the moment.");
                    }
                    else if ((funcName.equals("COUNT")) || (funcName.equals("SUM")) || (funcName.equals("AVG")) || (funcName.equals("MIN")) || (funcName.equals("MAX")))
                    {
                        try
                        {
                            String key = StringFunc.removeAllQuotes(field.toString()).replaceAll("\\.", "_");
                            attr[pos] = new Attribute();
                            attr[pos].setType(field.getOutputType());

                            if (alias == null) {
                                alias = "Expr" + pos;
                            }
                            if (alias != null)
                            {
                                alias = StringFunc.removeAllQuotes(alias).replaceAll("\\.", "_");
                                attr[pos].setName(alias);

                                if (groupby != null)
                                {
                                    Object obj2 = groupby.get(key);
                                    if (obj2 != null)
                                    {
                                        ((DBObject)in.groupBy.get("$group")).removeField(key);

                                        ((DBObject)in.groupBy.get("$group")).put(alias, obj2);
                                    }

                                }

                            }

                            pos++;

                            in.fieldNames.put(key, alias);
                        }
                        catch (Exception e)
                        {
                            throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Unsupported projection expression: " + field);
                        }
                    }
                    else
                    {
                        throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: We don't know how to handle expressions that aren't COUNT yet.");
                    }

                }
                else if (field.getType() != 100)
                {
                    int type = field.getType();
                    if ((type == 104) || (type == 101) || (type == 151) || (type == 140) || (type == 141) || (type == 142))
                    {
                        attr[pos] = new Attribute();
                        try
                        {
                            Expression exp = field.buildExpression(null, attr[pos], gq, null, null);
                            if (alias != null)
                                attr[pos].setName(alias);
                            attr[pos].setReference(exp);
                            pos++;
                        }
                        catch (SQLException e)
                        {
                            throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Unsupported projection expression: " + field);
                        }
                    }
                    else {
                        throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Unsupported projection expression: " + field);
                    }
                }
                else
                {
                    if ((field.getContent().toString().equalsIgnoreCase("current_timestamp")) || (field.getContent().toString().equalsIgnoreCase("current_time")) || (field.getContent().toString().equalsIgnoreCase("current_date")))
                    {
                        throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Functions in projections are not supported.");
                    }
                    String name = null;
                    Object ref = field.getContent();
                    AnnotatedSourceField sf = null;
                    if (ref != null)
                    {
                        if ((ref instanceof GQFieldRef))
                        {
                            sf = ((GQFieldRef)ref).getField();
                            if (sf != null)
                            {
                                name = sf.getSystemName();
                                if (alias == null)
                                    alias = sf.getColumnName();
                            }
                            else {
                                name = ((GQFieldRef)ref).getName();
                            }

                            name = StringFunc.removeAllQuotes(name);

                            if (sf.getDataType() != 0);
                        }
                        else
                        {
                            name = ref.toString();
                        }
                    }
                    else {
                        throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Unable to parse field in SELECT: " + field);
                    }
                    if (name.equals("*"))
                    {
                        in.clearProjections();
                        builtRelation = false;
                        break;
                    }

                    if (this.distinct)
                    {
                        if (in.hasProjections()) {
                            throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: DISTINCT with multiple projections is not allowed at the moment.");
                        }
                        in.distinctField = name;
                    }
                    else
                    {
                        if (in.fieldNames.containsKey(name))
                        {
                            throw new MongoBuilderUpstreamException("JDBC for MongoDB: Projecting the same field more than once is not supported.");
                        }
                        addProjectionField(in, name);

                        if (alias == null)
                            alias = name;
                        in.fieldNames.put(name, alias);

                        GQFieldRef fref = (GQFieldRef)field.getContent();
                        fref.setReferenceName(StringFunc.undelimitName(fref.getName(), '"'));

                        if (sf != null) {
                            attr[(pos++)] = new Attribute(StringFunc.undelimitName(alias, '"'), sf.getDataType(), sf.getColumnSize(), fref);
                        }
                        if (in.aggregateQuery)
                        {
                            attr[(pos - 1)].setReference("_id." + name.replaceAll("\\.", "_"));
                        }
                    }
                }
            }

            if (builtRelation)
            {
                Relation r = new Relation(attr);
                in.setRelation(r);
            }
        }

        if ((!in.projections.containsField("_id")) && (in.projections.size() != 0))
            in.projections.put("_id", Integer.valueOf(0));
    }

    public void addProjectionField(MongoSelectQuery in, String name)
    {
        String baseName = name;

        int idx = StringFunc.extractNumber(baseName);
        if (idx >= 0)
        {
            baseName = baseName.substring(0, idx);
        }

        String[] baseComponents = StringFunc.divideId(baseName);

        Iterator it = in.projections.keySet().iterator();
        while (it.hasNext())
        {
            String key = (String)it.next();
            String[] keyComponents = StringFunc.divideId(key);

            if (keyComponents[0].equals(baseComponents[0]))
            {
                for (int i = 1; i < baseComponents.length; i++)
                {
                    if (i >= keyComponents.length)
                        return;
                    if (!keyComponents[i].equals(baseComponents[i]))
                    {
                        break;
                    }
                }
                for (int i = 1; i < keyComponents.length; i++)
                {
                    if (i >= baseComponents.length) {
                        it.remove();
                    }
                    else {
                        if (!keyComponents[i].equals(baseComponents[i]))
                            break;
                    }
                }
            }
        }
        in.projections.put(baseName, Integer.valueOf(1));
    }

    private void buildCondition(LQCondNode condition, BasicDBObject out, LQNode parent, MongoQuery q)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        int type = condition.getType();
        LQNode left = condition.getChild(0);
        LQNode right = condition.getChild(1);

        switch (type)
        {
            case 118:
                buildXOR(condition, left, right);
                buildCondition(condition, out, parent, q);

                break;
            case 121:
                buildXNOR(condition, left, right);
                buildCondition(condition, out, parent, q);

                break;
            case 112:
                int leftType = left.getType();
                String leftContent = left.getContent().toString();

                if ((leftType == 111) || (leftType == 110) || (leftType == 118) || (left.getType() == 112) || ((leftType == 114) && ((leftContent.equals("=")) || (leftContent.equals("!=")) || (leftContent.equals("LIKE")) || (leftContent.equals("NOT LIKE")))))
                {
                    condition = pushDownNOTIntoLogicalOperator(parent, condition, left);

                    buildCondition(condition, out, parent, q);
                }
                else if (leftType == 114)
                {
                    BasicDBObject newOut = new BasicDBObject();

                    if (left.getChild(1).getType() == 100)
                    {
                        if (left.getChild(0).getType() != 100)
                        {
                            String op = (String)left.getContent();

                            LQNode temp = left.getChild(1);
                            condition.getChild(0).setChild(1, left.getChild(0));
                            condition.getChild(0).setChild(0, temp);

                            op = switchOperator(op);

                            condition.getChild(0).setContent(op);

                            buildCondition(condition, out, parent, q);
                            return;
                        }
                    }

                    pushNOTDownIntoComparisonIOperator(out, newOut, left);
                    buildCondition((LQCondNode)left, newOut, parent, q);
                }
                else
                {
                    throw new MongoBuilderUpstreamException("Invalid condition: " + condition.toString());
                }

                break;
            case 111:
                BasicDBList andArray = rebuildBinaryExpression(parent, left, right, q);
                addAndArray(andArray, out);
                break;
            case 110:
                BasicDBList orArray = rebuildBinaryExpression(parent, left, right, q);

                BasicDBList existingOrArray = (BasicDBList)out.get("$or");
                if (existingOrArray != null) {
                    BasicDBList Andarray = new BasicDBList();
                    BasicDBObject o1 = new BasicDBObject();
                    o1.put("$or", orArray);
                    BasicDBObject o2 = new BasicDBObject();
                    o2.put("$or", existingOrArray);
                    Andarray.add(o2);
                    Andarray.add(o1);
                    addAndArray(Andarray, out);
                    out.remove("$or");
                }
                else
                {
                    out.put("$or", orArray);
                }
                break;
            case 151:
                if ((condition.getParent() == null) && (((Boolean)left.getContent()).booleanValue()))
                    return;
                throw new MongoBuilderUpstreamException("MongoDB JDBC driver does not support boolean values true and false in condition without comparison to an attribute.");
            default:
                buildComparison(condition, (LQExprNode)left, (LQExprNode)right, out, q);
        }
    }

    private void addAndArray(BasicDBList Andarray, BasicDBObject out)
    {
        BasicDBList existingAndArray = (BasicDBList)out.get("$and");
        Iterator i$;
        if (existingAndArray != null)
        {
            for (i$ = Andarray.iterator(); i$.hasNext(); ) { Object obj = i$.next();

                existingAndArray.add(obj);
            }
        }
        else
        {
            out.put("$and", Andarray);
        }
    }

    private void buildComparison(LQCondNode condition, LQExprNode left, LQExprNode right, BasicDBObject out, MongoQuery q) throws MongoBuilderFatalException, MongoBuilderUpstreamException {
        MongoExpression leftExpr = this.buildExpression(left, (MongoExpression)null);
        MongoExpression rightExpr = this.buildExpression(right, (MongoExpression)null);
        boolean hasExprLeft = leftExpr.isExpression();
        boolean hasExprRight = rightExpr.isExpression();
        String op = condition.getContent().toString();
        LQCondNode newCond;
        if(!(condition.getReference() instanceof BasicDBObject) && (leftExpr.findReference() != null || rightExpr.findReference() != null)) {
            BasicDBObject tmp = new BasicDBObject();
            condition.setReference(tmp);
            newCond = (LQCondNode)condition.clone();
            LQExprNode tmpLeft = (LQExprNode)newCond.getChild(0);
            if(left.getContent() != null && left.getContent().toString().equalsIgnoreCase("ELEMMATCH")) {
                newCond.setChild(0, tmpLeft.getChild(0));
            }

            q.addCondition(newCond);
            this.buildComparison(condition, left, right, tmp, q);
        }

        String lval;
        String st;
        if(hasExprRight && hasExprLeft) {
            if(out == ((MongoSelectQuery)q).having) {
                throw new MongoBuilderUpstreamException("MongoDB JDBC Driver does not support expressions in HAVING clause.");
            } else {
                lval = leftExpr.toString(true);
                String rStr = rightExpr.toString(true);
                if(op.equals("=")) {
                    st = " == ";
                } else {
                    st = " " + op + " ";
                }

                String value = lval + st + rStr;
                out.put("$where", value);
            }
        } else {
            if(hasExprRight && !hasExprLeft) {
                op = this.switchOperator(op);
                MongoExpression tmp = leftExpr;
                leftExpr = rightExpr;
                rightExpr = tmp;
            }

            lval = leftExpr.toString();
            BasicDBObject matchcond;
            if(leftExpr.getType() != 100 && leftExpr.getType() != 101) {
                newCond = null;

                try {
                    Predicate p = condition.buildPredicate(op, condition);
                    Object v = LQExprNode.evaluateExpression(left);
                    if(v != null) {
                        Object v2 = LQExprNode.evaluateExpression(right);
                        boolean result = p.evaluate(v, v2);
                        LQNode parent = condition.getParent();
                        if(result) {
                            if(parent != null && parent.getType() != 111) {
                                matchcond = new BasicDBObject();
                                matchcond.put(this.rebuildOperator(op), (Object)null);
                                out.put("_id", matchcond);
                                return;
                            }

                            return;
                        }

                        out.put("_id", (Object)null);
                        return;
                    }
                } catch (Exception var20) {
                    ;
                }

                throw new MongoBuilderUpstreamException("MongoDB JDBC Driver only supports comparisons of the form attribute op value.  Left expression: " + lval);
            } else {
                Object rval = rightExpr.getValue();
                if(lval.equals("_id") && rval != null && rval instanceof String) {
                    st = (String)rval;
                    if(st.length() == 24) {
                        rval = new ObjectId((String)rval);
                    }
                }

                if(lval.contains("$elemMatch") && rval != null) {
                    st = lval.substring(11);
                    String[] components = StringFunc.divideId(st);
                    String arrayName = components[0];
                    BasicDBObject working = (BasicDBObject)out.get(arrayName);
                    if(working == null) {
                        working = new BasicDBObject();
                    }

                    matchcond = new BasicDBObject();
                    String lkey;
                    DBObject elemMatch;
                    if(components.length > 1) {
                        lkey = StringFunc.combineComponents(components, 1, components.length);
                        if(op.equals("=")) {
                            matchcond.put(lkey, rval);
                        } else {
                            String opkey = this.rebuildOperator(op);
                            matchcond.put(opkey, rval);
                            matchcond = new BasicDBObject(lkey, matchcond);
                        }

                        elemMatch = (DBObject)working.get("$elemMatch");
                        if(elemMatch == null) {
                            working.put("$elemMatch", matchcond);
                        } else {
                            elemMatch.putAll((BSONObject) matchcond);
                        }

                        out.put(arrayName, working);
                    } else {
                        if(op.equals("=")) {
                            elemMatch = (DBObject)working.get("$elemMatch");
                            lkey = this.rebuildOperator("<=");
                            matchcond.put(lkey, rval);
                            lkey = this.rebuildOperator(">=");
                            matchcond.put(lkey, rval);
                            if(elemMatch == null) {
                                working.put("$elemMatch", matchcond);
                            } else {
                                elemMatch.put(lkey, rval);
                                working = (BasicDBObject)elemMatch;
                            }
                        } else {
                            lkey = this.rebuildOperator(op);
                            matchcond.put(lkey, rval);
                            elemMatch = (DBObject)working.get("$elemMatch");
                            if(elemMatch == null) {
                                working.put("$elemMatch", matchcond);
                            } else {
                                elemMatch.put(lkey, rval);
                            }
                        }

                        out.put(arrayName, working);
                    }
                } else if(op.equals("=")) {
                    this.addToQueryObject(out, lval, rval);
                } else {
                    BasicDBObject working;
                    if(!op.equals(">") && !op.equals("<") && !op.equals("<=") && !op.equals(">=")) {
                        if(!op.equals("!=") && !op.equals("<>")) {
                            if(!op.equals("IN") && !op.equals("NOT IN")) {
                                if(condition.getType() == 114) {
                                    st = rval.toString();
                                    if(st.equals("NULL")) {
                                        this.addToQueryObject(out, lval, (Object)null);
                                    } else if(!st.equals("FALSE") && !st.equals("TRUE")) {
                                        if(st.equals("NOT NULL")) {
                                            working = new BasicDBObject();
                                            working.put("$ne", (Object)null);
                                            this.addToQueryObject(out, lval, working);
                                        } else {
                                            working = new BasicDBObject();
                                            if(op.equalsIgnoreCase("NOT LIKE")) {
                                                out.put(lval, working);
                                                working.put(this.rebuildOperator(op), Pattern.compile("^" + StringFunc.convertSQLPatternToJavaPattern(rval.toString()) + "$"));
                                            } else {
                                                this.addToQueryObject(out, lval, Pattern.compile("^" + StringFunc.convertSQLPatternToJavaPattern(rval.toString()) + "$"));
                                            }
                                        }
                                    } else {
                                        this.addToQueryObject(out, lval, Boolean.valueOf(Boolean.parseBoolean(st)));
                                    }
                                }
                            } else if(rval instanceof ArrayList) {
                                ArrayList<LQExprNode> list = (ArrayList)rval;
                                ArrayList<Object> listVals = new ArrayList(list.size());

                                Object v;
                                for(Iterator i$ = list.iterator(); i$.hasNext(); listVals.add(v)) {
                                    LQExprNode en = (LQExprNode)i$.next();
                                    v = en.getContent();
                                    if(v instanceof Parameter) {
                                        v = ((Parameter)v).getValue();
                                    }

                                    if(v instanceof String) {
                                        v = StringFunc.removeQuotes((String)v);
                                    }
                                }

                                if(op.equals("IN")) {
                                    out.put(lval, new BasicDBObject("$in", listVals));
                                } else if(op.equals("NOT IN")) {
                                    out.put(lval, new BasicDBObject("$nin", listVals));
                                }
                            } else {
                                if(!(rval instanceof String)) {
                                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: IN is only supported with constant values.  No subqueries or expressions.");
                                }

                                this.addToQueryObject(out, lval, Pattern.compile("^" + StringFunc.convertSQLPatternToJavaPattern(rval.toString()) + "$"));
                            }
                        } else {
                            working = new BasicDBObject();
                            out.put(lval, working);
                            working.put(this.rebuildOperator(op), rval);
                        }
                    } else {
                        working = (BasicDBObject)out.get(lval);
                        if(working == null) {
                            working = new BasicDBObject();
                            if(!lval.equalsIgnoreCase("NOT") && !lval.equalsIgnoreCase("NOT LIKE")) {
                                out.put(lval, working);
                            } else {
                                out.put(this.rebuildOperator(lval), working);
                            }
                        }

                        working.put(this.rebuildOperator(op), rval);
                    }
                }

            }
        }
    }

    private void pushNOTDownIntoComparisonIOperator(BasicDBObject out, BasicDBObject newOut, LQNode left)
    {
        Object lVal = left.getChild(0).getContent();

        if ((lVal instanceof String))
            lVal = StringFunc.removeQuotes((String)lVal);
        else if ((lVal instanceof GQFieldRef)) {
            lVal = StringFunc.undelimitName(((GQFieldRef)lVal).getName(), '"');
        }

        out.put((String)lVal, newOut);

        if (left.getContent().toString().compareTo("=") == 0)
        {
            left.setContent("!=");
        }
        else if (left.getContent().toString().compareToIgnoreCase("NOT LIKE") == 0)
        {
            left.setContent("LIKE");
            left.setType(114);
        }
        else if (left.getContent().toString().compareToIgnoreCase("LIKE") == 0)
        {
            left.setContent("NOT LIKE");
            left.setType(114);
        }
        else
        {
            left.getChild(0).setContent("NOT");
            left.getChild(0).setType(101);
        }
    }

    private LQCondNode pushDownNOTIntoLogicalOperator(LQNode parent, LQCondNode cond, LQNode left)
            throws MongoBuilderUpstreamException
    {
        LQCondNode condition = cond;

        complementConditionNode(condition, left);

        Iterator childIterator = left.getChildren().iterator();
        String leftStr = left.getContent().toString();
        if ((leftStr.compareToIgnoreCase("XOR") == 0) || (leftStr.compareToIgnoreCase("XNOR") == 0))
        {
            pushNOTDownAndComplementOperator(condition, childIterator);
        }
        else if (leftStr.compareToIgnoreCase("NOT") == 0)
        {
            condition = collapseDoubleNOT(parent, condition, childIterator);
        }
        else if ((leftStr.compareToIgnoreCase("AND") == 0) || (leftStr.compareToIgnoreCase("OR") == 0))
        {
            pushNOTDownANDandOR(condition, childIterator);
        }
        else if ((leftStr.compareToIgnoreCase("LIKE") == 0) || (leftStr.compareToIgnoreCase("NOT LIKE") == 0) || (leftStr.compareToIgnoreCase("=") == 0) || (leftStr.compareToIgnoreCase("!=") == 0) || (leftStr.compareToIgnoreCase("<>") == 0))
        {
            pushNOTDownAndComplementOperator(condition, childIterator);
        }
        return condition;
    }

    private BasicDBList rebuildBinaryExpression(LQNode parent, LQNode left, LQNode right, MongoQuery q)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        BasicDBList array = new BasicDBList();
        BasicDBObject leftTerm = new BasicDBObject();
        BasicDBObject rightTerm = new BasicDBObject();

        buildCondition((LQCondNode)left, leftTerm, parent, q);
        buildCondition((LQCondNode)right, rightTerm, parent, q);

        if (leftTerm.size() > 0)
            array.add(leftTerm);
        if (rightTerm.size() > 0)
            array.add(rightTerm);
        return array;
    }

    private void complementConditionNode(LQCondNode condition, LQNode left)
            throws MongoBuilderUpstreamException
    {
        String leftStr = left.getContent().toString();
        if (leftStr.compareToIgnoreCase("=") == 0)
        {
            condition.setContent("!=");
            condition.setType(114);
        }
        else if ((leftStr.compareToIgnoreCase("!=") == 0) || (leftStr.compareToIgnoreCase("<>") == 0))
        {
            condition.setContent("=");
            condition.setType(114);
        }
        else if (leftStr.compareToIgnoreCase("LIKE") == 0)
        {
            condition.setContent("NOT LIKE");
            condition.setType(114);
        }
        else if (leftStr.compareToIgnoreCase("NOT LIKE") == 0)
        {
            condition.setContent("LIKE");
            condition.setType(114);
        }
        else if (leftStr.compareToIgnoreCase("AND") == 0)
        {
            condition.setContent("OR");
            condition.setType(110);
        }
        else if (leftStr.compareToIgnoreCase("OR") == 0)
        {
            condition.setContent("AND");
            condition.setType(111);
        }
        else if (leftStr.compareToIgnoreCase("XOR") == 0)
        {
            condition.setContent("XNOR");
            condition.setType(121);
        }
        else if (leftStr.compareToIgnoreCase("XNOR") == 0)
        {
            condition.setContent("XOR");
            condition.setType(118);
        }
        else if (leftStr.compareToIgnoreCase("NOT") != 0)
        {
            throw new MongoBuilderUpstreamException(leftStr + " can not be negated properly");
        }
    }

    private void pushNOTDownANDandOR(LQCondNode condition, Iterator<LQNode> childIterator)
    {
        condition.removeChild(0);

        while (childIterator.hasNext())
        {
            LQNode oldChild = (LQNode)childIterator.next();
            LQCondNode newChild = new LQCondNode();

            newChild.setContent("NOT");
            newChild.setType(112);
            newChild.addChild(oldChild);

            condition.addChild(newChild);
        }
    }

    private LQCondNode collapseDoubleNOT(LQNode parent, LQCondNode cond, Iterator<LQNode> childIterator)
    {
        LQCondNode condition = cond;

        while (childIterator.hasNext())
        {
            LQNode oldChild = (LQNode)childIterator.next();

            if (condition.getParent() != null)
            {
                condition.getParent().replaceChild(condition, oldChild);
                condition = (LQCondNode)oldChild;
            }
            else if ((parent instanceof LQSelNode))
            {
                ((LQSelNode)parent).setCondition((LQCondNode)oldChild);
                condition = ((LQSelNode)parent).getCondition();
            }
            else if ((parent instanceof LQUpdateNode))
            {
                ((LQUpdateNode)parent).setCondition((LQCondNode)oldChild);
                condition = ((LQUpdateNode)parent).getCondition();
            }
            else if ((parent instanceof LQDeleteNode))
            {
                ((LQDeleteNode)parent).setCondition((LQCondNode)oldChild);
                condition = ((LQDeleteNode)parent).getCondition();
            }
        }

        return condition;
    }

    private void pushNOTDownAndComplementOperator(LQCondNode condition, Iterator<LQNode> childIterator)
    {
        condition.removeChild(0);

        while (childIterator.hasNext())
        {
            LQNode oldChild = (LQNode)childIterator.next();

            condition.addChild(oldChild);
            oldChild.setParent(condition);
        }
    }

    private void buildXNOR(LQCondNode condition, LQNode left, LQNode right)
    {
        LQNode llChild = (LQNode)left.clone();
        LQNode lrChild = (LQNode)right.clone();
        LQNode rlChild = left;
        LQNode rrChild = right;

        LQCondNode lAnd = new LQCondNode();
        lAnd.setType(111);
        lAnd.setContent("AND");

        LQCondNode rAnd = new LQCondNode();
        rAnd.setType(111);
        rAnd.setContent("AND");

        LQCondNode llNot = new LQCondNode();
        llNot.setType(112);
        llNot.setContent("NOT");

        LQCondNode lrNot = new LQCondNode();
        lrNot.setType(112);
        lrNot.setContent("NOT");

        lAnd.addChild(llNot);

        llNot.addChild(llChild);

        lAnd.addChild(lrNot);

        lrNot.addChild(lrChild);

        rAnd.addChild(rlChild);

        rAnd.addChild(rrChild);

        condition.replaceChild(left, lAnd);
        condition.replaceChild(right, rAnd);

        condition.setContent("OR");
        condition.setType(110);
    }

    private void buildXOR(LQCondNode condition, LQNode left, LQNode right)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        LQNode llChild = (LQNode)left.clone();
        LQNode lrChild = (LQNode)right.clone();
        LQNode rlChild = left;
        LQNode rrChild = right;

        LQCondNode lAnd = new LQCondNode();
        lAnd.setType(111);
        lAnd.setContent("AND");

        LQCondNode rAnd = new LQCondNode();
        rAnd.setType(111);
        rAnd.setContent("AND");

        LQCondNode llNot = new LQCondNode();
        llNot.setType(112);
        llNot.setContent("NOT");

        LQCondNode rrNot = new LQCondNode();
        rrNot.setType(112);
        rrNot.setContent("NOT");

        lAnd.addChild(llNot);

        llNot.addChild(llChild);

        lAnd.addChild(lrChild);

        rAnd.addChild(rrNot);

        rrNot.addChild(rrChild);

        rAnd.addChild(rlChild);

        condition.replaceChild(left, lAnd);
        condition.replaceChild(right, rAnd);

        condition.setContent("OR");
        condition.setType(110);
    }

    private String rebuildOperator(String op)
            throws MongoBuilderFatalException
    {
        if (op.equals("="))
            return "$eq";
        if (op.equals(">"))
            return "$gt";
        if (op.equals("<"))
            return "$lt";
        if (op.equals(">="))
            return "$gte";
        if (op.equals("<="))
            return "$lte";
        if ((op.equals("!=")) || (op.equals("<>"))) {
            return "$ne";
        }

        if ((op.equals("NOT LIKE")) || (op.equals("NOT"))) {
            return "$not";
        }

        throw new MongoBuilderFatalException("JDBC for MongoDB Driver: Operator " + op + " not supported.");
    }

    private String switchOperator(String op)
    {
        if (op.equals("<")) {
            return ">";
        }
        if (op.equals("<=")) {
            return ">=";
        }
        if (op.equals(">")) {
            return "<";
        }
        if (op.equals(">=")) {
            return "<=";
        }
        if (op.equals("!<")) {
            return "!<";
        }
        if (op.equals("!<")) {
            return "!>";
        }

        return op;
    }

    private MongoExpression buildExpression(LQExprNode node, MongoExpression parent)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        int type = node.getType();

        if ((type == 100) || (type == 120))
        {
            Object obj = node.getContent();
            String name = "";
            String ref = null;
            if ((obj instanceof GQFieldRef))
            {
                AnnotatedSourceField sf = ((GQFieldRef)obj).getField();
                if (sf != null)
                {
                    name = sf.getColumnName();

                    SourceTable parentTable = sf.getParentTable();
                    if ((parentTable != null) && (parentTable.isNestedField(sf)))
                    {
                        ref = name;
                    }
                    name = sf.getSystemName();
                }
                else {
                    name = ((GQFieldRef)obj).getName();
                }
                name = StringFunc.undelimitName(name, '"');
            }
            else
            {
                name = StringFunc.undelimitName(node.getContent().toString(), '"');
            }

            return new MongoExpression(type, parent, null, name, ref, true);
        }
        if ((type == 127) || (type == 101))
        {
            return new MongoExpression(type, parent, null, StringFunc.removeQuotes((String)node.getContent()), null, false);
        }
        if ((type == 104) || (type == 105) || (type == 151) || (type == 150))
        {
            return new MongoExpression(type, parent, null, node.getContent(), null, false);
        }
        if ((type == 140) || (type == 141) || (type == 142))
        {
            Object dateVal = node.getContent();
            if ((dateVal instanceof String))
                dateVal = StringFunc.removeQuotes((String)node.getContent());
            return new MongoExpression(type, parent, null, dateVal, null, false);
        }
        if (type == 126)
        {
            ArrayList children = new ArrayList(2);
            MongoExpression opExpr = new MongoExpression(type, parent, children, node.getContent(), null, true);
            MongoExpression leftExpr = buildExpression((LQExprNode)node.getChild(0), opExpr);
            MongoExpression rightExpr = buildExpression((LQExprNode)node.getChild(1), opExpr);
            children.add(leftExpr);
            children.add(rightExpr);
            return opExpr;
        }
        if ((type == 102) || (type == 125) || (type == 124))
        {
            String funcName = node.getContent().toString();
            if (funcName.equalsIgnoreCase("STR"))
            {
                LQExprNode child = (LQExprNode)node.getChild(0);
                if ((child.getContent() instanceof java.util.Date))
                {
                    child.setType(101);
                    child.setContent(child.getReference());

                    return buildExpression(child, parent);
                }
            }
            else if (funcName.equalsIgnoreCase("ELEMMATCH"))
            {
                LQExprNode child = (LQExprNode)node.getChild(0);
                if ((child != null) && ((child instanceof LQExprNode)) && (child.getType() == 100))
                {
                    String arrayName = StringFunc.removeAllQuotes(child.getContent().toString());

                    type = 100;
                    MongoExpression elemmatch = new MongoExpression(type, parent, null, "$elemMatch:" + arrayName, arrayName, false);
                    return elemmatch;
                }
            }
            else if (funcName.equalsIgnoreCase("OBJECTID"))
            {
                LQExprNode child = (LQExprNode)node.getChild(0);
                if (child == null)
                    throw new MongoBuilderUpstreamException("ObjectId function requires a parameter");
                return new MongoExpression(type, parent, null, new ObjectId(StringFunc.removeQuotes((String)child.getContent())), null, false);
            }

            throw new MongoBuilderUpstreamException("Function not supported: " + node.getContent());
        }
        if (type == 130)
        {
            return new MongoExpression(type, parent, null, node.getContent().toString(), null, false);
        }
        if (type == 145)
        {
            Object content = node.getContent();
            Object obj = Parameter.retrieveParameterValue(content);

            if ((obj instanceof String)) {
                return new MongoExpression(type, parent, null, StringFunc.removeQuotes((String)obj), null, false);
            }

            if ((obj instanceof BigDecimal))
            {
                obj = new Double(((BigDecimal)obj).doubleValue());
            }
            return new MongoExpression(type, parent, null, obj, null, false);
        }

        if (type == 134)
        {
            ArrayList valList = (ArrayList)node.getContent();

            MongoExpression list = new MongoExpression(type, parent, null, valList, null, false);
            return list;
        }
        if (type == 17)
        {
            throw new MongoBuilderUpstreamException("Subqueries are not supported by MongoDB JDBC driver.");
        }

        throw new MongoBuilderUpstreamException("Invalid expression: " + node.getContent());
    }

    private void addToQueryObject(BasicDBObject out, String key, Object value)
    {
        Object obj = out.get(key);
        if (obj == null) {
            out.put(key, value);
        }
        else {
            BasicDBList andArray = new BasicDBList();
            BasicDBObject otmp = new BasicDBObject();

            otmp.put(key, obj);
            andArray.add(otmp);

            otmp = new BasicDBObject();
            otmp.put(key, value);
            andArray.add(otmp);
            out.put("$and", andArray);
            out.remove(key);
        }
    }

    private void rebuildSelection(LQSelNode node, MongoSelectQuery in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        LQCondNode condition = node.getCondition();
        BasicDBObject out = in.query;

        if (node.bHavingCondition)
        {
            if (in.having == null)
                in.having = new BasicDBObject();
            out = in.having;
        }
        buildCondition(condition, out, node, in);
    }

    private void rebuildGroupBy(LQGroupByNode node, MongoSelectQuery in)
            throws MongoBuilderUpstreamException
    {
        if ((!in.isFlattening()) && (node.getExpressions().size() == 0) && (node.getFunctionList().size() == 1) && (((LQExprNode)node.getFunctionList().get(0)).getContent().toString().equalsIgnoreCase("count")))
        {
            LQExprNode en = (LQExprNode)node.getFunctionList().get(0);
            checkCountFieldExpression(en, in);
        }
        else
        {
            BasicDBObject groupByClause = new BasicDBObject();
            BasicDBObject groupByAttr = new BasicDBObject();
            BasicDBObject groupByAttrClause = new BasicDBObject();

            in.aggregateQuery = true;

            for (LQExprNode groupbyAttr : node.getExpressions())
            {
                BasicDBObject expr = new BasicDBObject();
                Object key = convertToMongoAggregateExpr(groupbyAttr, expr, false);
                if (key == null)
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Unsupported aggregate expression: " + groupbyAttr);
                String val = "$" + key;
                String keystr = key.toString();
                keystr = keystr.replaceAll("\\.", "_");
                groupByAttr.put(keystr, val);
            }
            if (groupByAttr.size() == 0)
                groupByAttr = null;
            groupByAttrClause.put("_id", groupByAttr);

            for (LQExprNode func : node.getFunctionList())
            {
                String key = StringFunc.removeAllQuotes(func.toString()).replaceAll("\\.", "_");

                if (groupByAttrClause.containsField(key));
                groupByAttrClause.put(key, convertToMongoAggregateExpression(func, in));
            }

            groupByClause.put("$group", groupByAttrClause);
            in.groupBy = groupByClause;
        }
    }

    public void checkCountFieldExpression(LQExprNode en, MongoSelectQuery in)
            throws MongoBuilderUpstreamException
    {
        LQExprNode child = (LQExprNode)en.getChild(0);
        if (child.getType() == 100)
        {
            String fieldName = StringFunc.removeAllQuotes(child.getContent().toString());

            Object currentFilter = in.query.get(fieldName);
            if (currentFilter != null)
            {
                BasicDBList andClauses = new BasicDBList();
                andClauses.add(new BasicDBObject(fieldName, new BasicDBObject("$exists", Boolean.valueOf(true))));
                andClauses.add(new BasicDBObject(fieldName, currentFilter));
                addAndArray(andClauses, in.query);
                in.query.remove(fieldName);
            }
            else
            {
                in.query.put(fieldName, new BasicDBObject("$exists", Boolean.valueOf(true)));
            }
        }
        else if (!en.toString().equalsIgnoreCase("count(*)"))
        {
            throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Only support COUNT(*) or COUNT(attr).");
        }
    }

    public DBObject convertToMongoAggregateExpression(LQExprNode en, MongoSelectQuery in)
            throws MongoBuilderUpstreamException
    {
        String aggregateFunctionName = en.getContent().toString();
        String aggfunc = null;

        if (aggregateFunctionName.equals("COUNT"))
        {
            LQExprNode childNode = (LQExprNode)en.getChild(0);
            if (childNode.getType() == 101)
            {
                if ((childNode.getContent() != null) && (childNode.getContent().toString().equals("*")))
                {
                    return new BasicDBObject("$sum", Integer.valueOf(1));
                }
            }
            String countExpression = convertToMongoAggregateExpr(childNode, null, false).toString();
            BasicDBObject gt = new BasicDBObject();
            BasicDBList options = new BasicDBList();
            options.add("$" + countExpression);
            options.add(null);
            gt.put("$gt", options);
            BasicDBObject overall = new BasicDBObject();
            BasicDBList arrayCount = new BasicDBList();
            arrayCount.add(gt);
            arrayCount.add(Integer.valueOf(1));
            arrayCount.add(Integer.valueOf(0));
            overall.put("$cond", arrayCount);
            return new BasicDBObject("$sum", overall);
        }

        if (aggregateFunctionName.equals("SUM"))
            aggfunc = "$sum";
        else if (aggregateFunctionName.equals("MAX"))
            aggfunc = "$max";
        else if (aggregateFunctionName.equals("MIN"))
            aggfunc = "$min";
        else if (aggregateFunctionName.equals("AVG")) {
            aggfunc = "$avg";
        }

        return new BasicDBObject(aggfunc, convertToMongoAggregateExpr((LQExprNode)en.getChild(0), null, true));
    }

    public Object convertToMongoAggregateExpr(LQExprNode en, BasicDBObject objexpr, boolean includeDollarSign)
            throws MongoBuilderUpstreamException
    {
        int type = en.getType();
        if (type == 100)
        {
            String fieldName = StringFunc.removeAllQuotes(en.getContent().toString());
            if (includeDollarSign) {
                return "$" + fieldName;
            }
            return fieldName;
        }
        if ((type == 108) || (type == 126))
        {
            BasicDBObject op = new BasicDBObject();
            BasicDBList operands = new BasicDBList();
            operands.add(convertToMongoAggregateExpr((LQExprNode)en.getChild(0), null, includeDollarSign));
            operands.add(convertToMongoAggregateExpr((LQExprNode)en.getChild(1), null, includeDollarSign));

            if (en.getContent().equals("+"))
                op.put("$add", operands);
            else if (en.getContent().equals("-"))
                op.put("$subtract", operands);
            else if (en.getContent().equals("*"))
                op.put("$multiply", operands);
            else if (en.getContent().equals("/"))
                op.put("$divide", operands);
            else
                throw new MongoBuilderUpstreamException("MongoDB JDBC Driver: Unable to convert operator: " + en);
            return op;
        }
        throw new MongoBuilderUpstreamException("Operator or expression not supported by MongoDB in aggregation pipeline: " + en);
    }

    private void rebuildOrderBy(LQOrderByNode node, MongoSelectQuery in)
            throws MongoBuilderUpstreamException
    {
        for (int i = 0; i < node.getOrderNumChildren(); i++)
        {
            LQNode childNode = node.getOrderChild(i);
            Object content = childNode.getContent();
            String fieldName = null;
            if (in.aggregateQuery)
            {
                fieldName = StringFunc.removeAllQuotes(content.toString()).replaceAll("\\.", "_");

                Object obj = in.groupBy.get("$group");
                Object objid = null;
                boolean found = false;

                if ((obj != null) && ((obj instanceof DBObject)))
                    objid = ((DBObject)obj).get("_id");
                if ((objid != null) && ((objid instanceof DBObject)))
                {
                    if (((DBObject)objid).containsField(fieldName)) {
                        fieldName = "_id." + fieldName;
                        found = true;
                    }
                }

                String aliasName = (String)in.fieldNames.get(fieldName);
                if (aliasName != null)
                {
                    found = true;
                    fieldName = aliasName;
                }
                if (!found) {
                    found = in.fieldNames.containsKey(fieldName);
                }
                if (!found) {
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: ORDER BY with an expression or function is not supported by the driver: " + childNode);
                }

            }
            else if ((content instanceof GQFieldRef)) {
                fieldName = ((GQFieldRef)content).getName();
            } else {
                throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: ORDER BY with an expression or function is not supported by the driver.");
            }

            if (in.isNestedField(fieldName)) {
                throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: ORDER BY on nested field is not supported by the driver.");
            }
            String sqlDirection = node.getDirection(i);
            int direction = sqlDirection == "DESC" ? -1 : 1;

            fieldName = StringFunc.undelimitName(fieldName, '"');
            in.orderBy.put(fieldName, Integer.valueOf(direction));
        }
    }

    private void rebuildFrom(LQNode node, MongoSelectQuery in)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        SourceTable st = ((GQTableRef)node.getContent()).getTable();
        if (st == null) {
            throw new MongoBuilderUpstreamException("ERROR: Invalid table name provided.");
        }

        String parentTableName = (String)st.getProperty("nested");
        if (parentTableName != null) {
            in.collectionName = parentTableName;
            in.setFlattening(true);
            in.setNestedFields(st.getFlattenedFields());
        }
        else {
            in.collectionName = st.getTableName();
        }

        if (StringFunc.isDelimited(in.collectionName, '"'))
            in.collectionName = StringFunc.undelimitName(in.collectionName, '"');
    }

    private void rebuildLimit(LQLimitNode node, MongoSelectQuery in)
    {
        if (node.hasOffset())
        {
            in.offset = Integer.valueOf(node.getStart());
        }

        in.limit = Integer.valueOf(node.getCount());
    }

    public MongoQuery toMongoQuery()
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        MongoQuery out = new MongoSelectQuery();
        return toMongoQuery(out, this.startNode);
    }

    public MongoQuery toMongoQuery(MongoQuery out, LQNode currentNode)
            throws MongoBuilderFatalException, MongoBuilderUpstreamException
    {
        if (currentNode != null)
        {
            int type = currentNode.getType();

            if ((type == 1) && (this.firstProj == null)) {
                this.firstProj = ((LQProjNode)currentNode);
            }

            if (type == 16) {
                this.distinct = true;
            }

            if (currentNode.getNumChildren() > 0) {
                toMongoQuery(out, currentNode.getChild());
            }
            switch (type)
            {
                case 1:
                    buildProjection((LQProjNode)currentNode, (MongoSelectQuery)out);
                    break;
                case 2:
                    rebuildSelection((LQSelNode)currentNode, (MongoSelectQuery)out);
                    break;
                case 6:
                case 100:
                    rebuildFrom(currentNode, (MongoSelectQuery)out);
                    break;
                case 18:
                    rebuildLimit((LQLimitNode)currentNode, (MongoSelectQuery)out);
                    break;
                case 4:
                    if (this.distinct)
                        throw new MongoBuilderUpstreamException("MongoDB does not support ORDER BY and DISTINCT together.");
                    rebuildOrderBy((LQOrderByNode)currentNode, (MongoSelectQuery)out);
                    break;
                case 5:
                    rebuildGroupBy((LQGroupByNode)currentNode, (MongoSelectQuery)out);
                    break;
                case 16:
                    buildDistinct((LQDupElimNode)currentNode, out);
                    break;
                case 22:
                case 23:
                    out = new MongoInsertQuery();
                    buildInsert((LQInsertNode)currentNode, (MongoInsertQuery)out);
                    break;
                case 25:
                    out = new MongoUpsertQuery();
                    buildInsert((LQUpsertNode)currentNode, (MongoUpsertQuery)out);
                    break;
                case 21:
                    out = new MongoUpdateQuery();
                    buildUpdate((LQUpdateNode)currentNode, (MongoUpdateQuery)out);
                    break;
                case 20:
                    out = new MongoDeleteQuery();
                    buildDelete((LQDeleteNode)currentNode, (MongoDeleteQuery)out);
                    break;
                case 60:
                    out = new MongoDropTable();
                    buildDrop((LQDropNode)currentNode, (MongoDropTable)out);
                    break;
                case 51:
                    out = new MongoCreateIndex();
                    buildCreateIndex((LQCreateIndexNode)currentNode, (MongoCreateIndex)out);
                    break;
                case 61:
                    out = new MongoDropIndex(((LQDropIndexNode)currentNode).getIndexName());
                    buildDropIndex((LQDropIndexNode)currentNode, (MongoDropIndex)out);
                    break;
                default:
                    throw new MongoBuilderUpstreamException("JDBC for MongoDB Driver: Got a query element we don't know how to deal with. Type " + currentNode + ".");
            }
        }

        return out;
    }

    public String toMongoString()
            throws MongoBuilderUpstreamException, MongoBuilderFatalException
    {
        return toMongoQuery().toMongoString();
    }

    public String toString()
    {
        try
        {
            return toMongoString();
        }
        catch (Exception e)
        {
        }
        return "";
    }
}