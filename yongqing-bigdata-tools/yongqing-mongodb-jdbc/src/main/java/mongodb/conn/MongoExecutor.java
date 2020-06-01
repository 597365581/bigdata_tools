//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package mongodb.conn;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import mongodb.jdbc.MongoResultSet;
import mongodb.jdbc.MongoStatement;
import mongodb.query.MongoBuilder;
import mongodb.query.MongoSelectQuery;
import org.bson.types.ObjectId;
import unity.annotation.AnnotatedSourceField;
import unity.annotation.AnnotatedSourceForeignKey;
import unity.annotation.AnnotatedSourceTable;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceDatabase;
import unity.annotation.SourceField;
import unity.annotation.SourceKey;
import unity.annotation.SourceTable;
import unity.engine.Attribute;
import unity.engine.Relation;
import unity.engine.TableData;
import unity.engine.Tuple;
import unity.functions.Expression;
import unity.jdbc.LocalResultSet;
import unity.jdbc.UnityDriver;
import unity.operators.Operator;
import unity.parser.GlobalParser;
import unity.predicates.SelectionPredicate;
import unity.query.GQFieldRef;
import unity.query.GlobalQuery;
import unity.query.LQCondNode;
import unity.query.LQNode;
import unity.query.LQTree;
import unity.query.SubQuery;
import unity.util.StringFunc;

public class MongoExecutor {
    private boolean resultSetComplete;
    private DBCursor cursor;
    private Relation relation;
    private ArrayList<DBObject> cachedObjects;
    private long statementId;
    private DB db;
    private MongoSelectQuery mq;
    private DBObject currentObject;
    private boolean flattening = false;
    private ArrayList<String> nestedFields = null;
    private SelectionPredicate predicate = null;
    private Iterator<Object>[] iterators;
    private int numIterators;
    private String[] components = null;
    private String fieldName = null;
    private BasicDBObject flattenedObject;
    private double SAMPLE_FRACTION = 0.001D;
    private static final int MIN_SIZE_TO_SAMPLE = 10;
    private int numOutput;
    private int rowNum;

    public MongoExecutor(DB db, long statementId) {
        this.db = db;
        this.statementId = statementId;
        this.mq = null;
    }

    public ResultSet execute(String sql, int resultSetType, MongoStatement stmt, GlobalSchema schema, ServerConnection conn, boolean schemaValidation) throws Exception {
        sql = StringFunc.verifyTerminator(sql);
        GlobalParser kingParser = new GlobalParser(false, schemaValidation);
        GlobalQuery gq = kingParser.parse(sql, schema);
        return this.execute(gq, resultSetType, stmt, schema, conn);
    }

    public ResultSet execute(GlobalQuery gq, int resultSetType, MongoStatement stmt, GlobalSchema schema, ServerConnection conn) throws Exception {
        LQTree lqtree = gq.getLogicalQueryTree();
        if(UnityDriver.DEBUG) {
            System.out.println("Logical query tree: \n");
            lqtree.print();
        }

        UnityDriver.debug("SQL query: " + gq.getQueryString());
        MongoBuilder m = new MongoBuilder(lqtree.getRoot());
        this.mq = (MongoSelectQuery)m.toMongoQuery();
        stmt.setQuery(this.mq);
        this.flattening = this.mq.isFlattening();
        this.nestedFields = this.mq.getNestedFields();
        int count;
        if(this.mq.aggregateQuery && this.nestedFields != null) {
            this.flattening = false;
        } else if(this.nestedFields != null) {
            this.fieldName = (String)this.nestedFields.get(0);
            this.components = StringFunc.divideId(this.fieldName);
            this.iterators = new Iterator[this.components.length];
            this.numIterators = this.components.length;
            ArrayList<LQCondNode> filters = this.mq.getConditions();
            if(filters != null && filters.size() > 0) {
                LQCondNode root = (LQCondNode)filters.get(0);

                for(count = 1; count < filters.size(); ++count) {
                    LQCondNode newRoot = new LQCondNode();
                    newRoot.setType(111);
                    newRoot.addChild(root);
                    newRoot.addChild((LQNode)filters.get(count));
                    root = newRoot;
                }

                this.predicate = root.buildSelectionPredicate(this.mq.getRelation(), new GlobalQuery(), (SubQuery)null, (Operator)null);
            }
        }

        UnityDriver.debug(this.mq.toString());
        Object result = this.mq.run(this.db);
        int i;
        if(result instanceof Integer) {
            i = ((Integer)result).intValue();
            ArrayList<ArrayList<Object>> data = new ArrayList();
            ArrayList<Object> row = new ArrayList(1);
            row.add(Integer.valueOf(i));
            data.add(row);
            ArrayList<SourceField> metadata = new ArrayList(1);
            metadata.add(new SourceField((String)null, (String)null, (String)null, (String)this.mq.fieldNames.get("Count(*)"), 4, "INT", 4, 0, 0, 0, "", (String)null, 0, 1, "YES"));
            return new LocalResultSet(data, new String[]{(String)this.mq.fieldNames.get("Count(*)")}, metadata);
        } else {
            this.cachedObjects = new ArrayList();
            if(result instanceof DBCursor) {
                this.cursor = (DBCursor)result;

                for(i = 0; i < 3 && this.cursor.hasNext(); ++i) {
                    DBObject dbo = this.cursor.next();
                    this.cachedObjects.add(dbo);
                }

                this.relation = this.mq.getRelation();
                if(this.relation == null) {
                    this.relation = this.buildRelation(this.mq, this.cachedObjects);
                }
            } else {
                Iterator it;
                if(result instanceof ArrayList && this.mq.aggregateQuery) {
                    this.cursor = null;
                    it = ((ArrayList)result).iterator();
                    count = 0;
                    int limit = 2147483647;
                    int offset = 0;
                    if(this.mq.limit != null) {
                        limit = this.mq.limit.intValue();
                    }

                    if(this.mq.offset != null) {
                        offset = this.mq.offset.intValue();
                    }

                    while(it.hasNext()) {
                        ++count;
                        if(count > limit + offset) {
                            break;
                        }

                        Object o = it.next();
                        if(count > offset) {
                            DBObject dbo = (DBObject)o;
                            this.cachedObjects.add(dbo);
                        }
                    }

                    this.relation = this.mq.getRelation();
                    if(this.relation == null || this.relation.getAttribute(0) == null) {
                        this.relation = this.buildRelation(this.mq, this.cachedObjects);
                    }
                } else if(result instanceof ArrayList) {
                    this.cursor = null;
                    it = ((ArrayList)result).iterator();

                    while(it.hasNext()) {
                        Object o = it.next();
                        DBObject dbo = new BasicDBObject(this.mq.distinctField.toString(), o);
                        this.cachedObjects.add(dbo);
                    }

                    this.relation = this.buildDistinctRelation(this.mq, this.cachedObjects);
                }
            }

            TableData rows = new TableData(conn, resultSetType != 1003, this.statementId);
            rows.setRelation(this.relation);
            return new MongoResultSet(rows, this.relation, resultSetType, stmt);
        }
    }

    public void close() {
        if(this.cursor != null) {
            this.cursor.close();
        }

    }

    private static DBObject flattenObjectSchema(SourceDatabase database, SourceTable table, DBObject dbo, String parentKey, DBObject out, boolean flattenList, HashMap<String, Subtable> subtables, Subtable parentTable) {
        String pkey = "";
        if(parentKey != null) {
            pkey = parentKey + '.';
        }

        Iterator i$;
        if(dbo instanceof BasicDBList) {
            if(!flattenList) {
                return out;
            } else {
                BasicDBList list = (BasicDBList)dbo;
                parentTable = buildNestedTable(list, database, table, parentKey, subtables, parentTable);
                i$ = list.iterator();

                while(i$.hasNext()) {
                    Object o = i$.next();
                    if(o instanceof DBObject) {
                        out = flattenObjectSchema(database, table, (DBObject)o, parentKey, out, flattenList, subtables, parentTable);
                    }
                }

                Object listElement = null;
                if(list.size() > 0) {
                    listElement = list.get(0);
                }

                out.put(pkey + "[0-" + 2147483647 + "]", listElement);
                return out;
            }
        } else {
            Set<String> keySet = dbo.keySet();
            i$ = keySet.iterator();

            while(true) {
                Object value;
                String fkey;
                do {
                    if(!i$.hasNext()) {
                        return out;
                    }

                    String key = (String)i$.next();
                    value = dbo.get(key);
                    fkey = pkey + key;
                } while(value instanceof BasicDBList && !flattenList);

                out.put(fkey, value);
                if(value instanceof DBObject) {
                    out = flattenObjectSchema(database, table, (DBObject)value, fkey, out, flattenList, subtables, parentTable);
                }
            }
        }
    }

    public ResultSet execute(String sql, int resultSetType, MongoStatement stmt, GlobalSchema schema, ServerConnection conn) throws Exception {
        boolean schemaValidation = false;
        String validation = stmt.getConnection().getClientInfo("validation");
        if(validation != null && validation.equalsIgnoreCase("strict")) {
            schemaValidation = true;
        }

        return this.execute(sql, resultSetType, stmt, schema, conn, schemaValidation);
    }

    public Attribute[] getSchema(SourceDatabase database, SourceTable table, DBCollection collection, HashMap<String, Subtable> subtables) {
        Attribute[] ret = null;
        int count = this.numberToSample(collection);
        if(count != -1) {
            ret = guessFromRandomRecords(collection, database, table, this.numberToSample(collection), subtables);
        } else {
            ret = guessFromAllRecords(collection, database, table, subtables);
        }

        return ret;
    }

    public void setSampleFraction(double fraction) {
        this.SAMPLE_FRACTION = fraction;
        if(this.SAMPLE_FRACTION <= 0.0D || this.SAMPLE_FRACTION > 1.0D) {
            this.SAMPLE_FRACTION = 0.001D;
        }

    }

    private int numberToSample(DBCollection collection) {
        long count = -1L;

        try {
            count = collection.count();
        } catch (Exception var5) {
            System.out.println("Error while sampling collection: " + collection.getName());
        }

        return count <= 10L?-1:(int)Math.ceil((double)count * this.SAMPLE_FRACTION);
    }

    private static Subtable buildNestedTable(BasicDBList list, SourceDatabase database, SourceTable rootTable, String listName, HashMap<String, Subtable> subtables, Subtable parentTable) {
        char separator_char = 95;
        String parentTableName = StringFunc.undelimitName(rootTable.getTableName(), '"');
        String tblName = StringFunc.delimitName(parentTableName + separator_char + listName);
        tblName = tblName.replaceAll("\\.", "_");
        Subtable subtable = (Subtable)subtables.get(tblName);
        if(subtable == null) {
            subtable = new Subtable();
            subtable.name = tblName;
            subtable.parentName = parentTableName;
            subtable.listName = listName;
            subtable.parentTable = parentTable;
            subtable.rootTable = rootTable;
            subtable.valueType = 0;
            subtables.put(tblName, subtable);
        }

        int valueType = subtable.valueType;
        if(list.size() > 0 && valueType == 0) {
            valueType = Attribute.getType(list.get(0));
        }

        Attribute[] ret = subtable.attr;
        Iterator i$ = list.iterator();

        while(i$.hasNext()) {
            Object o = i$.next();
            int type = Attribute.getType(o);
            if(valueType != type) {
                valueType = 12;
            }

            if(o instanceof DBObject && !(o instanceof BasicDBList)) {
                BasicDBObject result = (BasicDBObject)flattenObjectSchema(database, rootTable, (DBObject)o, (String)null, new BasicDBObject(), false, subtables, parentTable);
                ret = supersetSchema(guessSchema(result.toMap(), (String)null, (MongoSelectQuery)null), ret);
            }
        }

        subtable.attr = ret;
        subtable.valueType = valueType;
        return subtable;
    }

    private static void addAttributes(ArrayList<Attribute> result, Subtable parentSubtable) {
        if(parentSubtable != null) {
            addAttributes(result, parentSubtable.parentTable);
            Attribute[] arr$ = parentSubtable.attr;
            int len$ = arr$.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                Attribute a = arr$[i$];
                result.add(a);
                a.setReference(parentSubtable.listName + "." + StringFunc.undelimitName(a.getName(), '"'));
            }

        }
    }

    public static SourceTable buildNestedTable(Subtable subtable) {
        SourceTable parentTable = subtable.rootTable;
        String parentTableName = subtable.parentName;
        String tableName = subtable.name;
        String listName = subtable.listName;
        int valueType = subtable.valueType;
        SourceTable table = new AnnotatedSourceTable((String)null, (String)null, tableName, (String)null, (HashMap)null, (SourceKey)null);
        table.setParentDatabase(parentTable.getParentDatabase());
        table.setCaseSensitive(true);
        table.setProperty("nested", parentTableName);
        ArrayList<String> nestedFields = new ArrayList();
        nestedFields.add(listName);
        table.setProperty("nestedFields", nestedFields);
        HashMap<String, SourceField> fields = new HashMap();

        SourceField keyField = null;
        ArrayList<SourceField> keyFields = new ArrayList();
        Iterator it = parentTable.getSourceFields().entrySet().iterator();

        while(it.hasNext()) {
            Entry<String, SourceField> e = (Entry)it.next();
            SourceField sfp = (SourceField)e.getValue();
            if(sfp.getDataType() != 2003 && !sfp.getColumnName().contains("[0-")) {
                SourceField newFieldFromParent = new AnnotatedSourceField(sfp);
                newFieldFromParent.setParentTable(table);
                newFieldFromParent.setTableName(tableName);
                fields.put(sfp.getColumnName(), newFieldFromParent);
                keyFields.clear();
                keyFields.add(sfp);
                if(parentTable.isPrimaryKey(keyFields)) {
                    keyField = newFieldFromParent;
                }
            }
        }

        int colPos = fields.size() + 1;
        if(valueType != -2000) {
            SourceField sf = new AnnotatedSourceField();
            sf.setColumnName(StringFunc.delimitName(listName));
            sf.setProperty("systemname", listName);
            sf.setTableName(tableName);
            sf.setParentTable(table);
            if(valueType == 0) {
                valueType = 12;
            }

            sf.setDataType(valueType);
            sf.setDataTypeName(Attribute.getTypeName(valueType));
            SourceField.setSizeByType(sf, 16793600);
            sf.setOrdinalPosition(colPos++);
            fields.put(sf.getColumnName(), sf);
        }

        ArrayList<Attribute> result = new ArrayList();
        addAttributes(result, subtable.parentTable);
        if(subtable.attr != null) {
            Attribute[] arr$ = subtable.attr;
            int len$ = arr$.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                Attribute a = arr$[i$];
                result.add(a);
                a.setReference(listName + "." + StringFunc.undelimitName(a.getName(), '"'));
            }
        }

        Iterator i$ = result.iterator();

        while(i$.hasNext()) {
            Attribute a = (Attribute)i$.next();
            if(a == null) {
                System.out.println("Issue with a NULL attribute.");
            } else {
                AnnotatedSourceField f = new AnnotatedSourceField();
                f.setColumnName(StringFunc.delimitName(a.getName()));
                String systemName = (String)a.getReference();
                f.setProperty("systemname", systemName);
                f.setDataTypeName(Attribute.getTypeName(a.getType()));
                f.setTableName(tableName);
                f.setDataType(a.getType());
                SourceField.setSizeByType(f, 16793600);
                f.setOrdinalPosition(colPos++);
                f.setSemanticFieldName(StringFunc.undelimitName(a.getName(), '"').toLowerCase());
                fields.put(a.getName(), f);
                nestedFields.add(StringFunc.undelimitName(a.getName(), '"'));
            }
        }

        table.setSourceFields(fields);
        if(keyField != null) {
            keyFields.clear();
            keyFields.add(keyField);
            ArrayList<String> fieldNames = new ArrayList();
            fieldNames.add(keyField.getColumnName());
            AnnotatedSourceForeignKey sfk = new AnnotatedSourceForeignKey(table, keyFields, fieldNames, "fk_" + parentTableName, parentTableName);
            sfk.setToSourceTable(parentTable);
            sfk.setToKey(parentTable.getPrimaryKey());
            table.addForeignKey(sfk);
        }

        return table;
    }

    private static Attribute[] guessFromAllRecords(DBCollection collection, SourceDatabase database, SourceTable table, HashMap<String, Subtable> subtables) {
        UnityDriver.debug("Building schema for collection " + collection.getName() + " sampling all records.");
        DBCursor c = collection.find().limit(0);

        Attribute[] ret;
        BasicDBObject result;
        for(ret = null; c.hasNext(); ret = supersetSchema(guessSchema(result.toMap(), (String)null, (MongoSelectQuery)null), ret)) {
            result = (BasicDBObject)c.next();
            result = (BasicDBObject)flattenObjectSchema(database, table, result, (String)null, new BasicDBObject(), true, subtables, (Subtable)null);
        }

        c.close();
        return ret;
    }

    private static Attribute[] guessFromRandomRecords(DBCollection collection, SourceDatabase database, SourceTable table, int number, HashMap<String, Subtable> subtables) {
        long count = collection.count();
        int startAt = (int)Math.round(Math.random() * (double)(count - (long)number));
        if(startAt < 0) {
            startAt = 0;
        }

        long maxCount = (long)number;
        if(count < (long)number) {
            maxCount = count;
        }

        UnityDriver.debug("Building schema for collection " + collection.getName() + " sampling " + number + " documents in range " + startAt + " to " + ((long)startAt + maxCount));
        Attribute[] ret = null;
        DBCursor c = collection.find().skip(startAt);

        for(int i = 0; (long)i < maxCount; ++i) {
            BasicDBObject result = (BasicDBObject)c.next();
            result = (BasicDBObject)flattenObjectSchema(database, table, result, (String)null, new BasicDBObject(), true, subtables, (Subtable)null);
            ret = supersetSchema(guessSchema(result.toMap(), (String)null, (MongoSelectQuery)null), ret);
        }

        c.close();
        return ret;
    }

    public static Attribute[] guessSchema(Map<String, Object> map, String prefix) {
        return guessSchema(map, prefix, (MongoSelectQuery)null);
    }

    public static Attribute[] guessSchema(Map<String, Object> map, String prefix, MongoSelectQuery mq) {
        int i = 0;
        Attribute[] attr = new Attribute[map.size()];

        int type;
        String fieldName;
        String aliasReference;
        int len;
        for(Iterator i$ = map.entrySet().iterator(); i$.hasNext(); attr[i++] = new Attribute(fieldName, type, len, aliasReference)) {
            Entry<String, Object> entry = (Entry)i$.next();
            String key = (String)entry.getKey();
            if(prefix != null) {
                key = prefix + key;
            }

            Object value = entry.getValue();
            type = Attribute.getType(value);
            if(checkParent(map, key) && !key.contains("[")) {
                type = 2003;
            }

            if(type == 91) {
                type = 93;
            }

            fieldName = key;
            aliasReference = null;
            if(mq != null) {
                Object tmp = mq.fieldNames.get(key);
                if(tmp != null) {
                    aliasReference = key;
                    fieldName = (String)tmp;
                }
            } else {
                fieldName = StringFunc.delimitName(key);
            }

            len = 0;
            if(value instanceof ObjectId) {
                len = 24;
            } else if(Attribute.isStringType(type)) {
                len = 16793600;
            }
        }

        return attr;
    }

    private static boolean checkParent(Map<String, Object> map, String key) {
        String[] components = StringFunc.divideId(key);
        BasicDBObject obj = new BasicDBObject();
        obj.putAll(map);

        for(int i = components.length - 1; i >= 0; --i) {
            String k = StringFunc.idHashKeyNoDelim(components, 0, i);
            Object val = getValue(obj, k);
            if(val instanceof BasicDBList) {
                return true;
            }
        }

        return false;
    }

    private static int getLeastGeneralType(int type_a, int type_b) {
        return type_a;
    }

    public static Attribute[] supersetSchema(Attribute[] a_list, Attribute[] b_list)
    {
        if (a_list == null)
            return b_list;
        if (b_list == null) {
            return a_list;
        }

        ArrayList ret = new ArrayList(Arrays.asList(b_list));

        int i = 0;
        for (Attribute a : a_list)
        {
            if (a != null)
            {
                boolean found = false;
                for (Attribute b : b_list)
                {
                    if ((b != null) && (a.getName().equals(b.getName())))
                    {
                        if (a.getType() != b.getType())
                        {
                            b.setType(getLeastGeneralType(a.getType(), b.getType()));
                        }

                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    ret.add(i, a);
                }

                i++;
            }
        }
        return (Attribute[])ret.toArray(new Attribute[ret.size()]);
    }

    public Attribute[] buildAttributeList(MongoSelectQuery mq1, ArrayList<DBObject> objs) {
        LinkedHashMap mp;
        if(this.mq.fieldNames != null && this.mq.fieldNames.size() > 0) {
            mp = new LinkedHashMap();
            DBObject obj = null;
            if(objs != null && objs.size() > 0) {
                obj = (DBObject)objs.get(0);
            }

            Iterator i$ = mq1.fieldNames.keySet().iterator();

            while(i$.hasNext()) {
                String key = (String)i$.next();
                if(obj != null) {
                    Object value = getValue(obj, key);
                    mp.put(key, value);
                } else {
                    mp.put(key, "");
                }
            }
        } else {
            if(objs == null || objs.size() <= 0) {
                Attribute[] attr = new Attribute[]{new Attribute("No_Results_Returned", 12, 50)};
                return attr;
            }

            mp = new LinkedHashMap();
            Iterator i$ = objs.iterator();

            while(i$.hasNext()) {
                DBObject o = (DBObject)i$.next();
                mp.putAll((LinkedHashMap)o);
            }
        }

        return guessSchema(mp, (String)null, mq1);
    }

    public Relation buildRelation(MongoSelectQuery mq1, ArrayList<DBObject> objs) {
        return new Relation(this.buildAttributeList(mq1, objs));
    }

    public Relation buildDistinctRelation(MongoSelectQuery mq1, ArrayList<DBObject> objs) {
        int type = 0;
        Attribute[] attr = new Attribute[1];
        if(objs.isEmpty()) {
            type = Attribute.getType((Object)null);
        } else {
            DBObject o = (DBObject)objs.get(0);
            if(o instanceof BasicDBObject) {
                Object o2 = o.get(mq1.distinctField);
                if(o2 != null) {
                    type = Attribute.getType(o2);
                }
            }
        }

        attr[0] = new Attribute(this.mq.distinctField, type, -1, this.mq.fieldNames.get(this.mq.distinctField));
        return new Relation(attr);
    }

    private void addBaseFieldsToObject(BasicDBObject target, BasicDBObject source, String prefix) {
        Set<String> set = source.keySet();
        Iterator it = set.iterator();

        while(it.hasNext()) {
            String k = (String)it.next();
            Object obj = source.get(k);
            if(!(obj instanceof BasicDBList)) {
                if(prefix != null) {
                    k = prefix + "." + k;
                }

                target.put(k, obj);
            }
        }

    }

    public boolean next(Tuple t)
            throws SQLException
    {
        if (this.resultSetComplete) {
            return false;
        }

        if ((UnityDriver.isExpiredTrial()) && (this.numOutput >= UnityDriver.getMaxResults()))
        {
            System.out.println(UnityDriver.i18n.getString("ResultSet.MaxRows") + UnityDriver.getMaxResults() + UnityDriver.i18n.getString("ResultSet.TrialVersion"));

            return false;
        }

        try
        {
            while (true)
            {
                if (this.currentObject == null)
                {
                    this.currentObject = getNextObject();

                    if (this.currentObject == null) {
                        return false;
                    }

                    if (this.flattening)
                    {
                        this.flattenedObject = new BasicDBObject();

                        addBaseFieldsToObject(this.flattenedObject, (BasicDBObject)this.currentObject, null);

                        this.numIterators = 0;
                        Object obj = this.currentObject.get(this.components[0]);

                        int i = 0;
                        while ((obj instanceof BasicDBObject))
                        {
                            addBaseFieldsToObject(this.flattenedObject, (BasicDBObject)obj, StringFunc.combineComponents(this.components, 0, i + 1));
                            i++;
                            obj = ((BasicDBObject)obj).get(this.components[i]);
                        }
                        if (((obj instanceof BasicDBList)) && (i < this.components.length))
                        {
                            this.iterators[(this.numIterators++)] = ((BasicDBList)obj).iterator();
                            i++;
                            int curIterator = 0;
                            for (; i < this.components.length; i++)
                            {
                                curIterator++;
                                if (!this.iterators[(curIterator - 1)].hasNext())
                                    break;
                                Object o = this.iterators[(curIterator - 1)].next();
                                if ((o instanceof BasicDBList)) {
                                    this.iterators[curIterator] = ((BasicDBList)obj).iterator();
                                    this.numIterators += 1;
                                } else {
                                    if (!(o instanceof BasicDBObject)) break;
                                    obj = ((BasicDBObject)o).get(this.components[i]);
                                    addBaseFieldsToObject(this.flattenedObject, (BasicDBObject)o, StringFunc.combineComponents(this.components, 0, i));

                                    if ((obj instanceof BasicDBList)) {
                                        this.iterators[curIterator] = ((BasicDBList)obj).iterator();
                                        this.numIterators += 1;
                                    }

                                }

                            }

                        }

                    }

                }

                if ((!this.flattening) || (this.numIterators <= 0) || (this.iterators[0] == null))
                    break;
                int size = this.numIterators;
                while (true)
                {
                    if ((this.iterators[(size - 1)] != null) && (this.iterators[(size - 1)].hasNext())) {
                        Object obj = this.iterators[(size - 1)].next();

                        BasicDBObject copyObject = new BasicDBObject(this.flattenedObject);
                        if ((obj instanceof BasicDBObject))
                        {
                            addBaseFieldsToObject(copyObject, (BasicDBObject)obj, this.fieldName);
                            copyObject.put(this.fieldName, obj);
                        }
                        else
                        {
                            copyObject.put(this.fieldName, obj);
                        }

                        convertToTuple(copyObject, this.relation, t);

                        if ((this.predicate == null) || (this.predicate.evaluate(t)))
                        {
                            this.rowNum += 1;

                            if ((this.mq.offset != null) && (this.rowNum <= this.mq.offset.intValue()))
                                continue;
                            if ((this.mq.limit != null) && (this.numOutput >= this.mq.limit.intValue())) {
                                return false;
                            }

                            this.numOutput += 1;
                            return true;
                        }
                    }
                    else
                    {
                        int pos = size - 2;
                        boolean success = false;
                        while (pos >= 0) {
                            if (this.iterators[pos].hasNext())
                            {
                                Object obj = this.iterators[pos].next();
                                if ((obj instanceof BasicDBList))
                                {
                                    this.iterators[(++pos)] = ((BasicDBList)obj).iterator();
                                    success = true;
                                    break;
                                }
                                if (!(obj instanceof BasicDBObject))
                                    break;
                                addBaseFieldsToObject(this.flattenedObject, (BasicDBObject)obj, StringFunc.combineComponents(this.components, 0, pos + 1));

                                obj = ((BasicDBObject)obj).get(this.components[(pos + 1)]);
                                if ((obj instanceof BasicDBList))
                                    this.iterators[(++pos)] = ((BasicDBList)obj).iterator();
                                success = true;
                                break;
                            }

                            pos--;
                        }

                        if (!success) {
                            this.currentObject = null;
                            break;
                        }

                        for (int i = pos + 1; i < size; i++)
                        {
                            if (!this.iterators[(i - 1)].hasNext())
                                break;
                            Object o = this.iterators[(i - 1)].next();
                            if ((o instanceof BasicDBList)) {
                                this.iterators[i] = ((BasicDBList)o).iterator(); } else {
                                if (!(o instanceof BasicDBObject)) break;
                                Object obj = ((BasicDBObject)o).get(this.components[i]);
                                if ((obj instanceof BasicDBList)) {
                                    this.iterators[i] = ((BasicDBList)obj).iterator();
                                }

                            }

                        }

                    }

                }

            }

            convertToTuple(this.currentObject, this.relation, t);
            this.currentObject = null;

            this.numOutput += 1;
            return true;
        }
        catch (Exception e)
        {
            throw new SQLException(e);
        }
    }

    private DBObject getNextObject() {
        DBObject obj = null;
        if(this.cachedObjects.size() > 0) {
            obj = (DBObject)this.cachedObjects.get(0);
            this.cachedObjects.remove(0);
            return obj;
        } else if(this.cursor != null && this.cursor.hasNext()) {
            obj = this.cursor.next();
            return obj;
        } else {
            return null;
        }
    }

    private static Object getValue(DBObject dbo, String key) {
        Object obj = null;
        if(dbo instanceof BasicDBObject) {
            obj = dbo.get(key);
        }

        if(obj != null) {
            return obj;
        } else {
            String[] components = StringFunc.divideId(key);
            if(components.length == 1) {
                return null;
            } else {
                int start = 0;
                int end = components.length;

                for(end = components.length - 1; end >= 1; --end) {
                    String tmpkey = StringFunc.idHashKeyNoDelim(components, start, end);
                    obj = dbo.get(tmpkey);
                    if(obj != null) {
                        break;
                    }
                }

                if(obj == null) {
                    return null;
                } else {
                    while(end < components.length) {
                        if(!(obj instanceof BasicDBObject)) {
                            if(obj instanceof BasicDBList) {
                                BasicDBList newList;
                                try {
                                    int i = Integer.parseInt(components[end]);
                                    newList = (BasicDBList)obj;
                                    if(i >= 0 && i < newList.size()) {
                                        if(end == components.length - 1) {
                                            return newList.get(i);
                                        }

                                        Object o = newList.get(i);
                                        if(o instanceof DBObject) {
                                            return getValue((DBObject)newList.get(i), StringFunc.idHashKeyNoDelim(components, end + 1, components.length));
                                        }

                                        return null;
                                    }

                                    return null;
                                } catch (Exception var13) {
                                    System.out.println(var13);
                                    BasicDBList dbl = (BasicDBList)obj;
                                    newList = new BasicDBList();
                                    String restKey = StringFunc.idHashKeyNoDelim(components, end, components.length);
                                    Iterator i$ = dbl.iterator();

                                    while(i$.hasNext()) {
                                        Object o = i$.next();
                                        Object o2 = null;
                                        if(o instanceof DBObject) {
                                            o2 = getValue((DBObject)o, restKey);
                                        }

                                        if(o2 != null) {
                                            newList.add(o2);
                                        }
                                    }

                                    return newList;
                                }
                            }

                            return null;
                        }

                        DBObject dbobj = (DBObject)obj;
                        obj = dbobj.get(components[end]);
                        if(obj == null) {
                            return null;
                        }

                        ++end;
                    }

                    return obj;
                }
            }
        }
    }

    public void convertToTuple(DBObject obj, Relation r, Tuple t) throws SQLException {
        int num = r.getNumAttributes();
        Object[] objArray = t.getValues();
        if(objArray == null || objArray.length != num) {
            objArray = new Object[num];
        }

        for(int i = 0; i < r.getNumAttributes(); ++i) {
            Attribute a = r.getAttribute(i);
            Object ref = a.getReference();
            if(ref != null) {
                if(ref instanceof Expression) {
                    objArray[i] = ((Expression)ref).evaluate(t);
                } else {
                    String sysName = ref.toString();
                    if(ref instanceof GQFieldRef) {
                        GQFieldRef gqfref = (GQFieldRef)ref;
                        if(gqfref.getField() != null) {
                            sysName = gqfref.getField().getSystemName();
                            sysName = StringFunc.removeAllQuotes(sysName);
                        }
                    }

                    objArray[i] = getValue(obj, sysName);
                }
            } else {
                Object tObject = getValue(obj, r.getAttribute(i).getName());
                if(tObject instanceof BasicDBList) {
                    objArray[i] = new ArrayList((BasicDBList)tObject);
                } else if(tObject instanceof BasicDBObject) {
                    objArray[i] = (BasicDBObject)tObject;
                } else {
                    objArray[i] = tObject;
                }
            }
        }

        t.setValues(objArray);
    }
}
