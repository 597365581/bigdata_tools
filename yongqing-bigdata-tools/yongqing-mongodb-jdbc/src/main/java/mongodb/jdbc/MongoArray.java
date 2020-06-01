//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package mongodb.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import unity.engine.Attribute;
import unity.engine.IServerConnection;
import unity.engine.Relation;
import unity.jdbc.LocalResultSet;

public class MongoArray<V> implements Array {
    Map<String, V> arrayMap = null;
    String name = null;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MongoArray(V[] elements)
    {
        if (elements.length < 0) {
            return;
        }
        this.arrayMap = new HashMap();

        for (int idx = 0; idx < elements.length; idx++)
        {
            V obj = elements[idx];
            this.arrayMap.put(Integer.toString(idx), obj);
        }
    }

    public void free() throws SQLException {
        this.arrayMap = null;
    }

    public Object getArray() throws SQLException {
        return this.arrayMap.values().toArray();
    }

    public Object getArray(Map<String, Class<?>> arg0) throws SQLException {
        return null;
    }

    public Object getArray(long arg0, int arg1) throws SQLException {
        return null;
    }

    public Object getArray(long arg0, int arg1, Map<String, Class<?>> arg2) throws SQLException {
        return null;
    }

    public int getBaseType() throws SQLException {
        return -2000;
    }

    public String getBaseTypeName() throws SQLException {
        return Attribute.getTypeName(this.getBaseType());
    }

    public ResultSet getResultSet()
            throws SQLException
    {
        Attribute[] attr = new Attribute[1];
        attr[0] = new Attribute(getName(), getBaseType(), -1);

        Relation arrayRelation = new Relation(attr);

        String[] columnName = new String[1];
        columnName[0] = getName();

        MongoTableData rows = new MongoTableData(null, true, 0);

        rows.setRelation(arrayRelation);

        ArrayList results = new ArrayList();

        for (Iterator i$ = this.arrayMap.values().iterator(); i$.hasNext(); ) { Object entry = i$.next();

            ArrayList obj = new ArrayList();
            obj.add(entry);
            results.add(obj);
        }

        LocalResultSet localRS = new LocalResultSet(results, columnName, null);

        return localRS;
    }

    public ResultSet getResultSet(long arg0, int arg1) throws SQLException {
        throw new SQLException("notSupported");
    }

    public ResultSet getResultSet(long arg0, int arg1, Map<String, Class<?>> arg2) throws SQLException {
        throw new SQLException("notSupported");
    }

    public String toString() {
        return this.arrayMap.entrySet().toString();
    }

    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("notSupported");
    }
}
