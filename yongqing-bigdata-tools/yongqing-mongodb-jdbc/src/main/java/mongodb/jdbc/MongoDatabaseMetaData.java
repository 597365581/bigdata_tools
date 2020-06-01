//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package mongodb.jdbc;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import mongodb.conn.ServerConnection;
import unity.annotation.SourceField;
import unity.engine.IServerConnection;
import unity.generic.jdbc.DatabaseMetaDataImpl;
import unity.jdbc.LocalResultSet;

public class MongoDatabaseMetaData extends DatabaseMetaDataImpl {
    public MongoDatabaseMetaData(MongoConnection c, IServerConnection serverConnection) {
        super(c, serverConnection, new MongoDriver());
        super.caseSensitive = true;
        this.CATALOG = null;
    }

    public String getDatabaseProductName() throws SQLException {
        return "MongoDB";
    }

    public String getDriverName() throws SQLException {
        return "Mongo JDBC";
    }

    public int getDatabaseMajorVersion() throws SQLException {
        String version = ((ServerConnection)this.serverConnection).getVersion();
        return Integer.parseInt(version.substring(0, version.indexOf(".")));
    }

    public int getDatabaseMinorVersion() throws SQLException {
        String version = ((ServerConnection)this.serverConnection).getVersion();
        version = version.substring(version.indexOf(".") + 1);
        return Integer.parseInt(version.substring(0, version.indexOf(".")));
    }

    public String getDatabaseProductVersion() throws SQLException {
        return ((ServerConnection)this.serverConnection).getVersion();
    }

    public String getURL() throws SQLException {
        return this.serverConnection.getDatabase().getURL();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException
    {
        int mdsize = 13;
        String[] columns = new String[mdsize];
        ArrayList metadata = new ArrayList(mdsize);
        metadata.add(new SourceField(null, null, null, "TABLE_CAT", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 1, "YES"));
        metadata.add(new SourceField(null, null, null, "TABLE_SCHEM", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 2, "YES"));
        metadata.add(new SourceField(null, null, null, "TABLE_NAME", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 3, "YES"));
        metadata.add(new SourceField(null, null, null, "NON_UNIQUE", -7, "BOOLEAN", 1, 0, 0, 0, "", null, 0, 4, "YES"));
        metadata.add(new SourceField(null, null, null, "INDEX_QUALIFIER", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 5, "YES"));
        metadata.add(new SourceField(null, null, null, "INDEX_NAME", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 6, "YES"));
        metadata.add(new SourceField(null, null, null, "TYPE", 4, "INTEGER", 10, 0, 0, 0, "", null, 0, 7, "YES"));
        metadata.add(new SourceField(null, null, null, "ORDINAL_POSITION", 4, "INTEGER", 10, 0, 0, 0, "", null, 0, 8, "YES"));
        metadata.add(new SourceField(null, null, null, "COLUMN_NAME", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 9, "YES"));
        metadata.add(new SourceField(null, null, null, "ASC_OR_DESC", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 10, "YES"));
        metadata.add(new SourceField(null, null, null, "CARDINALITY", 4, "INTEGER", 10, 0, 0, 0, "", null, 0, 11, "YES"));
        metadata.add(new SourceField(null, null, null, "PAGES", 4, "INTEGER", 10, 0, 0, 0, "", null, 0, 12, "YES"));
        metadata.add(new SourceField(null, null, null, "FILTER_CONDITION", 12, "VARCHAR", 255, 0, 0, 0, "", null, 0, 13, "YES"));

        for (int i = 0; i < mdsize; i++) {
            columns[i] = ((SourceField)metadata.get(i)).getColumnName();
        }
        ArrayList data = new ArrayList();

        List idxList = ((ServerConnection)this.serverConnection).getIndexes(table);
        String schemaName = ((ServerConnection)this.serverConnection).getDatabase().getDatabaseName();

        for (Iterator i$ = idxList.iterator(); i$.hasNext(); ) {
            DBObject o = (DBObject)i$.next();

            Object key = o.get("key");

            if ((key instanceof BasicDBObject))
            {
                DBObject k = (BasicDBObject)key;
              int  count = 0;
                for (String s : k.keySet())
                {
                    boolean nonunique = !o.containsField("unique");
                    if ((!unique) || (!nonunique))
                    {
                        count++;
                        int type = 1;
                        String asc = "A";
                        Object val = k.get(s);
                        if ((val instanceof Number)) {
                            if (((Number)val).intValue() != 1) {
                                asc = "D";
                            }

                        }
                        else if (val.toString().equals("hashed")) {
                            type = 2;
                            asc = null;
                        }

                        ArrayList idx = new ArrayList(13);
                        idx.add(null);
                        idx.add(schemaName);
                        idx.add(table);
                        idx.add(Boolean.valueOf(nonunique));
                        idx.add(null);
                        idx.add(o.get("name"));
                        idx.add(Integer.valueOf(type));
                        idx.add(Integer.valueOf(count));
                        idx.add(s);
                        idx.add(asc);
                        idx.add(Integer.valueOf(0));
                        idx.add(Integer.valueOf(0));
                        idx.add(null);
                        data.add(idx);
                    }
                }
            }
        }
        DBObject o;
        BasicDBObject k;
        int count;
        Collections.sort(data, new MongoDatabaseMetaData.IndexComparator());

        return new LocalResultSet(data, columns, metadata);
    }

    private class IndexComparator implements Comparator<ArrayList<?>> {
        private IndexComparator() {
        }

        public int compare(ArrayList<?> o1, ArrayList<?> o2) {
            boolean nuq1 = ((Boolean)o1.get(3)).booleanValue();
            boolean nuq2 = ((Boolean)o2.get(3)).booleanValue();
            if(!nuq1 && nuq2) {
                return -1;
            } else if(nuq1 && !nuq2) {
                return 1;
            } else {
                int type1 = ((Integer)o1.get(6)).intValue();
                int type2 = ((Integer)o2.get(6)).intValue();
                if(type1 != type2) {
                    return type1 - type2;
                } else {
                    String name1 = (String)o1.get(5);
                    String name2 = (String)o2.get(5);
                    if(name1 != name2) {
                        return name1.compareTo(name2);
                    } else {
                        int pos1 = ((Integer)o1.get(7)).intValue();
                        int pos2 = ((Integer)o2.get(7)).intValue();
                        return pos1 != pos2?pos1 - pos2:0;
                    }
                }
            }
        }
    }
}
