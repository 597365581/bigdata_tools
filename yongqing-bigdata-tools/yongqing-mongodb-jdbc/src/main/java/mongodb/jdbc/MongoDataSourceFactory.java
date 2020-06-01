package mongodb.jdbc;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

public class MongoDataSourceFactory
        implements ObjectFactory
{
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment)
            throws Exception
    {
        Reference ref = (Reference)obj;
        String className = ref.getClassName();

        if ((className != null) && ((className.equals("mongodb.jdbc.MongoDataSource")) || (className.equals("mongodb.jdbc.MongoXADataSource"))))
        {
            MongoDataSource ds = null;
            try
            {
                ds = (MongoDataSource)Class.forName(className).newInstance();
            } catch (Exception ex) {
                throw new RuntimeException("Unable to create DataSource of class '" + className + "', reason: " + ex.toString());
            }

            ds.setUrl(getRefString("url", ref));
            ds.setPassword(getRefString("password", ref));
            ds.setUser(getRefString("user", ref));

            return ds;
        }

        return null;
    }

    private String getRefString(String name, Reference ref)
    {
        RefAddr ra = ref.get(name);
        return ra != null ? (String)ra.getContent() : null;
    }
}