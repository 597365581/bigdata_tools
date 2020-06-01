package mongodb.jdbc;

import java.sql.SQLException;
import java.util.ResourceBundle;
import unity.engine.Relation;
import unity.generic.jdbc.ResultSetMetaDataImpl;

public class MongoResultSetMetaData extends ResultSetMetaDataImpl
{
    protected static ResourceBundle resources = ResourceBundle.getBundle("resources/mongo/MongoResultSetMetaData", locale);

    public MongoResultSetMetaData(Relation relation)
            throws SQLException
    {
        super(relation);
    }
}