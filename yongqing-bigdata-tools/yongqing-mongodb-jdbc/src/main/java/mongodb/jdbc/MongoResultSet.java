package mongodb.jdbc;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ResourceBundle;
import unity.engine.Relation;
import unity.engine.TableData;
import unity.engine.Tuple;
import unity.generic.jdbc.ResultSetImpl;
import unity.generic.jdbc.ResultSetMetaDataImpl;
import unity.jdbc.UnityDriver;

public class MongoResultSet extends ResultSetImpl
{
    protected static ResourceBundle resources = ResourceBundle.getBundle("resources/mongo/MongoResultSet", locale);

    public MongoResultSet(TableData rows, Relation relation, int type, MongoStatement stmt)
            throws SQLException
    {
        this.relation = relation;
        this.meta = new MongoResultSetMetaData(relation);
        this.rows = rows;
        this.columnHeaders = this.meta.getColumnHeaders();
        this.stmt = stmt;
        this.cursor = 0;
        this.currentRow = new Tuple(relation);
        this._resultSetType = type;
    }

    public boolean next()
            throws SQLException
    {
        if ((UnityDriver.isExpiredTrial()) && (this.lastIndex >= UnityDriver.getMaxResults()))
        {
            System.out.println(UnityDriver.i18n.getString("ResultSet.MaxRows") + UnityDriver.getMaxResults() + UnityDriver.i18n.getString("ResultSet.TrialVersion") + " " + UnityDriver.getTrialExpiryDate());

            return false;
        }

        return super.next();
    }
}