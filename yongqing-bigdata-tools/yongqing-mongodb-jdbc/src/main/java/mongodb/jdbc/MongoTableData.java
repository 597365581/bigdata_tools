package mongodb.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import unity.engine.IServerConnection;
import unity.engine.TableData;
import unity.engine.Tuple;

public class MongoTableData extends TableData
{
    public MongoTableData(IServerConnection connection, boolean buffering, int statementId)
    {
        super(connection, buffering, statementId);
    }

    public void setTuples(ArrayList<Object[]> data)
            throws SQLException
    {
        this.rows = new ArrayList();

        for (int idx = 0; idx < data.size(); idx++)
        {
            this.rows.add(Tuple.convertToBytes((Object[])data.get(idx), this.relation));
        }

        this.dataComplete = true;
    }
}