package mongodb.conn;

import unity.annotation.SourceTable;
import unity.engine.Attribute;

public class Subtable
{
    public String name;
    public String parentName;
    public SourceTable rootTable;
    public Subtable parentTable;
    public Attribute[] attr;
    public String listName;
    public int valueType;
}