package mongodb.query;

import java.math.BigDecimal;
import java.util.ArrayList;

public class MongoExpression
{
    protected int type;
    protected MongoExpression parent;
    protected ArrayList<MongoExpression> children;
    protected Object value;
    protected Object reference;
    protected boolean expression;

    public MongoExpression(int type, MongoExpression parent, ArrayList<MongoExpression> children, Object value, Object reference, boolean expression)
    {
        this.type = type;
        this.parent = parent;
        this.children = children;
        this.value = value;
        if ((value instanceof BigDecimal))
        {
            this.value = new Double(((BigDecimal)value).doubleValue());
        }
        this.reference = reference;
        this.expression = expression;
    }

    public int getType()
    {
        return this.type;
    }

    public void setType(int type)
    {
        this.type = type;
    }

    public MongoExpression getParent()
    {
        return this.parent;
    }

    public void setParent(MongoExpression parent)
    {
        this.parent = parent;
    }

    public ArrayList<MongoExpression> getChildren()
    {
        return this.children;
    }

    public void setChildren(ArrayList<MongoExpression> children)
    {
        this.children = children;
    }

    public Object getValue()
    {
        return this.value;
    }

    public void setValue(Object value)
    {
        this.value = value;
    }

    public Object getReference()
    {
        return this.reference;
    }

    public void setReference(Object reference)
    {
        this.reference = reference;
    }

    public boolean isExpression()
    {
        return this.expression;
    }

    public void setExpression(boolean expression)
    {
        this.expression = expression;
    }

    public String toString(boolean inWhere)
    {
        StringBuilder buf = new StringBuilder(50);
        if ((this.children != null) && (this.children.size() >= 1))
        {
            buf.append(((MongoExpression)this.children.get(0)).toString(inWhere));
        }

        if ((this.type == 100) && (inWhere))
            buf.append("this.");
        buf.append(this.value.toString());

        if ((this.children != null) && (this.children.size() >= 2))
        {
            buf.append(((MongoExpression)this.children.get(1)).toString(inWhere));
        }
        return buf.toString();
    }

    public String toString()
    {
        return toString(false);
    }

    public Object findReference()
    {
        if (this.reference != null) {
            return this.reference;
        }
        if (this.children != null)
        {
            for (MongoExpression e : this.children)
            {
                Object ref = e.findReference();
                if (ref != null)
                    return ref;
            }
        }
        return null;
    }
}