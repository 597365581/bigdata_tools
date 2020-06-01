package mongodb.query;

public class MongoBuilderFatalException extends Exception
{
    private static final long serialVersionUID = -3471272040234423421L;

    public MongoBuilderFatalException(Throwable cause)
    {
        super(cause);
    }

    public MongoBuilderFatalException(String string)
    {
        this(new Throwable(string));
    }
}