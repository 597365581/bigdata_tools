package mongodb.query;

public class MongoBuilderUpstreamException extends Exception
{
    private static final long serialVersionUID = -6123204868374038276L;

    public MongoBuilderUpstreamException(Throwable cause)
    {
        super(cause);
    }

    public MongoBuilderUpstreamException(String string)
    {
        this(new Throwable(string));
    }
}