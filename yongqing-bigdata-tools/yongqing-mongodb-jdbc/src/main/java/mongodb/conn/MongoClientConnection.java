package mongodb.conn;

import com.mongodb.MongoClient;

public class MongoClientConnection
{
    public MongoClient client;
    public int count;

    public String toString()
    {
        return "Client: " + this.client.toString() + " SQL connections: " + this.count;
    }
}