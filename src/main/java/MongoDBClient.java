import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoDBClient {
    private static MongoClient mongoClient;

    public static synchronized MongoClient establishConnectionToMongoDB(String connectionStringURI) {
        ConnectionString connectionString = new ConnectionString(connectionStringURI);
        mongoClient = MongoClients.create(connectionString);
        return mongoClient;
    }

    //or
    public static synchronized MongoClient establishConnectionToMongoDBWithSettings(String connectionStringURI) {
        if (mongoClient == null) {
            ConnectionString connectionString = new ConnectionString(connectionStringURI);
            MongoClientSettings clientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
                    .build();
            mongoClient = MongoClients.create(clientSettings);
        }
        return mongoClient;
    }

    public static MongoCollection<Document> getCollectionFromDatabase(String collectionName, String databaseName) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection;
    }
}