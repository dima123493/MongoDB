import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDBClient {
    private static MongoClient mongoClient;

    public static synchronized MongoClient getMongoDBClient() {
        if (mongoClient == null) {
            ConnectionString connectionString = new ConnectionString("mongodb+srv://myAtlasDBUser:myAtlasDBUser@myatlasclusteredu.w1dxyhm.mongodb.net/?retryWrites=true&w=majority");
            MongoClientSettings clientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())
                    .build();
            mongoClient = MongoClients.create(clientSettings);
        }
        return mongoClient;
    }
}