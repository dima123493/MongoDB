import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Arrays.asList;

public class MongoDBPractice {
    public static void main(String[] args) {
        MongoClient connectionToMongoDB = MongoDBClient.establishConnectionToMongoDB("<your connection string here>");

        insertOne();
        insertMany(listOfDocuments().toArray(new Document[0]));
        findPlaceWithBedroomsNumber3AndMoreWhereCountryIsBrazil();
        findFirst();
        updateSerialNumberAndAddNewField();
        updateMany();
        deleteOne();

        connectionToMongoDB.close();
    }

    public static void insertOne() {
        Document inspection = new Document("_id", new ObjectId())
                .append("name", "Test Book Name")
                .append("author", Arrays.asList("Steven", "Marry"));

        InsertOneResult result = MongoDBClient.getCollectionFromDatabase("books", "library").insertOne(inspection);
        BsonValue getById = result.getInsertedId();
        System.out.println(getById);
    }

    public static List<Document> listOfDocuments() {
        Document inspection1 = new Document("_id", new ObjectId())
                .append("name", "Test Book Name2")
                .append("author", Arrays.asList("Michel", "Nicol"));
        Document inspection2 = new Document("_id", new ObjectId())
                .append("name", "Test Book Name3")
                .append("author", Arrays.asList("Dmytro", "Milosh"));
        List<Document> documentsToInsert = asList(inspection1, inspection2);
        return documentsToInsert;
    }

    public static void insertMany(Document... documentsToInsert) {
        InsertManyResult result = MongoDBClient.getCollectionFromDatabase("books", "library").insertMany(asList(
                documentsToInsert));
        System.out.println(result.getInsertedIds());
    }

    public static void findPlaceWithBedroomsNumber3AndMoreWhereCountryIsBrazil() {
        MongoDBClient.getCollectionFromDatabase("listingsAndReviews", "sample_airbnb").find(Filters.and(Filters.gte("bedrooms", 3),
                eq("address.country", "Brazil"))).forEach(doc -> System.out.println(doc.toString()));
    }

    public static void findFirst() {
        System.out.println("=================");
        Document firstDocument = MongoDBClient.getCollectionFromDatabase("listingsAndReviews", "sample_airbnb").find(
                Filters.and(Filters.gte("bedrooms", 4))).first();
        System.out.println(firstDocument.toJson());
    }

    public static void createIndexesAndFindThem() {
        MongoDBClient.getCollectionFromDatabase("planets", "sample_guides")
                .createIndex(Indexes.compoundIndex(Indexes.ascending("name"), Indexes.descending("orderFromSun")));
        // only searches fields with text indexes
        MongoDBClient.getCollectionFromDatabase("planets", "sample_guides")
                .find(Filters.text("Saturn"));
    }

    public static void updateSerialNumberAndAddNewField() {
        Bson query = eq("name", "The Sun");
        Bson update = Updates.combine(Updates.set("year", "2023"), Updates.inc("serial_number", 1));
        UpdateResult result = MongoDBClient.getCollectionFromDatabase("magazines", "library").updateOne(query, update);
        result.getMatchedCount();
    }

    public static void updateMany() {
        Bson manyQuery = eq("maximum_nights", "1125");
        Bson manyUpdate = Updates.combine(Updates.set("maximum_nights", "1025"));
        UpdateResult manyResult = MongoDBClient.getCollectionFromDatabase("listingsAndReviews", "sample_airbnb").updateMany(manyQuery, manyUpdate);
        manyResult.getModifiedCount();
    }

    public static void deleteOne() {
        Bson query = eq("account_id", 729049);
        DeleteResult deleteOneResult = MongoDBClient.getCollectionFromDatabase("accounts", "sample_analytics").deleteOne(query);
    }

    public static void transaction() {
        final MongoClient client = MongoClients.create("connectionString");
        final ClientSession clientSession = client.startSession();

        TransactionBody<String> txnBody = () -> {
            MongoCollection<Document> bankingCollection = client.getDatabase("bank").getCollection("accounts");

            Bson fromAccount = eq("account_id", "MDB310054629");
            Bson withdrawal = Updates.inc("balance", -200);

            Bson toAccount = eq("account_id", "MDB643731035");
            Bson deposit = Updates.inc("balance", 200);

            System.out.println("This is from Account " + fromAccount.toBsonDocument().toJson() + " withdrawn " + withdrawal.toBsonDocument().toJson());
            System.out.println("This is to Account " + toAccount.toBsonDocument().toJson() + " deposited " + deposit.toBsonDocument().toJson());
            bankingCollection.updateOne(clientSession, fromAccount, withdrawal);
            bankingCollection.updateOne(clientSession, toAccount, deposit);

            return "Transferred funds from John Doe to Mary Doe";
        };

        try {
            clientSession.withTransaction(txnBody);
        } catch (RuntimeException e) {
            System.out.println(e);
        } finally {
            clientSession.close();
        }

    }

    public static void aggregation() {
        Bson matchStage = Aggregates.match(Filters.eq("some", "some"));
        Bson groupStage = Aggregates.group("$keyToGroupBy", sum("totalBalance", "$balance"),
                avg("avgBalance", "$balance"));
        MongoDBClient.getCollectionFromDatabase("myAtlasClusterEDU", "library")
                .aggregate(asList(matchStage, groupStage)).forEach(doc -> System.out.println(doc.toJson()));
    }

    public static void machSortProject() {
        Bson matchStage = Aggregates.match(Filters.and(Filters.gt("balance", 100),
                Filters.eq("field", "vale")));
        Bson sortStage = Aggregates.sort(Sorts.orderBy(Sorts.descending("fieldsName")));
        Bson projectStage = Aggregates.project(Projections.fields(Projections.include("fields", "to", "include/display"),
                Projections.computed("fieldName", new Document("$field", asList("$field", 100))), Projections.excludeId()));
        MongoDBClient.getCollectionFromDatabase("myAtlasClusterEDU", "library")
                .aggregate(asList(matchStage, sortStage, projectStage)).forEach(doc -> System.out.println(doc.toJson()));
    }

    public static void replace() {
        Bson query = eq("title", "Music of the Heart");
        Document replaceDocument = new Document().
                append("title", "50 Violins").
                append("fullplot", " A dramatization of the true story of Roberta Guaspari who co-founded the Opus 118 Harlem School of Music");
        ReplaceOptions opts = new ReplaceOptions().upsert(true);
        UpdateResult result = MongoDBClient.getCollectionFromDatabase("myAtlasClusterEDU", "library").replaceOne(query, replaceDocument, opts);
    }

    public static void bulkOperations() {
        BulkWriteResult result = MongoDBClient.getCollectionFromDatabase("myAtlasClusterEDU", "library").bulkWrite(
                Arrays.asList(
                        new InsertOneModel<>(new Document("name", "A Sample Movie")),
                        new InsertOneModel<>(new Document("name", "Another Sample Movie")),
                        new InsertOneModel<>(new Document("name", "Yet Another Sample Movie")),
                        new UpdateOneModel<>(new Document("name", "A Sample Movie"),
                                new Document("$set", new Document("name", "An Old Sample Movie")),
                                new UpdateOptions().upsert(true)),
                        new DeleteOneModel<>(new Document("name", "Yet Another Sample Movie")),
                        new ReplaceOneModel<>(new Document("name", "Yet Another Sample Movie"),
                                new Document("name", "The Other Sample Movie").append("runtime", "42"))
                ));
    }

}
