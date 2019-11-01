package edu.nyu.cs6913;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public class mongodbDriver {
    private MongoClient client;
    public mongodbDriver(String hostname, int port) {
        client = new MongoClient(hostname, port);
    }

    public static void main(String[] args) {
        mongodbDriver testDriver = new mongodbDriver("localhost", 32770);
        MongoDatabase database = testDriver.client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("testCollection");
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
        collection.insertOne(doc);
        System.out.println(testDriver.client.getAddress());
        testDriver.client.getDatabaseNames().forEach(System.out::println);
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("name", "MongoDB");
        FindIterable<Document> cursor = collection.find(searchQuery);
    }
}

