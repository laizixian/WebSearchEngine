package edu.nyu.cs6913;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public class mongodbDriver {
    private MongoClient _client;
    private MongoDatabase _database;
    private MongoCollection<Document> _collection;
    mongodbDriver(String hostname, int port) {
        _client = new MongoClient(hostname, port);
    }

    void setCollection(String databaseName, String collectionName) {
        _database = _client.getDatabase(databaseName);
        _collection = _database.getCollection(collectionName);
    }

    void insert(String name, String content) {
        Document doc = new Document("name", name)
                 .append("content", content);
        _collection.insertOne(doc);
    }

    void insert(Long id, String url, String content) {
        Document doc = new Document("_id", id)
                 .append("url", url)
                 .append("content", content);
        _collection.insertOne(doc);
    }

    Document getDocLength() {
        return _collection.find().first();
    }

    public Document getWebsite(long id) {
        return _collection.find(new BasicDBObject("_id", id)).first();
    }



    public static void main(String[] args) {
        mongodbDriver testDriver = new mongodbDriver("localhost", 32770);
        MongoDatabase database = testDriver._client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("testCollection");
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
        collection.insertOne(doc);
        System.out.println(testDriver._client.getAddress());
        testDriver._client.getDatabaseNames().forEach(System.out::println);
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("name", "MongoDB");
        FindIterable<Document> cursor = collection.find(searchQuery);
    }
}

