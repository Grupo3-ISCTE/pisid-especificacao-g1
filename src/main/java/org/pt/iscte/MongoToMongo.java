package org.pt.iscte;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.ini4j.Ini;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class MongoToMongo {

    private final Map<String, MongoCollection<Document>> collections = new HashMap<>();
    String[] sensores = {"sensort1", "sensort2", "sensorh1", "sensorh2", "sensorl1", "sensorl2"};

    private final String mongo_address_from;
    private final int mongo_port_from;
    private final String mongo_user_from;
    private final String mongo_credential_database_from;
    private final char[] mongo_password_from;
    private final String mongo_database_name_from;
    private final String mongo_collection_name_from;

    private final String mongo_address_to;
    private final int mongo_port_to;
    private final String mongo_database_name_to;
    private final String mongo_user_to;
    private final char[] mongo_password_to;
    private final String mongo_credential_database_to;

    private MongoCollection<Document> mongo_collection_from;
    private MongoDatabase mongo_database_to;

    public MongoToMongo(Ini ini) {
        mongo_address_from = ini.get("Mongo Origin", "mongo_address_from");
        mongo_port_from = Integer.parseInt(ini.get("Mongo Origin", "mongo_port_from"));
        mongo_user_from = ini.get("Mongo Origin", "mongo_user_from");
        mongo_credential_database_from = ini.get("Mongo Origin", "mongo_credential_database_from");
        mongo_password_from = ini.get("Mongo Origin", "mongo_password_from").toCharArray();
        mongo_database_name_from = ini.get("Mongo Origin", "mongo_database_from");
        mongo_collection_name_from = ini.get("Mongo Origin", "mongo_collection_from");

        mongo_address_to = ini.get("Mongo Destination", "mongo_address_to");
        mongo_port_to  = Integer.parseInt(ini.get("Mongo Destination", "mongo_port_to"));
        mongo_database_name_to = ini.get("Mongo Destination", "mongo_database_to");
        mongo_user_to = ini.get("Mongo Destination", "mongo_user_to");
        mongo_password_to = ini.get("Mongo Destination", "mongo_password_to").toCharArray();
        mongo_credential_database_to = ini.get("Mongo Destination","mongo_credential_database_to");
    }

    public void connectFromMongo() {
        MongoClient mongo_client_from = new MongoClient(new ServerAddress(mongo_address_from,mongo_port_from),
                List.of(MongoCredential.createCredential(mongo_user_from,mongo_credential_database_from,mongo_password_from)));
        MongoDatabase mongo_database_from = mongo_client_from.getDatabase(mongo_database_name_from);
        mongo_collection_from = mongo_database_from.getCollection(mongo_collection_name_from);
    }

    public void connectToMongo() {
        MongoClient mongo_client_to = new MongoClient(new ServerAddress(mongo_address_to,mongo_port_to),
                List.of(MongoCredential.createCredential(mongo_user_to,mongo_credential_database_to,mongo_password_to)));
        mongo_database_to = mongo_client_to.getDatabase(mongo_database_name_to);
    }

    public void createAndGetCollections() {
        for (String s : sensores) {
            try {
                mongo_database_to.createCollection(s);
            } catch (Exception e) {
                System.out.println("Collection already created");
            }
            collections.put(s,mongo_database_to.getCollection(s));
        }
    }

    public void findAndInsertLastRecords() {
        FindIterable<Document> records = mongo_collection_from.find().sort(new Document("_id",-1)).limit(6);
        try {
            for (Document record: records) {
                record.append("Migrado", 0);
                String collectionName = "sensor" + record.get("Sensor").toString().toLowerCase();
                collections.get(collectionName).insertOne(record);
            }
        }catch(MongoWriteException e){
            //System.out.println("Duplicate key error");
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
        MongoToMongo mtm = new MongoToMongo(ini);
        long mongo_delay_to = Long.parseLong(ini.get("Mongo Destination","mongo_delay_to"));
        mtm.connectFromMongo();
        mtm.connectToMongo();
        mtm.createAndGetCollections();
        while(true) {
            mtm.findAndInsertLastRecords();
            Thread.sleep(mongo_delay_to);
        }
    }
}
