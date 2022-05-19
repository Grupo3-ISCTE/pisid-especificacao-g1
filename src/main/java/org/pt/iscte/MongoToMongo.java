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

    private static final String MONGO_ORIGIN = "Mongo Origin";
    private static final String MONGO_DESTINATION = "Mongo Destination";

    private final Map<String, MongoCollection<Document>> collections = new HashMap<>();
    private final String[] sensors;

    private final String mongo_address_from;
    private final int mongo_port_from;
    private final String mongo_user_from;
    private final String mongo_credential_database_from;
    private final char[] mongo_password_from;
    private final String mongo_database_name_from;
    private final String mongo_collection_name_from;

    private final String mongo_address1_to;
    private final int mongo_port1_to;
    private final String mongo_address2_to;
    private final int mongo_port2_to;
    private final String mongo_address3_to;
    private final int mongo_port3_to;

    private final String mongo_database_name_to;
    private final String mongo_user_to;
    private final char[] mongo_password_to;
    private final String mongo_credential_database_to;

    private MongoCollection<Document> mongo_collection_from;
    private MongoDatabase mongo_database_to;

    public MongoToMongo(Ini ini) {
        mongo_address_from = ini.get(MONGO_ORIGIN, "mongo_address_from");
        mongo_port_from = Integer.parseInt(ini.get(MONGO_ORIGIN, "mongo_port_from"));
        mongo_user_from = ini.get(MONGO_ORIGIN, "mongo_user_from");
        mongo_credential_database_from = ini.get(MONGO_ORIGIN, "mongo_credential_database_from");
        mongo_password_from = ini.get(MONGO_ORIGIN, "mongo_password_from").toCharArray();
        mongo_database_name_from = ini.get(MONGO_ORIGIN, "mongo_database_from");
        mongo_collection_name_from = ini.get(MONGO_ORIGIN, "mongo_collection_from");

        mongo_address1_to = ini.get(MONGO_DESTINATION, "mongo_address1_to");
        mongo_port1_to = Integer.parseInt(ini.get(MONGO_DESTINATION, "mongo_port1_to"));
        mongo_address2_to = ini.get(MONGO_DESTINATION, "mongo_address2_to");
        mongo_port2_to = Integer.parseInt(ini.get(MONGO_DESTINATION, "mongo_port2_to"));
        mongo_address3_to = ini.get(MONGO_DESTINATION, "mongo_address3_to");
        mongo_port3_to = Integer.parseInt(ini.get(MONGO_DESTINATION, "mongo_port3_to"));

        mongo_database_name_to = ini.get(MONGO_DESTINATION, "mongo_database_to");
        mongo_user_to = ini.get(MONGO_DESTINATION, "mongo_user_to");
        mongo_password_to = ini.get(MONGO_DESTINATION, "mongo_password_to").toCharArray();
        mongo_credential_database_to = ini.get(MONGO_DESTINATION, "mongo_credential_database_to");

        sensors = ini.get(MONGO_ORIGIN, "mongo_sensores_from").toString().split(",");
    }

    public void connectFromMongo() {
        MongoDatabase mongo_database_from;
        MongoClient mongo_client_from = new MongoClient(new ServerAddress(mongo_address_from, mongo_port_from),
                List.of(MongoCredential.createCredential(mongo_user_from, mongo_credential_database_from,
                        mongo_password_from)));
        mongo_database_from = mongo_client_from.getDatabase(mongo_database_name_from);
        mongo_collection_from = mongo_database_from.getCollection(mongo_collection_name_from);

    }

    public void connectToMongo() {
        List<ServerAddress> seeds = Arrays.asList(new ServerAddress(mongo_address1_to,mongo_port1_to),
                new ServerAddress(mongo_address2_to,mongo_port2_to),
                new ServerAddress(mongo_address3_to,mongo_port3_to));
        List<MongoCredential> credentialList = Arrays.asList(
                MongoCredential.createCredential(mongo_user_to, mongo_credential_database_to, mongo_password_to));
        MongoClient mongo_client_to = new MongoClient(seeds, credentialList);
        mongo_database_to = mongo_client_to.getDatabase(mongo_database_name_to);
    }

    public void createAndGetCollections() {
        for (String s : sensors) {
            String collection_name = "sensor" + s.toLowerCase();
            if(!isCollectionCreated(collection_name)) {
                try {
                    mongo_database_to.createCollection(collection_name);
                } catch (Exception e) {
                    // System.out.println("Collection already created");
                }
            }
            collections.put("sensor" + s.toLowerCase(), mongo_database_to.getCollection(collection_name));
        }
    }

        private boolean isCollectionCreated(String collection_name) {
            for(String a : mongo_database_to.listCollectionNames())
                if(collection_name.equals(a))
                    return true;
            return false;
        }

    public void findAndInsertLastRecords() {
        FindIterable<Document> records = mongo_collection_from.find().sort(new Document("_id", -1)).limit(sensors.length);
        try {
            for (Document r : records) {
                r.append("Migrado", 0);
                String collectionName = "sensor" + r.get("Sensor").toString().toLowerCase();
                System.out.println(collectionName);
                collections.get(collectionName).insertOne(r);
                System.out.println(r);
            }
        } catch (MongoWriteException e) {
            // System.out.println("Duplicate key error");
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
        MongoToMongo mtm = new MongoToMongo(ini);
        long mongo_delay_to = Long.parseLong(ini.get(MONGO_DESTINATION, "mongo_delay_to"));
        mtm.connectFromMongo();
        mtm.connectToMongo();
        mtm.createAndGetCollections();
        while (true) {
            mtm.findAndInsertLastRecords();
            Thread.sleep(mongo_delay_to);
        }
    }
}
