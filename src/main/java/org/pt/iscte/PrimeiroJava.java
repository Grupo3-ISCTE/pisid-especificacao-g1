package org.pt.iscte;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.ini4j.Ini;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

//TODO: Temos que ver como se criam as replicas do mongo

public class PrimeiroJava {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private final Ini ini;

    private MongoCollection<Document> mongo_collection_from;

    private MongoDatabase mongo_database_to;

    private MqttClient cloud_client_from;
    private String cloud_topic_from;

    public PrimeiroJava(Ini ini) {
        this.ini = ini;
    }

    /**
     * * Conecção à base de dados MongoDB da Cloud
     */
    public void connectFromMongo() {
        MongoClient mongo_client_from = new MongoClient(
                new ServerAddress(ini.get("Mongo Origin", "mongo_address_from"),
                        Integer.parseInt(ini.get("Mongo Origin", "mongo_port_from"))),
                List.of(MongoCredential.createCredential(ini.get("Mongo Origin", "mongo_user_from"),
                        ini.get("Mongo Origin", "mongo_credential_database_from"),
                        ini.get("Mongo Origin", "mongo_password_from").toCharArray())));
        MongoDatabase mongo_database_from = mongo_client_from
                .getDatabase(ini.get("Mongo Origin", "mongo_database_from"));
        mongo_collection_from = mongo_database_from.getCollection(ini.get("Mongo Origin", "mongo_collection_from"));
    }

    /**
     * * Conecção à base de dados MongoDB Local
     * ? necessário colocar user e password
     */
    public void connectToMongo() {
        MongoClient mongo_client_to = new MongoClient(ini.get("Mongo Destination", "mongo_address_to"),
                Integer.parseInt(ini.get("Mongo Destination", "mongo_port_to")));
        mongo_database_to = mongo_client_to
                .getDatabase(ini.get("Mongo Destination", "mongo_database_to"));
    }

    /**
     * * Conecção ao Broker para posterior envio das medicoes
     *
     */
    public void connectFromMQTT() throws MqttException {
        cloud_topic_from = ini.get("Cloud Origin", "cloud_topic_from");
        cloud_client_from = new MqttClient(ini.get("Cloud Origin", "cloud_server_from"),
                ini.get("Cloud Origin", "cloud_client_from"));
        MqttConnectOptions cloud_options_from = new MqttConnectOptions();
        cloud_options_from.setAutomaticReconnect(true);
        cloud_options_from.setCleanSession(true);
        cloud_options_from.setConnectionTimeout(10);
        cloud_client_from.connect(cloud_options_from);
    }

    /**
     * * Transferência de dados do MongoDB da Cloud para o MongoDB Local
     */
    public void mongoToMongo() {
        new Thread(() -> {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(new Timestamp(System.currentTimeMillis()).getTime());
            while (true) {
                FindIterable<Document> records = mongo_collection_from
                        .find(new Document("Data", sdf.format(new Timestamp(cal.getTime().getTime())))).limit(6);
                try {
                    for (Document record : records) {
                        Document leituraTransformada = new Document();
                        leituraTransformada.append("_id", record.getObjectId("_id"));
                        leituraTransformada.append("Zona", record.getString("Zona"));
                        leituraTransformada.append("Sensor", record.getString("Sensor"));
                        leituraTransformada.append("Data", record.getString("Data"));
                        leituraTransformada.append("Medicao", record.getString("Medicao"));
                        leituraTransformada.append("Migrado", 0);
                        MongoCollection<Document> mongo_collection_to = mongo_database_to
                                .getCollection("sensor" + record.getString("Sensor").toLowerCase());
                        mongo_collection_to.insertOne(leituraTransformada);
                        // System.out.println("MongoDB to MongoDB" + leituraTransformada);
                    }
                } catch (MongoWriteException e) {
                    // System.out.println("A medicao anterior ja esta na base de dados local.");
                }
                cal.add(Calendar.SECOND, +1);
            }
        }).start();
    }

    /**
     * * Envio das medições do Mongo para o Broker
     * ! Possibilidade de usar Broker para enviar dados perdidos caso programa vá
     * abaixo
     */
    public void mongoToMQTT() {
        while (true) {
            if (cloud_client_from.isConnected()) {
                try {
                    MongoIterable<String> collectionList = mongo_database_to.listCollectionNames();
                    for (String collection : collectionList) {
                        MongoCollection<Document> mongo_collection_to = mongo_database_to
                                .getCollection(collection);
                        FindIterable<Document> records = mongo_collection_to.find(eq("Migrado", 0));
                        for (Document medicao : records) {
                            MqttMessage m = new MqttMessage(medicao.toString().getBytes());
                            sendMessage(m);
                            System.out.println(m);
                            mongo_collection_to.updateOne(medicao,
                                    new BasicDBObject().append("$inc",
                                            new BasicDBObject().append("Migrado", 1)));
                        }
                    }
                    sendMessage(new MqttMessage("fim".getBytes()));
                    Thread.sleep(Integer.parseInt(ini.get("Mysql Destination", "sql_delay_to")));
                } catch (MqttException | NumberFormatException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void sendMessage(MqttMessage msg) throws MqttException {
        msg.setQos(Integer.parseInt(ini.get("Cloud Origin", "cloud_qos_from")));
        msg.setRetained(true);
        cloud_client_from.publish(cloud_topic_from, msg);
    }

    public static void main(String[] args) throws IOException, MqttException {
        PrimeiroJava primeiroJava = new PrimeiroJava(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
//        primeiroJava.connectFromMongo();
        primeiroJava.connectToMongo();
//        primeiroJava.mongoToMongo();
        primeiroJava.connectFromMQTT();
        primeiroJava.mongoToMQTT();
    }
}