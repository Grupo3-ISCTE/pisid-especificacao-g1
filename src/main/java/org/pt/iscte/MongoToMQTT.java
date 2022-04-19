package org.pt.iscte;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.ini4j.Ini;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

//TODO: Temos que ver como se criam as replicas do mongo

public class MongoToMQTT {

    private static final String CLOUD_ORIGIN = "Cloud Origin";
    private static final String MONGO_DESTINATION = "Mongo Destination";

    private final List<MongoCollection<Document>> collections = new ArrayList<>();
    String[] sensores = { "sensort1", "sensort2", "sensorh1", "sensorh2", "sensorl1", "sensorl2" };

    private final String mongo_address_to;
    private final int mongo_port_to;
    private final String mongo_database_name_to;
    private final String mongo_user_to;
    private final char[] mongo_password_to;
    private final String mongo_credential_database_to;
    private MongoDatabase mongo_database_to;

    private final String cloud_topic_from;
    private final String cloud_server_from;
    private final String cloud_client_name_from;
    private MqttClient cloud_client_from;
    private final int cloud_qos_from;

    public MongoToMQTT(Ini ini) {
        mongo_address_to = ini.get(MONGO_DESTINATION, "mongo_address_to");
        mongo_port_to = Integer.parseInt(ini.get(MONGO_DESTINATION, "mongo_port_to"));
        mongo_database_name_to = ini.get(MONGO_DESTINATION, "mongo_database_to");
        mongo_user_to = ini.get(MONGO_DESTINATION, "mongo_user_to");
        mongo_password_to = ini.get(MONGO_DESTINATION, "mongo_password_to").toCharArray();
        mongo_credential_database_to = ini.get(MONGO_DESTINATION, "mongo_credential_database_to");

        cloud_topic_from = ini.get(CLOUD_ORIGIN, "cloud_topic_from");
        cloud_server_from = ini.get(CLOUD_ORIGIN, "cloud_server_from");
        cloud_client_name_from = ini.get(CLOUD_ORIGIN, "cloud_client_from");
        cloud_qos_from = Integer.parseInt(ini.get(CLOUD_ORIGIN, "cloud_qos_from"));
    }

    public void connectToMongo() {
        MongoClient mongo_client_to = new MongoClient(new ServerAddress(mongo_address_to, mongo_port_to), List
                .of(MongoCredential.createCredential(mongo_user_to, mongo_credential_database_to, mongo_password_to)));
        mongo_database_to = mongo_client_to.getDatabase(mongo_database_name_to);
    }

    public void connectFromMQTT() throws MqttException {
        cloud_client_from = new MqttClient(cloud_server_from, cloud_client_name_from);
        cloud_client_from.connect(cloud_options_from());
    }

    private MqttConnectOptions cloud_options_from() {
        MqttConnectOptions cloud_options_from = new MqttConnectOptions();
        cloud_options_from.setAutomaticReconnect(true);
        cloud_options_from.setCleanSession(true);
        cloud_options_from.setConnectionTimeout(10);
        return cloud_options_from;
    }

    public void getCollections() {
        for (String s : sensores)
            collections.add(mongo_database_to.getCollection(s));
    }

    // TODO: Possibilidade de usar Broker para enviar dados perdidos caso programa
    // vá abaixo
    // TODO: Temos que importar apenas registos que no maximo têm uma certa idade
    // TODO: O Migrado so deve passar a um quando enviamos para o Java
    public void findAndSendLastRecords() {
        try {
            for (MongoCollection<Document> c : collections) {
                FindIterable<Document> records = c.find(eq("Migrado", 0));
                for (Document r : records) {
                    sendMessage(new MqttMessage(r.toString().getBytes()));
                    c.updateOne(r, new BasicDBObject().append("$inc", new BasicDBObject().append("Migrado", 1)));
                }
            }
        } catch (MqttException | NumberFormatException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(MqttMessage msg) throws MqttException {
        msg.setQos(cloud_qos_from);
        msg.setRetained(true);
        cloud_client_from.publish(cloud_topic_from, msg);
    }

    public static void main(String[] args) throws IOException, MqttException, InterruptedException {
        Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
        int sql_delay_to = Integer.parseInt(ini.get("Mysql Destination", "sql_delay_to"));
        MongoToMQTT mtmqtt = new MongoToMQTT(ini);
        mtmqtt.connectToMongo();
        mtmqtt.connectFromMQTT();
        mtmqtt.getCollections();
        while (true) {
            mtmqtt.findAndSendLastRecords();
            mtmqtt.sendMessage(new MqttMessage("fim".getBytes()));
            Thread.sleep(sql_delay_to);
        }
    }
}