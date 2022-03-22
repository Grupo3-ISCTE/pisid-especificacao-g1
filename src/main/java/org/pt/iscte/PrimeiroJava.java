package org.pt.iscte;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Sorts.descending;

public class PrimeiroJava {

    private final Ini ini;

    private MongoCollection<Document> mongo_collection_from;

    private MongoCollection<Document> mongo_collection_to;

    private IMqttClient cloud_client_from;
    private String cloud_topic_from;

    private IMqttClient cloud_client_to;
    private String cloud_topic_to;

    private Connection sql_connection_to;
    private String sql_table_to;

    public PrimeiroJava(Ini ini){
        this.ini = ini;
    }

    public void connectFromMongo(){
        MongoClient mongo_client_from = new MongoClient(new ServerAddress(ini.get("Mongo Origin", "mongo_address_from"), Integer.parseInt(ini.get("Mongo Origin", "mongo_port_from"))), List.of(MongoCredential.createCredential(ini.get("Mongo Origin", "mongo_user_from"), ini.get("Mongo Origin", "mongo_credential_database_from"), ini.get("Mongo Origin", "mongo_password_from").toCharArray())));
        MongoDatabase mongo_database_from = mongo_client_from.getDatabase(ini.get("Mongo Origin", "mongo_database_from"));
        mongo_collection_from = mongo_database_from.getCollection(ini.get("Mongo Origin","mongo_collection_from"));
    }

    public void connectToMongo(){
        MongoClient mongo_client_to = new MongoClient(ini.get("Mongo Destination", "mongo_address_to"), Integer.parseInt(ini.get("Mongo Destination", "mongo_port_to")));
        MongoDatabase mongo_database_to = mongo_client_to.getDatabase(ini.get("Mongo Destination", "mongo_database_to"));
        mongo_collection_to = mongo_database_to.getCollection(ini.get("Mongo Destination","mongo_collection_to"));
    }

    public void mongoToMongo() {
        new Thread(() -> {
            Document last = null;
            while(true){
                Document leitura = mongo_collection_from.find().sort(descending("Data")).first();
                try{
                    if(last == null || !leitura.get("_id").equals(last.get("_id"))) {
                        // if(last!=null) System.out.println("Last: " + last.get("_id") + "\n" + "Novo: " + leitura.get("_id"));
                        Document leituraTransformada = new Document();
                        leituraTransformada.append("_id", leitura.getObjectId("_id"));
                        leituraTransformada.append("zona", leitura.getString("Zona").charAt(1));
                        leituraTransformada.append("sensor", leitura.getString("Zona").charAt(1));
                        leituraTransformada.append("tipo", leitura.getString("Sensor").charAt(0));
                        leituraTransformada.append("data", leitura.getString("Data"));
                        leituraTransformada.append("medicao",leitura.getString("Medicao"));
                        mongo_collection_to.insertOne(leituraTransformada);
                        // System.out.println("MongoDB to MongoDB" + leituraTransformada);
                        last = leitura;
                    }
                }catch (MongoWriteException e){
                    System.out.println("A medicao anterior ja esta na base de dados local.");
                }
            }
        }).start();
    }

    
    public void connectFromMQTT() throws MqttException{
        cloud_topic_from = ini.get("Cloud Origin", "cloud_topic_from");
        cloud_client_from = new MqttClient(ini.get("Cloud Origin", "cloud_server_from"), ini.get("Cloud Origin", "cloud_client_from"));
        MqttConnectOptions cloud_options_from = new MqttConnectOptions();
        cloud_options_from.setAutomaticReconnect(true);
        cloud_options_from.setCleanSession(true);
        cloud_options_from.setConnectionTimeout(10);
        cloud_client_from.connect(cloud_options_from);        
    }

    public void connectToMQTT() throws MqttException{
        cloud_topic_to = ini.get("Cloud Destination", "cloud_topic_to");
        cloud_client_to = new MqttClient(ini.get("Cloud Destination", "cloud_server_to"), ini.get("Cloud Destination", "cloud_client_to"));
        MqttConnectOptions cloud_options_to = new MqttConnectOptions();
        cloud_options_to.setAutomaticReconnect(true);
        cloud_options_to.setCleanSession(true);
        cloud_options_to.setConnectionTimeout(10);
        cloud_client_to.connect(cloud_options_to);
    }

    public void mongoToMQTT(){
        new Thread(() -> {
            // while(true){
                Document leitura = mongo_collection_to.find().sort(descending("Data")).first();
                // System.out.println("MQTT sent message: " + leitura);
                MqttMessage msg = new MqttMessage(leitura.toString().getBytes());
                msg.setQos(Integer.parseInt(ini.get("Cloud Origin","cloud_qos_from")));
                msg.setRetained(true);
                try {
                    cloud_client_from.publish(cloud_topic_from, msg);
                } catch (MqttException e) {e.printStackTrace();}
            // }
        }).start();
    }


    public void connectToMySql() throws SQLException{
        sql_table_to = ini.get("Mysql Destination","sql_table_to");
        sql_connection_to = DriverManager.getConnection(ini.get("Mysql Destination","sql_database_connection_to"),ini.get("Mysql Destination","sql_database_user_to"),ini.get("Mysql Destination","sql_database_password_to"));
    }

    public void mQTTToMySQL() {
        //RECEBER DADOS DO MQTT (DONE)
        // OUTLIERS - ULTIMAS 10 MEDICOES (X)
        //TRATAR DADOS PARA NAO IREM REPETIDOS (DONE)
        //ENVIAR DADOS PARA O MYSQL LOCAL (DONE)
        //DORMIR 5 SEGUNDOS (DONE)

        new Thread(){
            private List<Medicao> medicoesGuardadas = new ArrayList<>();

            @Override
            public void run(){
                try {
                    cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
                        if(medicoesGuardadas.size() != Integer.parseInt(ini.get("Mysql Destination","sql_medicoes_a_enviar")))
                            medicoesGuardadas.add(new Medicao(stringToDocument(msg)));
                        else{
                            // removeOutliers();
                            removeDuplicates();
                            //ANTES DE INSERIR TEMOS QUE VERIFICAR O MYSQL PARA NAO INSERIR VALORES REPETIDOS
                            // PARA ISSO TEMOS DE COLOCAR NO FIM DE TUDO EM ORDEM DE DATAS E APENAS COMPARAR O PRIMEIRO COM O MYSQL
                            criarEMandarQueries();
                            medicoesGuardadas.clear();
                            sleep(Integer.parseInt(ini.get("Mysql Destination","delay")));
                        }
                    });
                } catch (MqttException e) {
                    try {
                        sql_connection_to.close();
                    } catch (SQLException e1) {e1.printStackTrace();}
                    e.printStackTrace();
                }
            }

            public Document stringToDocument(MqttMessage msg){
                String mensagem = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ", "\",\"");
                System.out.println("MQTT received message: " + mensagem);
                return Document.parse(mensagem.substring(1,mensagem.length() - 1).replace("}", "\"}").replace("{", "{\""));
            }

            // public void removeOutliers(){
            //     // FALTA ORDENAR OS OUTLIERS PARA SABER BEM Q1 e Q3
            //     // ESTA MAL EXPLICADO PQ TEMOS DE AGRUPAR POR SENSOR e ZONA
            //     List<Medicao> semOutliers = new ArrayList<>();
            //     double q1 = medicoesGuardadas.get(medicoesGuardadas.size()/4).leitura;
            //     double q3 = medicoesGuardadas.get(3 * medicoesGuardadas.size()/4).leitura;
            //     double iqr = q3 - q1;
            //     for(Medicao m: medicoesGuardadas){
            //         double val = m.leitura;
            //         if(val >= q1 - iqr - 1 && val <= q3 + iqr + 1){
            //             semOutliers.add(m);
            //         }
            //     }
            //     medicoesGuardadas = semOutliers;
            // }
            
            public void removeDuplicates(){
                List<Medicao> semDuplicados = new ArrayList<>();
                semDuplicados.add(medicoesGuardadas.get(0));
                for(Medicao m: medicoesGuardadas){
                    for(int i = 0; i < semDuplicados.size(); i++){
                        if(semDuplicados.get(i).iDSensor != m.iDSensor || semDuplicados.get(i).iDZona != m.iDZona || semDuplicados.get(i).leitura != m.leitura) {
                            semDuplicados.add(m);
                        }
                    }
                }
                medicoesGuardadas = semDuplicados;
            }

            public void criarEMandarQueries() throws SQLException{
                for(Medicao m: medicoesGuardadas){
                    String query = "INSERT INTO " + sql_table_to + "(IDSensor, IDZona, Hora, Leitura) VALUES("+m.iDSensor+", "+m.iDZona+", '"+m.hora+"', "+m.leitura+")";
                    sql_connection_to.prepareStatement(query).execute();
                    System.out.println("MySQL query: " + query);
                }
            }
        }.start();
    }

    public static void main(String[] args) throws IOException, SQLException, MqttException{
        PrimeiroJava primeiroJava = new PrimeiroJava(new Ini(new File("src/main/java/org/iscte/pt/config.ini")));

        primeiroJava.connectFromMongo();
        primeiroJava.connectToMongo();
        primeiroJava.mongoToMongo();

        primeiroJava.connectFromMQTT();
        primeiroJava.connectToMQTT();
        primeiroJava.mongoToMQTT();

        primeiroJava.connectToMySql();
        primeiroJava.mQTTToMySQL();
    }
}