package org.pt.iscte;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class PrimeiroJava {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private final Ini ini;

    private MongoCollection<Document> mongo_collection_from;

    private MongoCollection<Document> mongo_collection_to;

    private IMqttClient cloud_client_from;
    private String cloud_topic_from;

    private IMqttClient cloud_client_to;
    private String cloud_topic_to;

    private Connection sql_connection_to;
    private String sql_table_to;

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
        MongoDatabase mongo_database_to = mongo_client_to
                .getDatabase(ini.get("Mongo Destination", "mongo_database_to"));
        mongo_collection_to = mongo_database_to.getCollection(ini.get("Mongo Destination", "mongo_collection_to"));
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
                        leituraTransformada.append("zona", record.getString("Zona").charAt(1));
                        leituraTransformada.append("sensor", record.getString("Zona").charAt(1));
                        leituraTransformada.append("tipo", record.getString("Sensor").charAt(0));
                        leituraTransformada.append("data", record.getString("Data"));
                        leituraTransformada.append("medicao", record.getString("Medicao"));
                        leituraTransformada.append("migrado", 0);
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
     * * Conecção ao Broker para posterior envio das medicoes
     * 
     * @throws MqttException
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
     * * Conecção ao Broker para posterior leitura das medicoes
     * 
     * @throws MqttException
     */
    public void connectToMQTT() throws MqttException {
        cloud_topic_to = ini.get("Cloud Destination", "cloud_topic_to");
        cloud_client_to = new MqttClient(ini.get("Cloud Destination", "cloud_server_to"),
                ini.get("Cloud Destination", "cloud_client_to"));
        MqttConnectOptions cloud_options_to = new MqttConnectOptions();
        cloud_options_to.setAutomaticReconnect(true);
        cloud_options_to.setCleanSession(true);
        cloud_options_to.setConnectionTimeout(10);
        cloud_client_to.connect(cloud_options_to);
    }

    /**
     * * Envio das medições do Mongo para o Broker
     * ! Possibilidade de usar Broker para enviar dados perdidos caso programa vá
     * abaixo
     */
    public void mongoToMQTT() {
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        FindIterable<Document> records = mongo_collection_to.find(eq("migrado", 0));
                        for (Document medicao : records) {
                            MqttMessage msg = new MqttMessage(medicao.toString().getBytes());
                            sendMessage(msg);
                            // System.out.println("MQTT sent message: " + records.next());
                            mongo_collection_to.updateOne(medicao,
                                    new BasicDBObject().append("$inc", new BasicDBObject().append("migrado", 1)));
                        }
                        MqttMessage msg = new MqttMessage("fim".getBytes());
                        sendMessage(msg);
                        Thread.sleep(Integer.parseInt(ini.get("Mysql Destination", "sql_delay_to")));
                    } catch (MqttException | NumberFormatException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            public void sendMessage(MqttMessage msg) throws MqttPersistenceException, MqttException {
                msg.setQos(Integer.parseInt(ini.get("Cloud Origin", "cloud_qos_from")));
                msg.setRetained(true);
                cloud_client_from.publish(cloud_topic_from, msg);
            }
        }.start();
    }

    /**
     * * Conecção ao MySQL Local para envio posterior de queries
     * 
     * @throws SQLException
     */
    public void connectToMySql() throws SQLException {
        sql_table_to = ini.get("Mysql Destination", "sql_table_to");
        sql_connection_to = DriverManager.getConnection(ini.get("Mysql Destination", "sql_database_connection_to"),
                ini.get("Mysql Destination", "sql_database_user_to"),
                ini.get("Mysql Destination", "sql_database_password_to"));
    }

    /**
     * * Envio dos dados do Broker para o MySQL
     * * 1 - Recebemos os dados
     * * 2 - Removemos os duplicados
     * * 3 - Removemos os anomalos
     * * 4 - Removemos os outliers
     * * 5 - Enviamos as queries
     * * 6 - Dorme 5 segundos
     * 
     * TODO: necessário fazer a gestão de anómalos
     * TODO: necessário fazer método de remoção de OUTLIERS
     * TODO: comentar métodos da Thread
     * 
     * ? Quantos registos são de cada vez? 5 segundos? 10 registos?
     * ? Nas queries envia-se tudo de uma vez ou um registo de cada vez?
     */
    public void mQTTToMySQL() {
        new Thread() {
            private List<Medicao> medicoesGuardadas = new ArrayList<>();

            @Override
            public void run() {
                try {
                    cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
                        if (!msg.toString().equals("fim")) {
                            Medicao medicao = new Medicao(stringToDocument(msg));
                            medicoesGuardadas.add(medicao);
                        } else {
                            removerDuplicados();
                            removerAnomalos();
                            removerOutliers();
                            criarEMandarQueries();
                            medicoesGuardadas.clear();
                        }
                    });
                    sql_connection_to.close();
                } catch (MqttException | SQLException e) {
                    e.printStackTrace();
                }
            }

            public Document stringToDocument(MqttMessage msg) {
                String mensagem = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ",
                        "\",\"");
                mensagem = mensagem.substring(1, mensagem.length() - 1).replace("}", "\"}").replace("{", "{\"");
                // System.out.println("MQTT received message: " + mensagem.toString());
                return Document.parse(mensagem);
            }

            public void removerDuplicados() {
                List<Medicao> semDuplicados = new ArrayList<>();
                semDuplicados.add(medicoesGuardadas.get(0));
                for (Medicao m : medicoesGuardadas) {
                    if (semDuplicados.get(semDuplicados.size() - 1).getIDSensor() != m.getIDSensor()
                            || semDuplicados.get(semDuplicados.size() - 1).getIDZona() != m.getIDZona()
                            || semDuplicados.get(semDuplicados.size() - 1).getLeitura() != m.getLeitura()) {
                        semDuplicados.add(m);
                    }
                }
                medicoesGuardadas = semDuplicados;
            }

            public void removerAnomalos() {

            }

            // FALTA ORDENAR OS OUTLIERS PARA SABER BEM Q1 e Q3
            // ESTA MAL EXPLICADO PQ TEMOS DE AGRUPAR POR SENSOR e ZONA
            public void removerOutliers() {
                // List<Medicao> semOutliers = new ArrayList<>();
                // double q1 = medicoesGuardadas.get(medicoesGuardadas.size()/4).leitura;
                // double q3 = medicoesGuardadas.get(3 * medicoesGuardadas.size()/4).leitura;
                // double iqr = q3 - q1;
                // for(Medicao m: medicoesGuardadas){
                // double val = m.leitura;
                // if(val >= q1 - iqr - 1 && val <= q3 + iqr + 1){
                // semOutliers.add(m);
                // }
                // }
                // medicoesGuardadas = semOutliers;
            }

            public void criarEMandarQueries() throws SQLException {
                for (Medicao m : medicoesGuardadas) {
                    String query = "INSERT INTO Medicao(IDSensor, IDZona, Hora, Leitura) VALUES("
                            + m.getIDSensor() + ", " + m.getIDZona() + ", '" + m.getHora() + "', " + m.getLeitura()
                            + ")";
                    sql_connection_to.prepareStatement(query).execute();
                    // System.out.println("MySQL query: " + query);
                }
            }
        }.start();
    }

    public static void main(String[] args) throws IOException, SQLException, MqttException {
        PrimeiroJava primeiroJava = new PrimeiroJava(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
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