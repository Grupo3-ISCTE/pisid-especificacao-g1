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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

public class PrimeiroJava {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private final Ini ini;

    private MongoCollection<Document> mongo_collection_from;

    private MongoCollection<Document> mongo_collection_to;

    private MqttClient cloud_client_from;
    private String cloud_topic_from;

    private MqttClient cloud_client_to;
    private String cloud_topic_to;

    private Connection sql_connection_from;
    private Connection sql_connection_to;

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
     * * Conecção ao Broker para posterior leitura das medicoes
     *
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
     * * Conecção ao MySQL da Cloud para posterior análise da tabela Sensor
     * ? Para que vai ser utilizada a tabela Zona?
     * 
     * @throws SQLException
     */
    public void connectFromMySql() throws SQLException {
        String connection_link = ini.get("Mysql Origin", "sql_database_connection_from");
        String user = ini.get("Mysql Origin", "sql_database_user_from");
        String password = ini.get("Mysql Origin", "sql_database_password_from");
        sql_connection_from = DriverManager.getConnection(connection_link, user, password);
    }

    /**
     * * Conecção ao MySQL Local para envio posterior de queries
     * 
     * @throws SQLException
     */
    public void connectToMySql() throws SQLException {
        sql_connection_to = DriverManager.getConnection(ini.get("Mysql Destination", "sql_database_connection_to"),
                ini.get("Mysql Destination", "sql_database_user_to"),
                ini.get("Mysql Destination", "sql_database_password_to"));
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
                        leituraTransformada.append("tipo", record.getString("Sensor").charAt(0));
                        leituraTransformada.append("sensor", record.getString("Sensor").charAt(1));
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
     * * Envio das medições do Mongo para o Broker
     * ! Possibilidade de usar Broker para enviar dados perdidos caso programa vá
     * abaixo
     */
    public void mongoToMQTT() {
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    if (cloud_client_from.isConnected() && cloud_client_to.isConnected()) {
                        try {
                            FindIterable<Document> records = mongo_collection_to.find(eq("migrado", 0));
                            for (Document medicao : records) {
                                sendMessage(new MqttMessage(medicao.toString().getBytes()));
                                mongo_collection_to.updateOne(medicao,
                                        new BasicDBObject().append("$inc", new BasicDBObject().append("migrado", 1)));
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
        }.start();
    }

    /**
     * * Envio dos dados do Broker para o MySQL
     *
     * TODO: comentar métodos da Thread
     * ! Temos ainda que guardar o ultimo registo de cada um dos sensores para
     * ! tentar remover ainda mais duplicados
     */
    public void mQTTToMySQL() {
        new Thread() {
            String[] listaSensores = new String[] { "T1", "T2", "H1", "H2", "L1", "L2" };
            List<Document> mensagensRecebidas = new ArrayList<>();
            MMap medicoes = new MMap();
            Map<String, Double[]> limitesSensores = new HashMap<>();

            @Override
            public void run() {
                try {
                    cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
                        if (!msg.toString().equals("fim")) {
                            mensagensRecebidas.add(stringToDocument(msg));
                        } else {
                            removerMensagensRepetidas();
                            dividirMedicoes();
                            removerValoresDuplicados();
                            analisarTabelaSensor();
                            removerValoresAnomalos();
                            removerOutliers();
                            criarEMandarQueries();

                            medicoes.clear();
                            mensagensRecebidas.clear();
                        }
                    });
                } catch (MqttException ignored) {
                }
            }

            public Document stringToDocument(MqttMessage msg) {
                String m = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ",
                        "\",\"");
                return Document
                        .parse(m.substring(1, m.length() - 1).replace("}", "\"}").replace("{", "{\""));
            }

            public void removerMensagensRepetidas() {
                List<Document> temp = new ArrayList<>();
                for (Document d : mensagensRecebidas)
                    if (!temp.contains(d))
                        temp.add(d);
                mensagensRecebidas = temp;
            }

            public void dividirMedicoes() {
                for (Document d : mensagensRecebidas) {
                    Medicao m = new Medicao(d);
                    medicoes.get(m.getTipoSensor() + m.getIDSensor()).add(m);
                }
            }

            public void removerValoresDuplicados() {
                MMap temp = new MMap();
                for (String sensor : listaSensores) {
                    if (!medicoes.get(sensor).isEmpty()) {
                        temp.get(sensor).add(medicoes.get(sensor).get(0));
                        try {
                            for (int i = 1; i < medicoes.get(sensor).size(); i++)
                                if (medicoes.get(sensor).get(i).getLeitura() != medicoes.get(sensor).get(i - 1)
                                        .getLeitura()) {
                                    temp.get(sensor).add(medicoes.get(sensor).get(i));
                                }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                medicoes = temp;
            }

            public void analisarTabelaSensor() throws SQLException {
                Statement statement = sql_connection_from.createStatement();
                ResultSet rs = statement.executeQuery(ini.get("Mysql Origin", "sql_select_from_table"));
                while (rs.next()) {
                    limitesSensores.put(rs.getString(2) + rs.getInt(1),
                            new Double[] { rs.getDouble(3), rs.getDouble(4) });
                }
            }

            public void removerValoresAnomalos() {
                for (String s : listaSensores) {
                    if (!medicoes.get(s).isEmpty()) {
                        for (int i = 0; i < medicoes.get(s).size(); i++) {
                            if (medicoes.get(s).get(i).getLeitura() < limitesSensores.get(s)[0]
                                    || medicoes.get(s).get(i).getLeitura() > limitesSensores.get(s)[1]) {
                                medicoes.get(s).remove(i);
                                i--;
                            }
                        }
                    }
                }
            }

            // !Testes metodo sort e remoçao de outliers
            public void removerOutliers() {
                try {
                    if (!medicoes.isEmpty()) {
                        medicoes.sort();
                        for (String s : listaSensores) {
                            if (!medicoes.get(s).isEmpty()) {
                                double q1 = medicoes.get(s).get(medicoes.get(s).size() / 4).getLeitura();
                                double q3 = medicoes.get(s).get(3 * medicoes.get(s).size() / 4).getLeitura();
                                double iqr = q3 - q1;
                                for (int i = 0; i < medicoes.get(s).size(); i++) {
                                    double val = medicoes.get(s).get(i).getLeitura();
                                    if (val < q1 - iqr - 1 || val > q3 + iqr + 1) {
                                        System.out.println("OUTLIER: " + medicoes.get(s).get(i));
                                        medicoes.get(s).remove(i);
                                        i--;
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            public void criarEMandarQueries() throws SQLException {
                for (String s : listaSensores) {
                    for (Medicao m : medicoes.get(s)) {
                        String query = "INSERT INTO Medicao(IDSensor, IDZona, Hora, Leitura) VALUES("
                                + m.getIDSensor() + ", " + m.getIDZona() + ", '" + m.getHora() + "', " +
                                m.getLeitura()
                                + ")";
                        sql_connection_to.prepareStatement(query).execute();
                        // System.out.println("MySQL query: " + query);
                    }
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
        primeiroJava.connectFromMySql();
        primeiroJava.mQTTToMySQL();
    }
}