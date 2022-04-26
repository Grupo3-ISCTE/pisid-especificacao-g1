package org.pt.iscte;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;

import java.io.File;
import java.sql.*;
import java.util.*;

public class MQTTToMySQL {

    private static final String MYSQL_ORIGIN = "Mysql Origin";
    private static final String MYSQL_DESTINATION = "Mysql Destination";
    private static final String CLOUD_DESTINATION = "Cloud Destination";

    private final String cloud_topic_to;
    private final String cloud_server_to;
    private final String cloud_client_name_to;
    private MqttClient cloud_client_to;

    private final String sql_database_connection_from;
    private final String sql_database_user_from;
    private final String sql_database_password_from;
    private Connection sql_connection_from;
    private final String sql_select_table_from;

    private final String sql_database_connection_to;
    private final String sql_database_user_to;
    private final String sql_database_password_to;
    private Connection sql_connection_to;

    private final String[] sensors;

    private final List<Document> receivedMessages = new ArrayList<>();

    private Map<String, ArrayList<Record>> records = new HashMap<>();
    private Map<String , Record> previousRecords = new HashMap<>();
    private Map<String, Double[]> sensorsLimits = new HashMap<>();

    private ArrayList<Record> processed = new ArrayList<>();
    private static final int MIN_VALUES = 3;

    public MQTTToMySQL(Ini ini) {
        cloud_topic_to = ini.get(CLOUD_DESTINATION, "cloud_topic_to");
        cloud_server_to = ini.get(CLOUD_DESTINATION, "cloud_server_to");
        cloud_client_name_to = ini.get(CLOUD_DESTINATION, "cloud_client_to");

        sql_database_connection_from = ini.get(MYSQL_ORIGIN, "sql_database_connection_from");
        sql_database_user_from = ini.get(MYSQL_ORIGIN, "sql_database_user_from");
        sql_database_password_from = ini.get(MYSQL_ORIGIN, "sql_database_password_from");
        sql_select_table_from = ini.get(MYSQL_ORIGIN, "sql_select_from_table");

        sql_database_connection_to = ini.get(MYSQL_DESTINATION, "sql_database_connection_to");
        sql_database_user_to = ini.get(MYSQL_DESTINATION, "sql_database_user_to");
        sql_database_password_to = ini.get(MYSQL_DESTINATION, "sql_database_password_to");

        sensors = ini.get("Mongo Origin", "mongo_sensores_from").toString().split(",");

        for(String sensor : sensors) {
            records.put(sensor, new ArrayList<>());
            previousRecords.put(sensor, null);
        }
    }

    public void connectToMQTT() throws MqttException {
        cloud_client_to = new MqttClient(cloud_server_to, cloud_client_name_to);
        cloud_client_to.connect(cloud_options_to());
    }

    private MqttConnectOptions cloud_options_to() {
        MqttConnectOptions cloud_options_to = new MqttConnectOptions();
        cloud_options_to.setAutomaticReconnect(true);
        cloud_options_to.setCleanSession(true);
        cloud_options_to.setConnectionTimeout(10);
        return cloud_options_to;
    }

    // TODO: Para que vai ser utilizada a tabela Zona? Deveria ser usada para análise de outliers
    public void connectFromMySql() throws SQLException {
        sql_connection_from = DriverManager.getConnection(sql_database_connection_from, sql_database_user_from,
                sql_database_password_from);
    }

    public void connectToMySql() throws SQLException {
        sql_connection_to = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to,
                sql_database_password_to);
    }

    public void receiveAndSendLastRecords() {
        try {
            if (cloud_client_to.isConnected()) {

                cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
                    if (!msg.toString().equals("fim")) {
                        receivedMessages.add(stringToDocument(msg));
                    } else {
                        removeRepeatedMessages();
                        splitRecords();
                        removeDuplicatedValuesAndDates();
                        getSensorsLimits();
                        removeAnomalousValues();
                        // System.err.println("inciar outliers");
                        // //TODO PROBLEMA E Q TEM DE SER MAIOR QUE 3 PARA NAO DAR MERDA. NAO
                        // CONSEGUIMOS DISTINGUIR QUEM SAO OS CERTOS OU ERRADOS COM POUCOS
                        // removerOutliers();
                        // System.err.println("sai outliers");
                        insertLastRecords();
                        sendRecordsToMySQL();

                        for (ArrayList<Record> listOfRecords : records.values())
                            listOfRecords.clear();

                        receivedMessages.clear();
                        // processadas.clear();
                    }
                });
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public Document stringToDocument(MqttMessage msg) {
        String m = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ", "\",\"");
        m = m.substring(1, m.length() - 1).replace("}", "\"}").replace("{", "{\"");
        return Document.parse(m);
    }

    public void removeRepeatedMessages() {
        Set<Document> set = new LinkedHashSet<>(receivedMessages);
        receivedMessages.clear();
        receivedMessages.addAll(set);
    }

    public void splitRecords() {
        for (Document d : receivedMessages) {
            Record m = new Record(d);
            records.get(m.getSensor()).add(m);
        }
    }

    // Removes both duplicated readings and duplicated timestamps
    public void removeDuplicatedValuesAndDates() {
        Map<String, ArrayList<Record>> temp = new HashMap<>();
        for (String sensor : sensors) {
            temp.put(sensor, new ArrayList<>());
            if (!records.get(sensor).isEmpty()) {
                try {
                    if(previousRecords.get(sensor) == null) {
                        temp.get(sensor).add(records.get(sensor).get(0));
                    } else {
                        if (records.get(sensor).get(0).getLeitura() != previousRecords.get(sensor).getLeitura() &&
                                !records.get(sensor).get(0).getHora().equals(previousRecords.get(sensor).getHora()))
                            temp.get(sensor).add(records.get(sensor).get(0));
                    }
                    for (int i = 1; i < records.get(sensor).size(); i++) {
                        if (records.get(sensor).get(i).getLeitura() != records.get(sensor).get(i - 1).getLeitura() &&
                                !records.get(sensor).get(i).getHora().equals(records.get(sensor).get(i - 1).getHora()))
                            temp.get(sensor).add(records.get(sensor).get(i));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        records = temp;
    }


    public void getSensorsLimits() throws SQLException {
        Statement statement = sql_connection_from.createStatement();
        ResultSet rs = statement.executeQuery(sql_select_table_from);
        while (rs.next()) {
            sensorsLimits.put(rs.getString(2) + rs.getInt(1),
                    new Double[] { rs.getDouble(3), rs.getDouble(4) });
        }
    }

    // TODO: Enviar alertas cinzentos
    public void removeAnomalousValues() {
        for (String s : sensors) {
            if (!records.get(s).isEmpty()) {
                for (int i = 0; i < records.get(s).size(); i++) {
                    if (records.get(s).get(i).getLeitura() < sensorsLimits.get(s)[0]
                            || records.get(s).get(i).getLeitura() > sensorsLimits.get(s)[1]) {
                        records.get(s).remove(i);
                        i--;
                    }
                }
            }
        }
    }

    // TODO: fazer o "trigger" deles em java

    // TODO: Fazer método (ordenar por data)
    public void removeOutliers() {
        try {
            if (!records.isEmpty()) {

                //System.out.println(records.values());

                for (String sensor : sensors) {
                    if (!records.get(sensors).isEmpty()) {
                        ArrayList<Record> values = records.get(sensor);

                        if (values.size() > MIN_VALUES) {

                            ArrayList<Record> temp = new ArrayList<>();
                            //System.out.println("valores: " + values);
                            Collections.sort(values);

                            double Q1 = calculateMedian(values.subList(0, values.size() / 2));
                            double Q3 = calculateMedian(values.subList(values.size() / 2 + 1, values.size()));
                            double Aq = Q3 - Q1;
                            //System.out.println("Q1: " + Q1);
                            //System.out.println("Q3: " + Q3);
                            //System.out.println("Aq: " + Aq);

                            for (Record medicao : values) {
                                if (medicao.getLeitura() >= Q1 - 1.5 * Aq && medicao.getLeitura() <= Q3 + 1.5 * Aq) {
                                    temp.add(medicao);
                                }
                            }

                            //System.out.println("Processadas: " + temp);
                            records.put(sensor, temp);

                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static double calculateMedian(List<Record> values) {
        if (values.size() % 2 == 0)
            return (values.get(values.size() / 2).getLeitura() + values.get(values.size()
                    / 2 - 1).getLeitura()) / 2;
        else
            return values.get(values.size() / 2).getLeitura();
    }

    public void sendRecordsToMySQL() throws SQLException {
        for (String s : sensors) {
            for (Record m : records.get(s)) {
                // for (Medicao m : processadas) {
                String query = "INSERT INTO Medicao(IDZona, Sensor, DataHora, Leitura) VALUES(" + "'" +
                        m.getZona().split("Z")[1] + "', '" + m.getSensor() + "', '" + m.getHora()
                        + "', " + m.getLeitura() + ")";
                sql_connection_to.prepareStatement(query).execute();
                System.out.println("MySQL query: " + query);
            }
        }
    }

    private void insertLastRecords() {
        // Inserts the last record of the last dump from mqtt per sensor (it never needs to be cleared)
        for(String sensor : sensors)
            if(!records.get(sensor).isEmpty())
                previousRecords.put(sensor, records.get(sensor).get(records.get(sensor).size()-1));
    }

    public static void main(String[] args) {
        try {
            MQTTToMySQL mqttsql = new MQTTToMySQL(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
            mqttsql.connectToMQTT();
            mqttsql.connectFromMySql();
            mqttsql.connectToMySql();
            mqttsql.receiveAndSendLastRecords();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}