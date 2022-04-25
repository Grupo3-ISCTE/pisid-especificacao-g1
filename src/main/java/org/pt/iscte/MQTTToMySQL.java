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

    String[] sensores;

    List<Document> mensagensRecebidas = new ArrayList<>();
    MMap medicoes = new MMap();
    Map<String, Double[]> limitesSensores = new HashMap<>();
    ArrayList<Medicao> processadas = new ArrayList<>();

    private final int MIN_VALUES = 3;

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

        sensores = ini.get("Mongo Origin", "mongo_sensores_from").toString().split(",");
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

    // TODO: Para que vai ser utilizada a tabela Zona?
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
                        mensagensRecebidas.add(stringToDocument(msg));
                    } else {
                        removeRepeatedMessages();
                        splitRecords();
                        removeDuplicatedValues();
                        removerValoresNaMesmaHora();
                        getSensorsLimits();
                        removeAnomalousValues();
                        // System.err.println("inciar outliers");
                        // //TODO PROBLEMA E Q TEM DE SER MAIOR QUE 3 PARA NAO DAR MERDA. NAO
                        // CONSEGUIMOS DISTINGUIR QUEM SAO OS CERTOS OU ERRADOS COM POUCOS
                        // removerOutliers();
                        // System.err.println("sai outliers");
                        sendRecordsToMySQL();

                        medicoes.clear();
                        mensagensRecebidas.clear();
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
        Set<Document> set = new LinkedHashSet<>(mensagensRecebidas);
        mensagensRecebidas.clear();
        mensagensRecebidas.addAll(set);
    }

    public void splitRecords() {
        for (Document d : mensagensRecebidas) {
            Medicao m = new Medicao(d);
            medicoes.get(m.getSensor()).add(m);
        }
    }

    public void removeDuplicatedValues() {
        MMap temp = new MMap();
        for (String sensor : sensores) {
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

    // TODO: fazer método caso o professor decida duplicar os bat
    public void removerValoresNaMesmaHora() {
    }

    public void getSensorsLimits() throws SQLException {
        Statement statement = sql_connection_from.createStatement();
        ResultSet rs = statement.executeQuery(sql_select_table_from);
        while (rs.next()) {
            limitesSensores.put(rs.getString(2) + rs.getInt(1),
                    new Double[] { rs.getDouble(3), rs.getDouble(4) });
        }
    }

    // TODO: Enviar alertas cinzentos
    public void removeAnomalousValues() {
        for (String s : sensores) {
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

    // TODO: Fazer método (ordenar por data)
    // public void removerOutliers() {
    // try {
    // if (!medicoes.isEmpty()) {
    // System.out.println(medicoes.getValuesLists());
    // for (ArrayList<Medicao> valores : medicoes.getValuesLists()) {
    // if (valores.size() > MIN_VALUES) {

    // ArrayList<Medicao> proc = new ArrayList<Medicao>() {
    // };
    // System.out.println("valores: " + valores);
    // Collections.sort(valores);

    // double Q1 = calculteMedian(valores.subList(0, valores.size() / 2));
    // double Q3 = calculteMedian(valores.subList(valores.size() / 2 + 1,
    // valores.size()));
    // double Aq = Q3 - Q1;
    // System.out.println("Q1: " + Q1);
    // System.out.println("Q3: " + Q3);
    // System.out.println("Aq: " + Aq);

    // for (Medicao medicao : valores) {
    // if (medicao.getLeitura() >= Q1 - 1.5 * Aq && medicao.getLeitura() <= Q3 + 1.5
    // * Aq) {
    // proc.add(medicao);
    // }
    // }

    // System.out.println("Processadas: " + proc);
    // }
    // }
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }

    // private static double calculteMedian(List<Medicao> values) {
    // if (values.size() % 2 == 0)
    // return (values.get(values.size() / 2).getLeitura() + values.get(values.size()
    // / 2 - 1).getLeitura()) / 2;
    // else
    // return values.get(values.size() / 2).getLeitura();
    // }

    // TODO: Criar sistema de controlo de ID para nao ter AI
    public void sendRecordsToMySQL() throws SQLException {
        for (String s : sensores) {
            for (Medicao m : medicoes.get(s)) {
                // for (Medicao m : processadas) {
                String query = "INSERT INTO Medicao(IDZona, Sensor, DataHora, Leitura) VALUES(" + "'" +
                        m.getZona().split("Z")[1] + "', '" + m.getSensor() + "', '" + m.getHora()
                        + "', " + m.getLeitura() + ")";
                sql_connection_to.prepareStatement(query).execute();
                System.out.println("MySQL query: " + query);
            }
        }
    }

    public static void main(String[] args) {
        try {
            MQTTToMySQL mqttsql = new MQTTToMySQL(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
            mqttsql.connectToMQTT();
            mqttsql.connectFromMySql();
            mqttsql.connectToMySql();
            // System.err.println("init funciton");
            mqttsql.receiveAndSendLastRecords();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}