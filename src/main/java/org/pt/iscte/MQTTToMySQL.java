package org.pt.iscte;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    String[] listaSensores = new String[] { "T1", "T2", "H1", "H2", "L1", "L2" };
    List<Document> mensagensRecebidas = new ArrayList<>();
    MMap medicoes = new MMap();
    Map<String, Double[]> limitesSensores = new HashMap<>();
    ArrayList<Medicao> processadas = new ArrayList<>();

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
            cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
                if (!msg.toString().equals("fim")) {
                    mensagensRecebidas.add(stringToDocument(msg));
                } else {
                    removerMensagensRepetidas();
                    dividirMedicoes();
                    removerMedicoesInvalidas();
                    removerValoresDuplicados();
                    removerValoresNaMesmaHora();
                    analisarTabelaSensor();
                    removerValoresAnomalos();
                    // removerOutliers();
                    criarEMandarQueries();

                    medicoes.clear();
                    mensagensRecebidas.clear();
                }
            });
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public Document stringToDocument(MqttMessage msg) {
        String m = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ", "\",\"");
        m = m.substring(1, m.length() - 1).replace("}", "\"}").replace("{", "{\"");
        return Document.parse(m);
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
            // System.out.println(m);
            medicoes.get(m.getSensor()).add(m);
        }
    }

    // TODO: Fazer método
    public void removerMedicoesInvalidas() {
        // TODO document why this method is empty
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

    // TODO: fazer método caso o professor decida duplicar os bat
    public void removerValoresNaMesmaHora() {

    }

    public void analisarTabelaSensor() throws SQLException {
        Statement statement = sql_connection_from.createStatement();
        ResultSet rs = statement.executeQuery(sql_select_table_from);
        while (rs.next()) {
            limitesSensores.put(rs.getString(2) + rs.getInt(1),
                    new Double[] { rs.getDouble(3), rs.getDouble(4) });
        }
    }

    // TODO: Enviar alertas cinzentos
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

    // JOAO TRAVASSOS
    // TODO: Fazer método (ordenar por data)
    public void removerOutliers() {
        try {
            if (!medicoes.isEmpty()) {
                DetectOutliers detetor = null;
                for (char type : MMap.sensorTypes) {
                    detetor = new DetectOutliers();
                    // processadas = processadas +
                    // detetor.eliminateOutliers(medicoes.getValuesAsArray(type),1.5f);
                    // processadas.addAll(detetor.eliminateOutliers(medicoes.getValuesAsArray(type),
                    // 1.5f));
                    System.out.println(type);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: Criar sistema de controlo de ID para nao ter AI
    public void criarEMandarQueries() throws SQLException {
        for (String s : listaSensores) {
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
            mqttsql.receiveAndSendLastRecords();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}