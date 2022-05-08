package org.pt.iscte;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private int sql_grey_alert_delay;
    private int max_number_of_recs_to_use_past_recs;

    private final String[] sensors;
    private final List<Document> receivedMessages = new ArrayList<>();
    private Map<String, ArrayList<Record>> records = new HashMap<>();
    private Map<String, ArrayList<Record>> previousRecords = new HashMap<>();
    private Map<String, Double[]> sensorsLimits = new HashMap<>();
    private List<Record> recordsForGreyAlerts = new ArrayList<>();


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
        sql_grey_alert_delay = Integer.parseInt(ini.get(MYSQL_DESTINATION, "sql_grey_alert_delay"));

        sensors = ini.get("Mongo Origin", "mongo_sensores_from").toString().split(",");
        max_number_of_recs_to_use_past_recs = Integer.parseInt(ini.get("Java", "max_number_of_recs_to_use_past_recs"));

        for (String sensor : sensors) {
            previousRecords.put(sensor, new ArrayList<>());
            records.put(sensor, new ArrayList<>());
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
                        getSensorsLimits();
                        removeDuplicatedDates();
                        removeOutliers(); // remover antes tmb porque 0 -1 -1 -1 -1 14 14 14 14 14 -1 -1 -1 -1 (H1)
                        removeAnomalousValues();
                        sendGreyAlerts();
                        removeOutliers();
                        insertLastRecords();
                        sendRecordsToMySQL();

                        for (ArrayList<Record> listOfRecords : records.values())
                            listOfRecords.clear();

                        receivedMessages.clear();
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
            System.out.println(d); // AQUI
        }
    }

    // Removes duplicated timestamps
    public void removeDuplicatedDates() {
        Map<String, ArrayList<Record>> temp = new HashMap<>();
        for (String sensor : sensors) {
            temp.put(sensor, new ArrayList<>());
            if (!records.get(sensor).isEmpty()) {
                try {
                    if (previousRecords.get(sensor) == null) {
                        temp.get(sensor).add(records.get(sensor).get(0));
                    } else if (previousRecords.get(sensor).isEmpty()) {
                        temp.get(sensor).add(records.get(sensor).get(0));
                    }else{
                        int size = previousRecords.get(sensor).size();
                        if (!records.get(sensor).get(0).getHora().equals(previousRecords.get(sensor).get(size - 1).getHora()))
                            temp.get(sensor).add(records.get(sensor).get(0));
                    }
                    for (int i = 1; i < records.get(sensor).size(); i++) {
                        if (!records.get(sensor).get(i).getHora().equals(records.get(sensor).get(i - 1).getHora()))
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

    public void removeAnomalousValues() {
        for (String s : sensors) {
            if (!records.get(s).isEmpty()) {
                for (int i = 0; i < records.get(s).size(); i++) {
                    if (records.get(s).get(i).getLeitura() < sensorsLimits.get(s)[0]
                            || records.get(s).get(i).getLeitura() > sensorsLimits.get(s)[1]) {
                        recordsForGreyAlerts.add(records.get(s).get(i));
                        records.get(s).remove(i);
                        i--;
                    }
                }
            }
        }
    }

    public void sendGreyAlerts() throws SQLException {
        for (Record r : recordsForGreyAlerts) {
            Statement statement = sql_connection_to.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT IDCultura, IDUtilizador, NomeCultura FROM cultura WHERE IDZona = "
                            + r.getZona().split("Z")[1] + " AND Estado = 'A'");
            while (rs.next()) {
                ResultSet last = statement.executeQuery(
                        "SELECT DataHoraEscrita FROM alerta WHERE IDAlerta = (SELECT max(IDAlerta) FROM alerta WHERE IDZona = "
                                + r.getZona().split("Z")[1] + " AND Sensor = '" + r.getSensor() + "'"
                                + " AND TipoAlerta = 'C' ) AND IDZona = "
                                + r.getZona().split("Z")[1] + " AND Sensor = '" + r.getSensor() + "'"
                                + " AND TipoAlerta = 'C' ");

                if (!last.next() || new Timestamp(System.currentTimeMillis()).getTime() > (last.getTimestamp(1)
                        .getTime() + TimeUnit.MINUTES.toMillis(sql_grey_alert_delay))) {
                    String query = "INSERT INTO Alerta(IDZona, IDCultura, IDUtilizador, NomeCultura, Sensor, Leitura, DataHora, DataHoraEscrita, TipoAlerta, Mensagem) VALUES("
                            + r.getZona().split("Z")[1] + ", "
                            + rs.getString(1) + ", '"
                            + rs.getString(2) + "', '"
                            + rs.getString(3) + "', '"
                            + r.getSensor() + "', "
                            + r.getLeitura() + ", '"
                            + r.getHora() + "', '"
                            + new Timestamp(System.currentTimeMillis()) + "', '"
                            + "C" + "', '"
                            + "Potencial avaria detetada no sensor " + r.getSensor() + " da Zona "
                            + r.getZona().split("Z")[1] + " onde se encontra(m) a(s) sua(s) cultura(s)." + "')";
                    sql_connection_to.prepareStatement(query).execute();
                    System.out.println("Grey Alert: " + query);
                }
            }
        }
        recordsForGreyAlerts.clear();
    }

    public void removeOutliers() {
        if (!records.isEmpty()) {
            for (String s : sensors) {
                if (!records.get(s).isEmpty()) {
                    List<Record> temp = new ArrayList<>();
                    if (records.get(s).size() < max_number_of_recs_to_use_past_recs) {
                        temp.addAll(records.get(s));
                        if (previousRecords.get(s) != null)
                            temp.addAll(previousRecords.get(s));
                    } else
                        temp = records.get(s);

                    List<Record> analize = findDuplicateValuesToCalculateOutliers(temp).stream()
                            .collect(Collectors.toList());
                    Collections.sort(analize);

                    double q1 = analize.get((int) ((analize.size() - 1) * 0.25)).getLeitura();
                    double q3 = analize.get((int) ((analize.size() - 1) * 0.75)).getLeitura();
                    double aq = q3 - q1;

                    for (int i = 0; i != records.get(s).size(); i++) {
                        if (records.get(s).get(i).getLeitura() < (q1 - 1.5 * aq)
                                || records.get(s).get(i).getLeitura() > (q3 + 1.5 * aq)) {
                            System.out.println("Removi: " + records.get(s).get(i));
                            records.get(s).remove(i);
                            i--;
                        }
                    }
                }
            }
        }
    }

    public List<Record> findDuplicateValuesToCalculateOutliers(List<Record> rec) {
        List<Record> temp = new ArrayList<>();
        temp.add(rec.get(0));
        for (int i = 1; i < rec.size(); i++)
            if (rec.get(i).getLeitura() != rec.get(i - 1)
                    .getLeitura()) {
                temp.add(rec.get(i));
            }
        return temp;
    }

    public void sendRecordsToMySQL() throws SQLException {
        for (String s : sensors) {
            for (Record m : records.get(s)) {
                String query = "INSERT INTO Medicao(IDZona, Sensor, DataHora, Leitura) VALUES(" + "'" +
                        m.getZona().split("Z")[1] + "', '" + m.getSensor() + "', '" + m.getHora()
                        + "', " + m.getLeitura() + ")";
                sql_connection_to.prepareStatement(query).execute();
                System.out.println("MySQL query: " + query); // AQUI
            }
        }
    }

    private void insertLastRecords() {
        //previousRecords.clear(); // cant clear otherwise we lose past records
        for (String s : sensors)
            if (!records.get(s).isEmpty()) {
                System.out.println("Previous antes: " + previousRecords.get(s));
                previousRecords.put(s, (ArrayList<Record>) records.get(s).clone());
                System.out.println("Previous depois: " + previousRecords.get(s));
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