package org.pt.iscte;

import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;

import java.io.File;
import java.sql.*;
import java.util.*;

public class MQTTToMySQL2 {

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

    private double max_growth_outliers_percentage;
    private final String[] sensors;
    private final List<Document> receivedMessages = new ArrayList<>();

    private Map<String, ArrayList<Record>> records = new HashMap<>();
    private Map<String, ArrayList<Record>> previousRecords = new HashMap<>();
    private Map<String, Double[]> sensorsLimits = new HashMap<>();
    private Map<String, Double> sensorOutlierRanges = new HashMap<>();
    private Map<String, Record> previousRecord = new HashMap<>();
    private List<Record> recordsForGreyAlerts = new ArrayList<>();

    public MQTTToMySQL2(Ini ini) {
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
        max_growth_outliers_percentage = 0.01 * Integer.parseInt(ini.get("Java", "max_growth_outliers"));

        for (String sensor : sensors) {
            records.put(sensor, new ArrayList<>());
            previousRecords.put(sensor, new ArrayList<>());
            previousRecord.put(sensor, null);
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
                        removeAnomalousValues();
                        removeOutliers2();
                        generateAlerts();

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

    public void getSensorsLimits() throws SQLException {
        Statement statement = sql_connection_from.createStatement();
        ResultSet rs = statement.executeQuery(sql_select_table_from);
        while (rs.next()) {
            sensorsLimits.put(rs.getString(2) + rs.getInt(1),
                    new Double[] { rs.getDouble(3), rs.getDouble(4) });
            sensorOutlierRanges.put(rs.getString(2) + rs.getInt(1),
                    max_growth_outliers_percentage * (rs.getDouble(4) - rs.getDouble(3)));
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

    public void removeOutliers2() {
        for (String s : sensors) {
            if (!records.get(s).isEmpty()) {
                // caso nao exista um previous record
                if (previousRecord.get(s) == null)
                    previousRecord.put(s, records.get(s).get(0));
                // outras situações
                for (int i = 0; i != records.get(s).size(); i++) {
                    if (Math.abs(records.get(s).get(i).getLeitura()
                            - previousRecord.get(s).getLeitura()) > sensorOutlierRanges.get(s)) {
                        records.get(s).remove(i);
                        i--;
                    } else
                        previousRecord.put(s, records.get(s).get(i));
                }
            }
        }
    }

    public void generateAlerts() throws SQLException {
        for (ArrayList<Record> listOfRecords : records.values()) {
            for (Record r : listOfRecords) {
                Statement statement = sql_connection_to.createStatement();
                ResultSet rs = sql_connection_to.createStatement()
                        .executeQuery("SELECT IDCultura,NomeCultura,IDUtilizador,Estado FROM Cultura WHERE IDZona = "
                                + r.getZona().charAt(1));
                while (rs.next()) {
                    String estado = rs.getString(4);
                    if (estado.equals("A")) {
                        String IDCultura = rs.getString(1);
                        String NomeCultura = rs.getString(2);
                        String IDUtilizador = rs.getString(3);
                        ResultSet min_max_lim = null;
                        switch (r.getSensor().charAt(0)) {
                            case 'T':
                                min_max_lim = statement.executeQuery(
                                        "SELECT TemperaturaMin, TemperaturaMax, TemperaturaLim from ParametroCultura where IDCultura = "
                                                + IDCultura);
                                break;
                            case 'H':
                                min_max_lim = statement.executeQuery(
                                        "SELECT HumidadeMin, HumidadeMax, HumidadeLim from ParametroCultura where IDCultura = "
                                                + IDCultura);
                                break;
                            case 'L':
                                min_max_lim = statement.executeQuery(
                                        "SELECT Luzmin, LuzMax, LuzLim from ParametroCultura where IDCultura = "
                                                + IDCultura);
                                break;
                        }
                        while (min_max_lim.next()) {
                            double leitura = r.getLeitura();
                            double min = min_max_lim.getDouble(1);
                            double max = min_max_lim.getDouble(2);
                            double lim = min_max_lim.getDouble(3);

                            String tipoAlerta = "";
                            String mensagem = "";

                            if ((leitura >= (max - lim) && leitura < (max - 0.5 * lim))
                                    || (leitura > (min + lim * 0.5) && leitura <= (min + lim))) {
                                tipoAlerta = "A";
                                mensagem = "[ALERTA Amarelo]";
                            } else if ((leitura >= max - 0.5 * lim && leitura < max)
                                    || (leitura > min && leitura <= min + 0.5 * lim)) {
                                tipoAlerta = "L";
                                mensagem = "[ALERTA Laranja]";
                            } else if (leitura <= min || leitura >= max) {
                                tipoAlerta = "V";
                                mensagem = "[ALERTA Vermelho]";
                            }

                            if (tipoAlerta != "") {
                                String query = "INSERT INTO Alerta(IDUtilizador ,IDCultura ,IDZona,IDSensor,DataHora,Leitura,TipoAlerta,NomeCultura,Mensagem,DataHoraEscrita) "
                                        +
                                        "VALUES('" + IDUtilizador + "'," + IDCultura + "," + r.getZona().charAt(1)
                                        + ",'"
                                        + r.getSensor() + "','" + r.getHora() + "'," + r.getLeitura() +
                                        ",'" + tipoAlerta + "','" + NomeCultura + "','" + mensagem + "','"
                                        + new Timestamp(DateUtils
                                                .round(new Timestamp(System.currentTimeMillis()), Calendar.SECOND)
                                                .getTime())
                                        + "')";
                                sql_connection_to.prepareStatement(query).execute();
                                System.out.println("Alert Query: " + query);
                            }
                        }
                    }
                }
            }
        }

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
        // Inserts the last record of the last dump from mqtt per sensor (it never needs
        // to be cleared)
        for (String sensor : sensors)
            if (!records.get(sensor).isEmpty())
                previousRecords.put(sensor, (ArrayList<Record>) records.get(sensor).clone()); // "put" makes it replace
                                                                                              // the list
        // Note to self: clone is very needed.
    }

    public static void main(String[] args) {
        try {
            MQTTToMySQL2 mqttsql = new MQTTToMySQL2(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
            mqttsql.connectToMQTT();
            mqttsql.connectFromMySql();
            mqttsql.connectToMySql();
            mqttsql.receiveAndSendLastRecords();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}