package org.pt.iscte;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.ini4j.Ini;

import javax.xml.transform.Result;
import java.io.File;
import java.sql.*;
import java.util.*;

public class alertasDeles {

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

    private final String[] sensors;

    private final List<Document> receivedMessages = new ArrayList<>();

    private Map<String, ArrayList<Record>> records = new HashMap<>();
    private Map<String, Double[]> sensorsLimits = new HashMap<>();
    private Map<String, Double> limiares = new HashMap<>();

    public alertasDeles(Ini ini) {
        cloud_topic_to = ini.get(CLOUD_DESTINATION, "cloud_topic_to");
        cloud_server_to = ini.get(CLOUD_DESTINATION, "cloud_server_to");
        cloud_client_name_to = ini.get(CLOUD_DESTINATION, "cloud_client_to");

        sql_database_connection_from = ini.get(MYSQL_ORIGIN, "sql_database_connection_from");
        sql_database_user_from = ini.get(MYSQL_ORIGIN, "sql_database_user_from");
        sql_database_password_from = ini.get(MYSQL_ORIGIN, "sql_database_password_from");
        sql_select_table_from = ini.get(MYSQL_ORIGIN, "sql_select_from_table");

        sql_database_connection_to = ini.get(MYSQL_DESTINATION, "sql_database_grupo01_connection_to");
        sql_database_user_to = ini.get(MYSQL_DESTINATION, "sql_database_user_to");
        sql_database_password_to = ini.get(MYSQL_DESTINATION, "sql_database_password_to");
        sql_grey_alert_delay = Integer.parseInt(ini.get(MYSQL_DESTINATION, "sql_grey_alert_delay"));

        sensors = ini.get("Mongo Origin", "mongo_sensores_from").toString().split(",");

        for (String sensor : sensors) {
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
                        removeAnomalousValues();

                        generateAlerts();

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

    public void getSensorsLimits() throws SQLException {
        Statement statement = sql_connection_from.createStatement();
        ResultSet rs = statement.executeQuery(sql_select_table_from);
        while (rs.next()) {
            sensorsLimits.put(rs.getString(2) + rs.getInt(1),
                    new Double[]{rs.getDouble(3), rs.getDouble(4)});
        }
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

    public void sendRecordsToMySQL() throws SQLException {
        for (String s : sensors) {
            for (Record m : records.get(s)) {
                // for (Medicao m : processadas) {
                String query = "INSERT INTO Medicao(IDZona, Sensor, DataHora, Leitura) VALUES(" + "'" +
                        m.getZona().split("Z")[1] + "', '" + m.getSensor() + "', '" + m.getHora()
                        + "', " + m.getLeitura() + ")";
                sql_connection_to.prepareStatement(query).execute();
                System.out.println("MySQL query: " + query); // AQUI
            }
        }
    }

    public void generateAlerts() throws SQLException {
        for (ArrayList<Record> listOfRecords : records.values())
            //TALVEZ FAZER LISTAS JUNTANDOS TODOS OS RECORDS DAS CULTURAS DIFERETES
            for (Record r : listOfRecords) {
                Statement statement = sql_connection_to.createStatement();
                ResultSet rs = statement.executeQuery("SELECT IDCultura,NomeCultura,IDUtlizador FROM Cultura WHERE IDZona = "
                        + r.getZona());
                String IDCultura = rs.getString(1);
                String NomeCultura = rs.getString(2);
                String IDUtlizador = rs.getString(3);
                while (rs.next()) {//VER SE ISTO ASSIM NAO DA PROBLEMAS COM O NEXT AO INVES DE HASNEXT. NAO VAI SALTAR UM?
                    ResultSet limir = null;
                    ResultSet min_max_amarelo = null;
                    if (r.getSensor().charAt(0) == 'T') {
                        min_max_amarelo = statement.executeQuery(
                                "SELECT TemperaturaMin, TemperaturaMax from ParametroCultura where IDCultura" + IDCultura
                        );
                        limir = statement.executeQuery(
                                "SELECT TemperaturaLim FROM ParametroCultura WHERE IDCultura = " + IDCultura);
                    } else if (r.getSensor().charAt(0) == 'H') {
                        min_max_amarelo = statement.executeQuery(
                                "SELECT HumidadeMin, HumidadeMax from ParametroCultura where IDCultura" + IDCultura
                        );
                        limir = statement.executeQuery(
                                "SELECT HumidadeLim FROM ParametroCultura WHERE IDCultura = " + IDCultura);

                    } else if (r.getSensor().charAt(0) == 'L') {
                        min_max_amarelo = statement.executeQuery(
                                "SELECT Luzmin, LuzMax from ParametroCultura where IDCultura" + IDCultura
                        );
                        limir = statement.executeQuery(
                                "SELECT LuzLim FROM ParametroCultura WHERE IDCultura = " + IDCultura);
                    }

                    double leitura = r.getLeitura();
                    double limiar = limir.getDouble(1);
                    double min = min_max_amarelo.getDouble(1);
                    double max = min_max_amarelo.getDouble(2);

                    String tipoAlerta = "";
                    String mensagem = "";



                    //MIN
                    //PRECISO DO ID UTILIZADOR
                    if (leitura >= (min - limiar) && leitura < (min - 0.5 * limiar)) {
                        tipoAlerta = "A";
                        mensagem = "Mensagem alerta Amarelo de baixo";
                    }

                    if (leitura > (min + limiar * 0.5) && leitura <= (min + limiar)) {
                        tipoAlerta = "A";
                        mensagem = "Mensagem alerta Amarelo de cima";
                    } else if () {

                    }

                    if (tipoAlerta != "") {
                        String query = "INSERT INTO Alerta(IDUtilizador,IDCultura,IDZona,IDSensor,Hora,Leitura,TipoAlerta,NomeCultura,Mensagem,HoraEscrita) " +
                                "VALUES(" + IDUtlizador + "," + IDCultura + "," + r.getZona() + "," + r.getSensor() + "," + r.getHora() + "," + r.getLeitura() +
                                "" + tipoAlerta + "," + NomeCultura + "," + mensagem + "," + new Timestamp(System.currentTimeMillis()) + ")";
                        sql_connection_to.prepareStatement(query).execute();
                        System.out.println("O ALERTA: " + query);
                    }

                }
            }
    }


    public static void main(String[] args) {
        try {
            alertasDeles mqttsql = new alertasDeles(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
            mqttsql.connectToMQTT();
            mqttsql.connectFromMySql();
            mqttsql.connectToMySql();
            mqttsql.receiveAndSendLastRecords();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
