package org.pt.iscte;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.ini4j.Ini;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;

public class MongoToMySQL {

    private static final String MONGO_DESTINATION = "Mongo Destination";
    private static final String MYSQL_ORIGIN = "Mysql Origin";
    private static final String MYSQL_DESTINATION = "Mysql Destination";
    private static final int PAST_MINUTES_FOR_MONGO_FIND = 1;

    private final String mongo_address_to;
    private final int mongo_port_to;
    private final String mongo_database_name_to;
    private final String mongo_user_to;
    private final char[] mongo_password_to;
    private final String mongo_credential_database_to;
    private MongoDatabase mongo_database_to;

    private final String sql_database_connection_from;
    private final String sql_database_user_from;
    private final String sql_database_password_from;
    private Connection sql_connection_from;
    private final String sql_select_table_from;

    private final String sql_database_connection_to;
    private final String sql_database_user_to;
    private final String sql_database_password_to;
    private Connection sql_connection_to;

    private final List<MongoCollection<Document>> collections = new ArrayList<>();
    private final String[] sensors;
    private Map<String, ArrayList<Record>> records = new HashMap<>();
    private Map<String , Record> previousRecords = new HashMap<>();
    private Map<String, Double[]> sensorsLimits = new HashMap<>();
    private static final int MIN_VALUES = 3;

    public MongoToMySQL(Ini ini) {
        mongo_address_to = ini.get(MONGO_DESTINATION, "mongo_address_to");
        mongo_port_to = Integer.parseInt(ini.get(MONGO_DESTINATION, "mongo_port_to"));
        mongo_database_name_to = ini.get(MONGO_DESTINATION, "mongo_database_to");
        mongo_user_to = ini.get(MONGO_DESTINATION, "mongo_user_to");
        mongo_password_to = ini.get(MONGO_DESTINATION, "mongo_password_to").toCharArray();
        mongo_credential_database_to = ini.get(MONGO_DESTINATION, "mongo_credential_database_to");

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

    public void connectToMongo() {
        MongoClient mongo_client_to = new MongoClient(new ServerAddress(mongo_address_to, mongo_port_to), List
                .of(MongoCredential.createCredential(mongo_user_to, mongo_credential_database_to, mongo_password_to)));
        mongo_database_to = mongo_client_to.getDatabase(mongo_database_name_to);

    }

    public void connectFromMySql() throws SQLException {
        sql_connection_from = DriverManager.getConnection(sql_database_connection_from, sql_database_user_from,
                sql_database_password_from);
    }

    public void connectToMySql() throws SQLException {
        sql_connection_to = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to,
                sql_database_password_to);
    }

    public void getCollections() {
        for (String s : sensors)
            collections.add(mongo_database_to.getCollection("sensor" + s.toLowerCase()));
    }

    private BasicDBObject getCriteriaForMongoSearch() {
        String[] date = new Timestamp(new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(-PAST_MINUTES_FOR_MONGO_FIND)).getTime()).toString().split(" ");
        date[1] = date[1].split("\\.")[0];
        String mongoDate = date[0] + "T" + date[1] + "Z";

        BasicDBObject criteria = new BasicDBObject();
        criteria.append("Migrado", 0);
        criteria.append("Data", new BasicDBObject("$gt", mongoDate));

        return criteria;
    }

    public void findAndSendLastRecords() {
        for (MongoCollection<Document> c : collections) {
            FindIterable<Document> records = c.find(getCriteriaForMongoSearch());
            for (Document r : records) {
                Record m = new Record(r);
                this.records.get(m.getSensor()).add(m);
                c.updateOne(r, new BasicDBObject().append("$inc", new BasicDBObject().append("Migrado", 1)));
                System.out.println(r); // AQUI
            }
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

    // TODO: se o da outra classe funcionar, meter aqui
    // Atenção que no fim da remoção deverá ser updated a lista records (é a que vai para o mysql)
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

    private void insertLastRecords() {
        // Inserts the last record of the last dump from mqtt per sensor (it never needs to be cleared)
        for(String sensor : sensors)
            if(!records.get(sensor).isEmpty())
                previousRecords.put(sensor, records.get(sensor).get(records.get(sensor).size()-1));
    }

    public void sendRecordsToMySQL() throws SQLException {
        for (String s : sensors) {
            for (Record m : records.get(s)) {
                String query = "INSERT INTO Medicao(IDZona, Sensor, DataHora, Leitura) VALUES("
                        + "'" + m.getZona().split("Z")[1] + "', '" + m.getSensor() + "', '" + m.getHora()
                        + "', " +
                        m.getLeitura()
                        + ")";
                sql_connection_to.prepareStatement(query).execute();
                System.out.println("MySQL query: " + query);
            }
        }
    }

    public static void main(String args[]) {
        try {
            Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
            int sql_delay_to = Integer.parseInt(ini.get("Mysql Destination", "sql_delay_to"));

            MongoToMySQL mongoToMySQL = new MongoToMySQL(ini);
            mongoToMySQL.connectToMongo();
            mongoToMySQL.connectFromMySql();
            mongoToMySQL.connectToMySql();
            mongoToMySQL.getCollections();
            while (true) {
                mongoToMySQL.findAndSendLastRecords();
                mongoToMySQL.removeDuplicatedValuesAndDates();
                mongoToMySQL.getSensorsLimits();
                mongoToMySQL.removeAnomalousValues();
                mongoToMySQL.removeOutliers();
                mongoToMySQL.insertLastRecords();
                mongoToMySQL.sendRecordsToMySQL();
                for (ArrayList<Record> list : mongoToMySQL.records.values())
                    list.clear();
                Thread.sleep(sql_delay_to);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}