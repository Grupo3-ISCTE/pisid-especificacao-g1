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
    String[] sensors;
    MMap records = new MMap();
    Map<String, Double[]> sensorsLimits = new HashMap<>();

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
                Medicao m = new Medicao(r);
                this.records.get(m.getSensor()).add(m);
                c.updateOne(r, new BasicDBObject().append("$inc", new BasicDBObject().append("Migrado", 1)));
            }
        }
    }

    public void removeDuplicatedValues() {
        MMap temp = new MMap();
        for (String sensor : sensors) {
            if (!records.get(sensor).isEmpty()) {
                temp.get(sensor).add(records.get(sensor).get(0));
                try {
                    for (int i = 1; i < records.get(sensor).size(); i++)
                        if (records.get(sensor).get(i).getLeitura() != records.get(sensor).get(i - 1)
                                .getLeitura()) {
                            temp.get(sensor).add(records.get(sensor).get(i));
                        }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        records = temp;
    }

    // TODO: fazer método caso o professor decida duplicar os bat
    public void removerValoresNaMesmaHora() {

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

    // TODO: nao sei calcular os quartis
    public void removeOutliers() {
        try {
            // if (!medicoes.isEmpty()) {
            // medicoes.sort();
            // for (String s : listaSensores) {
            // if (!medicoes.get(s).isEmpty()) {
            // double q1 = medicoes.get(s).get(3 * (medicoes.get(s).size() + 1) /
            // 4).getLeitura();
            // double q3 = medicoes.get(s).get((medicoes.get(s).size() + 1) / 4)
            // .getLeitura();
            // double iqr = q3 - q1;
            // for (int i = 0; i < medicoes.get(s).size(); i++) {
            // double val = medicoes.get(s).get(i).getLeitura();
            // if (val < q1 - iqr * 1.5 || val > q3 + iqr * 1.5) {
            // // System.out.println("OUTLIER: " + medicoes.get(s).get(i));
            // medicoes.get(s).remove(i);
            // i--;
            // }
            // }
            // }
            // }
            // }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendRecordsToMySQL() throws SQLException {
        for (String s : sensors) {
            for (Medicao m : records.get(s)) {
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
                mongoToMySQL.removeDuplicatedValues();
                mongoToMySQL.removerValoresNaMesmaHora();
                mongoToMySQL.getSensorsLimits();
                mongoToMySQL.removeAnomalousValues();
                mongoToMySQL.removeOutliers();
                mongoToMySQL.sendRecordsToMySQL();
                mongoToMySQL.records.clear();
                Thread.sleep(sql_delay_to);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}