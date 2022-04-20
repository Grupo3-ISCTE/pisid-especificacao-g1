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

import static com.mongodb.client.model.Filters.eq;

public class MongoToMySQL {

    private static final String MONGO_DESTINATION = "Mongo Destination";
    private static final String MYSQL_ORIGIN = "Mysql Origin";
    private static final String MYSQL_DESTINATION = "Mysql Destination";

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
    String[] sensores = new String[] { "T1", "T2", "H1", "H2", "L1", "L2" };
    MMap medicoes = new MMap();
    Map<String, Double[]> limitesSensores = new HashMap<>();

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
        for (String s : sensores)
            collections.add(mongo_database_to.getCollection(s));
    }

    public void getMedicoesFromMongo() {
        for (MongoCollection<Document> c : collections) {
            FindIterable<Document> records = c.find(eq("Migrado", 0));
            for (Document r : records) {
                Medicao m = new Medicao(r);
                // System.out.println(m);
                medicoes.get(m.getSensor()).add(m);
                c.updateOne(r, new BasicDBObject().append("$inc", new BasicDBObject().append("Migrado", 1)));
            }
        }
    }

    // TODO: Fazer método
    public void removerMedicoesInvalidas() {

    }

    public void removerValoresDuplicados() {
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

    // TODO: nao sei calcular os quartis
    public void removerOutliers() {
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

    public void criarEMandarQueries() throws SQLException {
        for (String s : sensores) {
            for (Medicao m : medicoes.get(s)) {
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
            MongoToMySQL mongoToMySQL = new MongoToMySQL(ini);
            int sql_delay_to = Integer.parseInt(ini.get("Mysql Destination", "sql_delay_to"));

            mongoToMySQL.connectToMongo();
            mongoToMySQL.connectFromMySql();
            mongoToMySQL.connectToMySql();
            mongoToMySQL.getCollections();
            while (true) {
                mongoToMySQL.getMedicoesFromMongo();
                mongoToMySQL.removerValoresDuplicados();
                mongoToMySQL.removerValoresNaMesmaHora();
                mongoToMySQL.analisarTabelaSensor();
                mongoToMySQL.removerValoresAnomalos();
                mongoToMySQL.removerOutliers();
                mongoToMySQL.criarEMandarQueries();
                mongoToMySQL.medicoes.clear();
                Thread.sleep(sql_delay_to);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}