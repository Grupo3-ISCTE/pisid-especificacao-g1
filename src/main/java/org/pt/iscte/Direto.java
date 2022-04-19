package org.pt.iscte;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.ini4j.Ini;

import javax.naming.InitialContext;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Direto {

    private final Ini ini;

    private MongoCollection<Document> mongo_collection_from_cloud;

    private MongoDatabase mongo_database_local;
    private Connection sql_connection_from;
    private Connection sql_connection_to;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    String[] listaSensores = new String[] {"T1", "T2", "H1", "H2", "L1", "L2"};
    List<Document> mensagensRecebidas = new ArrayList<>();
    MMap medicoes = new MMap();
    Map<String, Double[]> limiteSensores = new HashMap<>();


    public Direto(Ini ini){
       this.ini=ini;
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
        mongo_collection_from_cloud = mongo_database_from.getCollection(ini.get("Mongo Origin", "mongo_collection_from"));
    }

    /**
     * * Conecção à base de dados MongoDB Local
     * ? necessário colocar user e password
     */
    public void connectToMongo() {
        MongoClient mongo_client_to = new MongoClient(ini.get("Mongo Destination", "mongo_address_to"),
                Integer.parseInt(ini.get("Mongo Destination", "mongo_port_to")));
        mongo_database_local = mongo_client_to
                .getDatabase(ini.get("Mongo Destination", "mongo_database_to"));
    }

    /**
     * * Transferência de dados do MongoDB da Cloud para o MongoDB Local
     */
    public void mongoToMongo() {
        new Thread(() -> {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(new Timestamp(System.currentTimeMillis()).getTime());
            while (true) {
                FindIterable<Document> records = mongo_collection_from_cloud
                        .find(new Document("Data", sdf.format(new Timestamp(cal.getTime().getTime())))).limit(6);
                try {
                    for (Document record : records) {
                        Document leituraTransformada = new Document();
                        leituraTransformada.append("_id", record.getObjectId("_id"));
                        leituraTransformada.append("Zona", record.getString("Zona"));
                        leituraTransformada.append("Sensor", record.getString("Sensor"));
                        leituraTransformada.append("Data", record.getString("Data"));
                        leituraTransformada.append("Medicao", record.getString("Medicao"));
                        leituraTransformada.append("Migrado", 0);
                        MongoCollection<Document> mongo_collection_to = mongo_database_local
                                .getCollection("sensor" + record.getString("Sensor").toLowerCase());
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
     * * Conecção ao MySQL da Cloud para posterior análise da tabela Sensor
     * ? Para que vai ser utilizada a tabela Zona?
     */
    public void connectFromMySql() throws SQLException {
        String connection_link = ini.get("Mysql Origin", "sql_database_connection_from");
        String user = ini.get("Mysql Origin", "sql_database_user_from");
        String password = ini.get("Mysql Origin", "sql_database_password_from");
        sql_connection_from = DriverManager.getConnection(connection_link, user, password);
    }

    /**
     * * Conecção ao MySQL Local para envio posterior de queries
     */
    public void connectToMySql() throws SQLException {
        sql_connection_to = DriverManager.getConnection(ini.get("Mysql Destination", "sql_database_connection_to"),
                ini.get("Mysql Destination", "sql_database_user_to"),
                ini.get("Mysql Destination", "sql_database_password_to"));
    }

    /**
     * * Envio dos dados do Broker para o MySQL
     */
//    public void mongoToMySQL() {
//        try {
//            cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
//                if (!msg.toString().equals("fim")) {
//                    mensagensRecebidas.add(stringToDocument(msg));
//                    //System.out.println(msg);
//                } else {
//                    removerMensagensRepetidas();
//                    dividirMedicoes();
//                    removerValoresDuplicados();
//                    analisarTabelaSensor();
//                    removerValoresAnomalos();
//                    // removerOutliers();
//                    criarEMandarQueries();
//
//                    medicoes.clear();
//                    mensagensRecebidas.clear();
//                }
//            });
//        } catch (MqttException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Document stringToDocument(MqttMessage msg) {
//        String m = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ",
//                "\",\"");
//        m = m.substring(1, m.length() - 1).replace("}", "\"}").replace("{", "{\"");
//        // System.out.println(m);
//        return Document
//                .parse(m);
//    }
//
//    public void removerMensagensRepetidas() {
//        List<Document> temp = new ArrayList<>();
//        for (Document d : mensagensRecebidas)
//            if (!temp.contains(d))
//                temp.add(d);
//        mensagensRecebidas = temp;
//    }
//
//    public void dividirMedicoes() {
//        for (Document d : mensagensRecebidas) {
//            Medicao m = new Medicao(d);
//            //System.out.println(m);
//            medicoes.get(m.getSensor()).add(m);
//        }
//    }
//
//    public void removerValoresDuplicados() {
//        MMap temp = new MMap();
//        for (String sensor : listaSensores) {
//            if (!medicoes.get(sensor).isEmpty()) {
//                temp.get(sensor).add(medicoes.get(sensor).get(0));
//                try {
//                    for (int i = 1; i < medicoes.get(sensor).size(); i++)
//                        if (medicoes.get(sensor).get(i).getLeitura() != medicoes.get(sensor).get(i - 1)
//                                .getLeitura()) {
//                            temp.get(sensor).add(medicoes.get(sensor).get(i));
//                        }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        medicoes = temp;
//    }
//
//    public void analisarTabelaSensor() throws SQLException {
//        Statement statement = sql_connection_from.createStatement();
//        ResultSet rs = statement.executeQuery(ini.get("Mysql Origin", "sql_select_from_table"));
//        while (rs.next()) {
//            limitesSensores.put(rs.getString(2) + rs.getInt(1),
//                    new Double[] { rs.getDouble(3), rs.getDouble(4) });
//        }
//    }
//
//    /**
//     * ! Devem ser enviados aqui alertas cinzentos
//     */
//    public void removerValoresAnomalos() {
//        for (String s : listaSensores) {
//            if (!medicoes.get(s).isEmpty()) {
//                for (int i = 0; i < medicoes.get(s).size(); i++) {
//                    if (medicoes.get(s).get(i).getLeitura() < limitesSensores.get(s)[0]
//                            || medicoes.get(s).get(i).getLeitura() > limitesSensores.get(s)[1]) {
//                        medicoes.get(s).remove(i);
//                        i--;
//                    }
//                }
//            }
//        }
//    }

    // !Testes metodo sort e remoçao de outliers
    // !Os quartis estao mal feitos
    public void removerOutliers() {
        try {
            if (!medicoes.isEmpty()) {
                medicoes.sort();
                for (String s : listaSensores) {
                    if (!medicoes.get(s).isEmpty()) {
                        double q1 = medicoes.get(s).get(3 * (medicoes.get(s).size() + 1) / 4).getLeitura();
                        double q3 = medicoes.get(s).get((medicoes.get(s).size() + 1) / 4)
                                .getLeitura();
                        double iqr = q3 - q1;
                        for (int i = 0; i < medicoes.get(s).size(); i++) {
                            double val = medicoes.get(s).get(i).getLeitura();
                            if (val < q1 - iqr * 1.5 || val > q3 + iqr * 1.5) {
                                // System.out.println("OUTLIER: " + medicoes.get(s).get(i));
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

    /**
     * ! Se calhar é melhor termos um sistema de controlo do ID porque já tenho 610
     * ! de IDMedicao mas so 16 medicoes na tabela xD
     *
     */
    public void criarEMandarQueries() throws SQLException {
        for (String s : listaSensores) {
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

    public static void main(String args[]){
        try{
            Direto direto = new Direto(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
            direto.connectFromMongo();
            direto.connectToMongo();
            direto.mongoToMongo();
            direto.connectFromMySql();
            direto.connectToMySql();
//            direto.mongoToMySQL();

        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
