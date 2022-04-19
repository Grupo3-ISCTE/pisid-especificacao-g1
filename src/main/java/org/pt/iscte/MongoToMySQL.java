package org.pt.iscte;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.print.Doc;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;

/*
As migrações das leituras dos sensores do MongoDB local para o
MySQL deverão ser realizadas com uma periodicidade de 10 segundos de
forma automática, tentando assim aproximar migrações em tempo real
sem provocar um excesso de operações de migração.
Para que seja possível assegurar os pressupostos anteriores deve existir
apenas um main designado "DataBridge" sem threads associadas.
Primeiramente, o programa começa por estabelecer duas conexões,
uma com o MongoDB local e outra com o MySQL local.
De seguida, irá começar o processo de migração.
Obtém então os registos do MongoDB, processa-os efetuando os devidos
tratamentos e após o tratamento insere os selecionados no MySQL.
Cada processo de migração ocorre com uma periodicidade de 10 segundos,
como referido anteriormente, e, portanto, implica que o programa deverá
esperar 10 segundos para efetuar um novo processo de migração.
Nas leituras já migradas do MongoDB local para a base de dados MySql,
deve ser trocado o valor do campo "Migrado" de "0" para "1".
Este processo é feito de forma a garantir que não são enviados os
mesmos registos várias vezes tanto em condições normais como de
potenciais avarias. Adicionalmente, todos os registos que não
sejam migrados para o MySQL por terem sido descartados na
fase de tratamento de medições devem trocar o valor do campo
"Migrado" de "0" para "1".
*/

public class MongoToMySQL {
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
    String[] sensores = {"sensort1", "sensort2", "sensorh1", "sensorh2", "sensorl1", "sensorl2"};
    List<Document> temp = new ArrayList<>();
    List<Document> mensagensRecebidas = new ArrayList<>();
    MMap medicoes = new MMap();
    String[] listaSensores = new String[] { "T1", "T2", "H1", "H2", "L1", "L2" };
    Map<String, Double[]> limitesSensores = new HashMap<>();

    public MongoToMySQL(Ini ini) {
        mongo_address_to = ini.get("Mongo Destination", "mongo_address_to");
        mongo_port_to  = Integer.parseInt(ini.get("Mongo Destination", "mongo_port_to"));
        mongo_database_name_to = ini.get("Mongo Destination", "mongo_database_to");
        mongo_user_to = ini.get("Mongo Destination", "mongo_user_to");
        mongo_password_to = ini.get("Mongo Destination", "mongo_password_to").toCharArray();
        mongo_credential_database_to = ini.get("Mongo Destination","mongo_credential_database_to");

        sql_database_connection_from = ini.get("Mysql Origin", "sql_database_connection_from");
        sql_database_user_from = ini.get("Mysql Origin", "sql_database_user_from");
        sql_database_password_from = ini.get("Mysql Origin", "sql_database_password_from");
        sql_select_table_from = ini.get("Mysql Origin", "sql_select_from_table");

        sql_database_connection_to = ini.get("Mysql Destination", "sql_database_connection_to");
        sql_database_user_to = ini.get("Mysql Destination", "sql_database_user_to");
        sql_database_password_to = ini.get("Mysql Destination", "sql_database_password_to");
    }


    public void connectToMongo() {
        MongoClient mongo_client_to = new MongoClient(new ServerAddress(mongo_address_to,mongo_port_to),
                    List.of(MongoCredential.createCredential(mongo_user_to,mongo_credential_database_to,mongo_password_to)));
        mongo_database_to = mongo_client_to.getDatabase(mongo_database_name_to);
    }

    public void connectFromMySql() throws SQLException {
        sql_connection_from = DriverManager.getConnection(sql_database_connection_from, sql_database_user_from, sql_database_password_from);
    }

    public void connectToMySql() throws SQLException {
        sql_connection_to = DriverManager.
                getConnection(sql_database_connection_to,sql_database_user_to,sql_database_password_to);
    }

    public void getCollections() {
        System.out.println("vou fzr o get collections");
        for (String s : sensores) {
            collections.add(mongo_database_to.getCollection(s));
        }
    }

    public void findAndSendLastRecords() {
            try {
                for(MongoCollection<Document> c : collections) {
                    FindIterable<Document> records = c.find(eq("Migrado",0));
                    for (Document record : records) {
                       // System.out.println(record.toString());
                        mensagensRecebidas.add(record);
                    }
                }
            } catch(NumberFormatException e) {
                e.printStackTrace();
            }
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
        try {
            Medicao m = new Medicao(d);
            //System.out.println(m);
            medicoes.get(m.getSensor()).add(m);
        } catch (Exception e) {
            //TODO: handle exception
            //System.out.println("ERROU " + d.toString());
            System.out.println("WEEE");
            mudarMigradoDocumento(d);
        }

    }
  }

  // usa a medicao
    public void mudarMigradoMedicao(Medicao m) throws Exception {
      System.out.println(m.getNomeColecao() +  " " + m.getId());
      MongoCollection<Document> c = mongo_database_to.getCollection(m.getNomeColecao());
      /*FindIterable<Document> record = c.find(eq("_id ",m.getId()));
      System.out.println(record.first());
      for(Document r : record) {
          c.updateOne(r,new BasicDBObject().append("$inc", new BasicDBObject().append("Migrado", 1)) );
      }

       */
    }

  // usa o documento
  public void mudarMigradoDocumento(Document record) {
    System.out.println("ENCONTREI ERRADO D");
    String nomeColecao = "sensor" + record.get("Sensor").toString().toLowerCase();
    MongoCollection<Document> c = mongo_database_to.getCollection(nomeColecao);
    c.updateOne(record,new BasicDBObject().append("$inc", new BasicDBObject().append("Migrado", 1)) );
    //System.out.println(record.toString());
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
            else {
             mudarMigradoMedicao(medicoes.get(sensor).get(i)); // AQUI?
            }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    medicoes = temp;
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
           // mudarMigradoMedicao(medicoes.get(s).get(i));
            medicoes.get(s).remove(i);
            i--;
          }
        }
      }
    }
  }

  // TODO: Fazer método (ordenar por data)
  public void removerOutliers() {

  }

  //TODO: Criar sistema de controlo de ID para nao ter AI
  public void criarEMandarQueries() throws SQLException {
    for (String s : listaSensores) {
      for (Medicao m : medicoes.get(s)) {
        String query = "INSERT INTO Medicao(IDZona, Sensor, DataHora, Leitura) VALUES("+ "'" +
                m.getZona().split("Z")[1] + "', '" + m.getSensor() + "', '" + m.getHora()
                + "', " +m.getLeitura()+ ")";
        //sql_connection_to.prepareStatement(query).execute();
        //mudarMigradoMedicao(m);
        //System.out.println("MySQL query: " + query);
      }
    }
  }

    public static void main(String[] args) throws InterruptedException, InvalidFileFormatException, IOException, SQLException {
        Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
        int sql_delay_to = Integer.parseInt(ini.get("Mysql Destination", "sql_delay_to"));
        System.out.println("comecei");
        try {
            MongoToMySQL mtmsql = new MongoToMySQL(ini);
            mtmsql.connectToMongo();
            mtmsql.connectFromMySql();
            mtmsql.connectToMySql();
            mtmsql.getCollections();

            while(true) {
                 // adiciona os dados a uma estrutura
                mtmsql.findAndSendLastRecords();
                // percorre sempre com base nessa estrutura
                mtmsql.removerMensagensRepetidas();
                mtmsql.dividirMedicoes(); // remoçao dos que tem estrutura errada
                mtmsql.removerValoresDuplicados(); // remoçao dos duplicaods
                mtmsql.analisarTabelaSensor();
                mtmsql.removerValoresAnomalos(); // remoçao dos anomalos
                mtmsql.removerOutliers(); // remoçao dos outliers
                mtmsql.criarEMandarQueries();
                // sera que deva apagar???
                mtmsql.medicoes.clear();
                mtmsql.mensagensRecebidas.clear();
                Thread.sleep(sql_delay_to);
            }



        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

