package org.pt.iscte;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;
import org.ini4j.Ini;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO: Temos que ver como se criam as replicas do mongo

public class SegundoJava {

  private final Ini ini;

  private String cloud_topic_to;
  private MqttClient cloud_client_to;

  private Connection sql_connection_from;
  private Connection sql_connection_to;

  String[] listaSensores = new String[] { "T1", "T2", "H1", "H2", "L1", "L2" };
  List<Document> mensagensRecebidas = new ArrayList<>();
  MMap medicoes = new MMap();
  Map<String, Double[]> limitesSensores = new HashMap<>();

  public SegundoJava(Ini ini) {
    this.ini = ini;
  }

  /**
   * * Conecção ao Broker para posterior leitura das medicoes
   */
  public void connectToMQTT() throws MqttException {
    cloud_topic_to = ini.get("Cloud Destination", "cloud_topic_to");
    cloud_client_to = new MqttClient(ini.get("Cloud Destination", "cloud_server_to"),
            ini.get("Cloud Destination", "cloud_client_to"));
    MqttConnectOptions cloud_options_to = new MqttConnectOptions();
    cloud_options_to.setAutomaticReconnect(true);
    cloud_options_to.setCleanSession(true);
    cloud_options_to.setConnectionTimeout(10);
    cloud_client_to.connect(cloud_options_to);
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
  public void mQTTToMySQL() {
    try {
      cloud_client_to.subscribe(cloud_topic_to, (topic, msg) -> {
        if (!msg.toString().equals("fim")) {
          mensagensRecebidas.add(stringToDocument(msg));
          //System.out.println(msg);
        } else {
          removerMensagensRepetidas();
          dividirMedicoes();
          removerValoresDuplicados();
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
    String m = new String(msg.getPayload()).split("Document")[1].replace("=", "\":\"").replace(", ",
        "\",\"");
    m = m.substring(1, m.length() - 1).replace("}", "\"}").replace("{", "{\"");
    // System.out.println(m);
    return Document
        .parse(m);
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
      //System.out.println(m);
      medicoes.get(m.getSensor()).add(m);
    }
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

  public void analisarTabelaSensor() throws SQLException {
    Statement statement = sql_connection_from.createStatement();
    ResultSet rs = statement.executeQuery(ini.get("Mysql Origin", "sql_select_from_table"));
    while (rs.next()) {
      limitesSensores.put(rs.getString(2) + rs.getInt(1),
          new Double[] { rs.getDouble(3), rs.getDouble(4) });
    }
  }

  /**
   * ! Devem ser enviados aqui alertas cinzentos
   */
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

  public static void main(String[] args) {
    try {
      SegundoJava segundojava = new SegundoJava(new Ini(new File("src/main/java/org/pt/iscte/config.ini")));
      segundojava.connectToMQTT();
      segundojava.connectFromMySql();
      segundojava.connectToMySql();
      segundojava.mQTTToMySQL();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
