package org.pt.iscte;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

public class SegundoJava {

  private Ini ini;
  private Connection sql_connection_from;
  private String sql_table_to;

  public SegundoJava(Ini ini) throws SQLException {
    // Leitura das medicoes inseridas na tabela Medicoes
    // Criar novo registo na tabela ErroSensor quando necessário tendo em conta a
    // tabela Sensor
    // Remocao de valores fora dos limites definidos na tabela Sensor
    // Criar novo registo na tabela Alerta quando necessario tendo em conta a tabela
    // ParametroCultura

    this.ini = ini;
    connectFromMySql();
    connectToMySql();
    new Thread(() -> {
      try {
        lerTabelaMedicao();
        verificarTabelaSensor();
        criarRegistoErroSensor();
        removerRegistoMedicao();
        lerTabelaParametroCultura();
        enviarAlertas();
        sql_connection_from.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }).start();
  }

  public void connectFromMySql() throws SQLException {
    String connection_link = ini.get("Mysql Origin", "sql_database_connection_from");
    String user = ini.get("Mysql Origin", "sql_database_user_from");
    String password = ini.get("Mysql Origin", "sql_database_password_from");
    sql_connection_from = DriverManager.getConnection(connection_link, user, password);
  }

  public void connectToMySql() throws SQLException {
    sql_table_to = ini.get("Mysql Destination", "sql_table_to");
    Connection sql_connection_to = DriverManager.getConnection(
        ini.get("Mysql Destination", "sql_database_connection_to"),
        ini.get("Mysql Destination", "sql_database_user_to"), ini.get("Mysql Destination", "sql_database_password_to"));
  }

  public void lerTabelaMedicao() throws SQLException {
    Statement statement = sql_connection_from.createStatement();
    ResultSet resultSet = statement.executeQuery(sql_table_to);

  }

  public void verificarTabelaSensor() throws SQLException {
    Statement statement = sql_connection_from.createStatement();
    ResultSet resultSet = statement.executeQuery(ini.get("Mysql Origin", "sql_select_from_table"));

    while (resultSet.next()) {
      System.out.println("idsensor: " + resultSet.getInt(1) + " tipo: " + resultSet.getString(2) + " limiteinferior: "
          + resultSet.getBigDecimal(3) + " limitesuperior: " + resultSet.getBigDecimal(4) + " idzona: "
          + resultSet.getInt(5));
    }
  }

  public void criarRegistoErroSensor() {

  }

  public void removerRegistoMedicao() {

  }

  public void lerTabelaParametroCultura() {

  }

  public void enviarAlertas() {

  }

  public static void main(String[] args) throws InvalidFileFormatException, IOException, SQLException {
    Ini ini = new Ini();
    SegundoJava segundojava = new SegundoJava(new Ini(new File("src/main/java/org/iscte/pt/config.ini")));
  }
}
