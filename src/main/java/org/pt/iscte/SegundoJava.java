package org.pt.iscte;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

public class SegundoJava {

  private Ini ini;
  private Connection sql_connection_from;
  private String sql_table_to;

  public SegundoJava(Ini ini) {
    this.ini = ini;
  }

  // Leitura das medicoes inseridas na tabela Medicoes
  // Criar novo registo na tabela ErroSensor quando necessário tendo em conta a
  // tabela Sensor
  // Remocao de valores fora dos limites definidos na tabela Sensor
  // Criar novo registo na tabela Alerta quando necessario tendo em conta a tabela
  // ParametroCultura
  public void init() throws SQLException {
    // connectToMySql();
    connectFromMySql();

    new Thread(() -> {
      try {
        lerTabelaMedicao();
        // lerTabelaParametroCultura();
        // criarRegistoAlerta();
      } catch (SQLException e) {
        try {
          sql_connection_from.close();
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
        e.printStackTrace();
      }
    }).start();
  }

  /**
   * * Conecção ao MySQL Local para posterior análise, remoção e inserção
   * TODO: Necessário criar credenciais de acesso
   * 
   * @throws SQLException
   */
  public void connectToMySql() throws SQLException {
    System.out.println("Connecting to Local MySQL Database");
    sql_table_to = ini.get("Mysql Destination", "sql_table_to");
    Connection sql_connection_to = DriverManager.getConnection(
        ini.get("Mysql Destination", "sql_database_connection_to"),
        ini.get("Mysql Destination", "sql_database_user_to"), ini.get("Mysql Destination", "sql_database_password_to"));
    System.out.println("Connected to Local MySQL Database");
  }

  /**
   * * Conecção ao MySQL da Cloud para posterior análise da tabela Sensor
   * ? Para que vai ser utilizada a tabela Zona?
   * 
   * @throws SQLException
   */
  public void connectFromMySql() throws SQLException {
    System.out.println("Connecting to External MySQL Database");
    String connection_link = ini.get("Mysql Origin", "sql_database_connection_from");
    String user = ini.get("Mysql Origin", "sql_database_user_from");
    String password = ini.get("Mysql Origin", "sql_database_password_from");
    sql_connection_from = DriverManager.getConnection(connection_link, user, password);
    System.out.println("Connected to External MySQL Database");
  }

  /**
   * * Leitura das medições presentes na tabela Medicao para posterior análise de
   * * valores anómalos
   * TODO: Falta guardar registos numa lista
   * 
   * @throws SQLException
   */
  public void lerTabelaMedicao() throws SQLException {
    Statement statement = sql_connection_from.createStatement();
    ResultSet resultSet = statement.executeQuery(sql_table_to);
  }

  public void lerTabelaParametroCultura() {

  }

  /**
   * ? Como saber se o registo que vamos analisar já nao foi analizado
   */
  public void criarRegistoAlerta() {

  }

  public static void main(String[] args) throws InvalidFileFormatException, IOException, SQLException {
    Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
    SegundoJava segundojava = new SegundoJava(ini);
    segundojava.init();
  }
}
