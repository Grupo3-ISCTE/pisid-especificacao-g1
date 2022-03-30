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

//TODO: Temos que ver como se criam as replicas do mongo

public class SegundoJava {

  private Ini ini;
  private Connection sql_connection_from;

  public SegundoJava(Ini ini) {
    this.ini = ini;
  }

  /**
   * * Conecção ao MySQL Local para posterior análise das tabelas Medicao e
   * * ParametroCultura
   * 
   * @throws SQLException
   */
  public void connectToMySql() throws SQLException {
    System.out.println("Connecting to Local MySQL Database");
    Connection sql_connection_to = DriverManager.getConnection(
        ini.get("Mysql Destination", "sql_database_connection_to"),
        ini.get("Mysql Destination", "sql_database_user_to"), ini.get("Mysql Destination", "sql_database_password_to"));
    System.out.println("Connected to Local MySQL Database");
  }

  /**
   * * Leitura das medições presentes na tabela Medicao para posterir análise de
   * * futuros alertas
   * TODO: Falta guardar registos numa lista
   * 
   * @throws SQLException
   */
  public void lerTabelaMedicao() throws SQLException {
    Statement statement = sql_connection_from.createStatement();
    ResultSet resultSet = statement.executeQuery("medicao");
    System.out.println(resultSet.toString());
  }

  /**
   * * Leitura dos parametros da cultura em questao mediante a medição a ser
   * * analisada
   * TODO: Este método ainda só tem um resultSet falta todo o resto
   * 
   * @throws SQLException
   */
  public void lerTabelaParametroCultura(Medicao medicao) throws SQLException {
    Statement statement = sql_connection_from.createStatement();
    ResultSet resultSet = statement.executeQuery("parametro_cultura");
  }

  /**
   * ? Como saber se a medicao que vamos analisar já nao foi analizado
   */
  public void criarRegistoAlerta() throws SQLException {
    new Thread(() -> {
      try {
        lerTabelaMedicao();
        // lerTabelaParametroCultura();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }).start();
  }

  public static void main(String[] args) throws InvalidFileFormatException, IOException, SQLException {
    Ini ini = new Ini(new File("src/main/java/org/pt/iscte/config.ini"));
    SegundoJava segundojava = new SegundoJava(ini);
    segundojava.connectToMySql();
    segundojava.criarRegistoAlerta();
  }
}
