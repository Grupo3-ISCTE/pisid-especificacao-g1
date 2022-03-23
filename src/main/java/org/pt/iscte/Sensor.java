package org.pt.iscte;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Sensor {
  private final int iDSensor;
  private final String tipo;
  private final double limiteInferior;
  private final double limiteSuperior;
  private final int iDZona;

  public Sensor(ResultSet resultSet) throws SQLException {
    iDSensor = resultSet.getInt(1);
    tipo = resultSet.getString(2);
    limiteInferior = resultSet.getDouble(3);
    limiteSuperior = resultSet.getDouble(4);
    iDZona = resultSet.getInt(5);
  }

  public int getIDSensor() {
    return iDSensor;
  }

  public String getTipo() {
    return tipo;
  }

  public double getLimiteInferior() {
    return limiteInferior;
  }

  public double getLimiteSuperior() {
    return limiteSuperior;
  }

  public int getIDZona() {
    return iDZona;
  }

}
