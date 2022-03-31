package org.pt.iscte;

import java.sql.Timestamp;
import org.bson.Document;

public class Medicao {
  private String sensor;
  private String zona;
  private Timestamp hora;
  private double leitura;
  private int migrado;

  public Medicao(Document medicao) {
    sensor = medicao.get("Sensor").toString();
    zona = medicao.get("Zona").toString();
    hora = Timestamp.valueOf(medicao.get("Data").toString().split("T")[0] + " "
        + medicao.get("Data").toString().split("T")[1].split("Z")[0]);
    leitura = Double.parseDouble(medicao.get("Medicao").toString());
    migrado = Integer.parseInt(medicao.get("Migrado").toString());
  }

  public void setMedicao(Medicao m) {
    sensor = m.getSensor();
    zona = m.getZona();
    hora = m.getHora();
    leitura = m.getLeitura();
    migrado = m.getMigrado();
  }

  public String getSensor() {
    return sensor;
  }

  public String getZona() {
    return zona;
  }

  public Timestamp getHora() {
    return hora;
  }

  public double getLeitura() {
    return leitura;
  }

  public int getMigrado() {
    return migrado;
  }

  public String toString() {
    return "Sensor: " + sensor +
        ", Zona: " + zona +
        ", Hora: " + hora +
        ", Leitura: " + leitura +
        ", Migrado: " + migrado;
  }
}