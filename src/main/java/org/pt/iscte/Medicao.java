package org.pt.iscte;

import java.sql.Timestamp;
import org.bson.Document;

public class Medicao {
  private int iDSensor;
  private int iDZona;
  private String tipoSensor;
  private Timestamp hora;
  private double leitura;
  private int migrado;

  public Medicao(Document medicao) {
    iDSensor = Integer.parseInt(medicao.get("sensor").toString());
    iDZona = Integer.parseInt(medicao.get("zona").toString());
    tipoSensor = medicao.get("tipo").toString();
    hora = Timestamp.valueOf(medicao.get("data").toString().split("T")[0] + " "
        + medicao.get("data").toString().split("T")[1].split("Z")[0]);
    leitura = Double.parseDouble(medicao.get("medicao").toString());
    migrado = Integer.parseInt(medicao.get("migrado").toString());
  }

  public void setMedicao(Medicao m) {
    iDSensor = m.getIDSensor();
    iDZona = m.getIDZona();
    tipoSensor = m.getTipoSensor();
    hora = m.getHora();
    leitura = m.getLeitura();
    migrado = m.getMigrado();
  }

  public int getIDSensor() {
    return iDSensor;
  }

  public int getIDZona() {
    return iDZona;
  }

  public String getTipoSensor() {
    return tipoSensor;
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
    return "sensor: " + iDSensor +
        ", zona: " + iDZona +
        ", hora: " + hora +
        ", leitura: " + leitura +
        ", migrado: " + migrado;
  }
}