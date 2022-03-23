package org.pt.iscte;

import java.sql.Timestamp;
import org.bson.Document;

public class Medicao {
  private final int iDSensor;
  private final int iDZona;
  private final Timestamp hora;
  private final double leitura;

  public Medicao(Document medicao) {
    iDSensor = Integer.parseInt(medicao.get("sensor").toString());
    iDZona = Integer.parseInt(medicao.get("zona").toString());
    hora = Timestamp.valueOf(medicao.get("data").toString().split("T")[0] + " "
        + medicao.get("data").toString().split("T")[1].split("Z")[0]);
    leitura = Double.parseDouble(medicao.get("medicao").toString());
  }

  public int getIDSensor() {
    return iDSensor;
  }

  public int getIDZona() {
    return iDZona;
  }

  public Timestamp getHora() {
    return hora;
  }

  public double getLeitura() {
    return leitura;
  }
}