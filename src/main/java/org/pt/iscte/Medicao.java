package org.pt.iscte;
import java.sql.Timestamp;
import org.bson.Document;

public class Medicao {
  public final int iDSensor;
  public final int iDZona;
  public final Timestamp hora;
  public final double leitura;
  public Medicao(Document medicao){
    iDSensor = Integer.parseInt(medicao.get("sensor").toString());
    iDZona = Integer.parseInt(medicao.get("zona").toString());
    hora = Timestamp.valueOf(medicao.get("data").toString().split("T")[0] + " " + medicao.get("data").toString().split("T")[1].split("Z")[0]);
    leitura = Double.parseDouble(medicao.get("medicao").toString());
  }
}