
package org.pt.iscte;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;

import javax.print.Doc;

public class Medicao implements Comparable<Medicao> {
    private  String sensor;
    private  String zona;
    private  Timestamp hora;
    private  double leitura;
    private  int migrado;

    public Medicao(Document medicao) {
        try {
            sensor = medicao.get("Sensor").toString();
            zona = medicao.get("Zona").toString();
            hora = Timestamp.valueOf(medicao.get("Data").toString().split("T")[0] + " "
                    + medicao.get("Data").toString().split("T")[1].split("Z")[0]);
            leitura = Double.parseDouble(medicao.get("Medicao").toString());
            migrado = Integer.parseInt(medicao.get("Migrado").toString());
        } catch (Exception e) {
            // System.out.println("Medicao inválida");
        }
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


    public String toString() {
        return "Sensor: " + sensor +
//                ", Zona: " + zona +
//                ", Hora: " + hora +
                ", Leitura: " + leitura; //+
//                ", Migrado: " + migrado;
    }

    @Override
    public int compareTo(Medicao otherMedicao) {
        return Double.compare(getLeitura(), otherMedicao.getLeitura());
    }
}