
package org.pt.iscte;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;

import org.bson.Document;

public class Record implements Comparable<Record> {
    private String sensor;
    private String zona;
    private Timestamp hora;
    private double leitura;
    private int migrado;

    public Record(Document medicao) {
        try {
            sensor = medicao.get("Sensor").toString();
            zona = medicao.get("Zona").toString();
            hora = Timestamp.valueOf(medicao.get("Data").toString().split("T")[0] + " "
                    + medicao.get("Data").toString().split("T")[1].split("Z")[0]);
            BigDecimal bd = new BigDecimal(Double.parseDouble(medicao.get("Medicao").toString())).setScale(2,
                    RoundingMode.HALF_UP);
            leitura = bd.doubleValue();
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
        // ", Zona: " + zona +
        // ", Hora: " + hora +
                ", Leitura: " + leitura; // +
        // ", Migrado: " + migrado;
    }

    @Override
    public int compareTo(Record otherRecord) {
        return Double.compare(getLeitura(), otherRecord.getLeitura());
    }
}