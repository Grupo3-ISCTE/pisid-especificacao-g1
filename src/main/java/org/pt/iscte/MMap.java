package org.pt.iscte;

import java.sql.SQLOutput;
import java.util.*;

public class MMap {

    private final Map<String, ArrayList<Medicao>> medicoes = new HashMap<>();

    //public final static char[] sensorTypes = {'T', 'H', 'L'};

    public MMap() {
        medicoes.put("T1", new ArrayList<>());
        medicoes.put("T2", new ArrayList<>());
        medicoes.put("H1", new ArrayList<>());
        medicoes.put("H2", new ArrayList<>());
        medicoes.put("L1", new ArrayList<>());
        medicoes.put("L2", new ArrayList<>());
    }

    public List<Medicao> get(String sensor) {
        return medicoes.get(sensor);
    }

    public void clear() {
        this.get("T1").clear();
        this.get("T2").clear();
        this.get("H1").clear();
        this.get("H2").clear();
        this.get("L1").clear();
        this.get("L2").clear();
    }

    public Collection<ArrayList<Medicao>> getValuesLists(){
        return medicoes.values();
    }

//    public ArrayList<Medicao> getValuesAsArray(char sensorType) {
//        if (containsSensorType(sensorType)) {
////            System.out.println(medicoes.get(sensorType + "1"));
////            System.out.println(medicoes.get(sensorType + "2"));
//            return joinLists(medicoes.get(sensorType + "1"), medicoes.get(sensorType + "2"));
//        }
//        throw new RuntimeException();
//    }

//    private ArrayList<Medicao> joinLists(ArrayList<Medicao> list1, ArrayList<Medicao> list2) {
//        list1.addAll(list2);
//        return list1;
//    }

    public boolean isEmpty() {
        if (this.get("T1").size() == 0 &&
                this.get("T2").size() == 0 &&
                this.get("H1").size() == 0 &&
                this.get("H2").size() == 0 &&
                this.get("L1").size() == 0 &&
                this.get("L2").size() == 0)
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "MMap{" +
                "T1: "+this.get("T1").toString()+
                "T2: "+this.get("T2").toString()+
                "H1: "+this.get("H1").toString()+
                "H2: "+this.get("H2").toString()+
                "L1: "+this.get("L1").toString()+
                "L2: "+this.get("L2").toString()+
                '}';
    }
}
