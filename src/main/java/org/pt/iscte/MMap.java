package org.pt.iscte;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MMap {

    private final Map<String, ArrayList<Medicao>> medicoes = new HashMap<>();

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

    public ArrayList<Medicao> getValuesAsArray(char sensorType) {
        if (containsSensorType(sensorType))
            return joinLists(medicoes.get(sensorType + "1"), medicoes.get(sensorType + "2"));
        throw new RuntimeException();
    }

    private ArrayList<Medicao> joinLists(ArrayList<Medicao> list1, ArrayList<Medicao> list2) {
        list1.addAll(list2);
        return list1;
    }

    public boolean containsSensorType(char sensorType) {
        for (String s : medicoes.keySet())
            if (s.charAt(0) == sensorType)
                return true;
        return false;
    }
}
