package org.pt.iscte;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MMap {

    final private Map<String, ArrayList<Medicao>> medicoes = new HashMap<>();

    final public static char[] sensorTypes = {'T', 'H', 'L'};

    public MMap() {
        medicoes.put("T1", new ArrayList<>());
        medicoes.put("T2", new ArrayList<>());
        medicoes.put("H1", new ArrayList<>());
        medicoes.put("H2", new ArrayList<>());
        medicoes.put("L1", new ArrayList<>());
        medicoes.put("L2", new ArrayList<>());
    }

    public ArrayList<Medicao> get(String sensor) {
        return medicoes.get(sensor);
    }

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

    public void clear() {
        this.get("T1").clear();
        this.get("T2").clear();
        this.get("H1").clear();
        this.get("H2").clear();
        this.get("L1").clear();
        this.get("L2").clear();
    }

    public void sort() {
        String[] listaSensores = new String[]{"T1", "T2", "H1", "H2", "L1", "L2"};
        for (String s : listaSensores) {
            Medicao temp;
            for (int i = 1; i < this.get(s).size(); i++) {
                for (int j = i; j > 0; j--) {
                    if (this.get(s).get(j).getLeitura() < this.get(s).get(j - 1).getLeitura()) {
                        temp = this.get(s).get(j);
                        this.get(s).get(j).setMedicao(this.get(s).get(j - 1));
                        this.get(s).get(j - 1).setMedicao(temp);
                    }
                }
            }
        }
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
