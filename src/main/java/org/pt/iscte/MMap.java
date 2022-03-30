package org.pt.iscte;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MMap {

  final private Map<String, ArrayList<Medicao>> medicoes = new HashMap<>();

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
    String[] listaSensores = new String[] { "T1", "T2", "H1", "H2", "L1", "L2" };
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
}
