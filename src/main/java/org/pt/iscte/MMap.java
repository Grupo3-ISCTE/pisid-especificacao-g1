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

  public boolean isEmpty() {
    return (this.get("T1").isEmpty() &&
        this.get("T2").isEmpty() &&
        this.get("H1").isEmpty() &&
        this.get("H2").isEmpty() &&
        this.get("L1").isEmpty() &&
        this.get("L2").isEmpty());
  }

  public void clear() {
    this.get("T1").clear();
    this.get("T2").clear();
    this.get("H1").clear();
    this.get("H2").clear();
    this.get("L1").clear();
    this.get("L2").clear();
  }
}
