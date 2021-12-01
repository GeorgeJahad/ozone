package org.apache.hadoop.util;

import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class DummyTopology implements DNSToSwitchMapping {
  private Map<String, String> getCurrentRacks() {
    Map<String, String> currRacks = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      InetAddress addr = null;
      try {
        addr = InetAddress.getByName("datanode-" + i + ".datanode");
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      currRacks.put(addr.getHostAddress(), "/rack-" + i);
      try {
        addr = InetAddress.getByName("s3g-" + i + ".s3g");
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      currRacks.put(addr.getHostAddress(), "/rack-" + i);
    }
    return currRacks;
  }
  @Override
  public List<String> resolve(List<String> list) {
    List<String> results = new ArrayList<>(list.size());
    Map<String, String> currRacks = getCurrentRacks();
    for (String addr: list) {
      results.add(currRacks.getOrDefault(addr, NetworkTopology.DEFAULT_RACK));
    }
    return results;
  }

  @Override
  public void reloadCachedMappings() {

  }

  @Override
  public void reloadCachedMappings(List<String> list) {

  }
  public static void main(String[] args) {

    DummyTopology dt = new DummyTopology();
    List<String> datanodes = Arrays
        .asList("10.42.0.172", "10.42.0.178", "10.42.0.180", "10.42.0.182");
    List<String> results = dt.resolve(datanodes);
    String r = results.stream().collect(Collectors.joining());
    System.out.println("results are: " + r);
  }
}
