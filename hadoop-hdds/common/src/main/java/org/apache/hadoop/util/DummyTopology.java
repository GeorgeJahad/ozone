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
    for (int i = 0; i < 4; i++) {
      InetAddress addr;
      try {
        addr = InetAddress.getByName("datanode-" + i + ".datanode");
        currRacks.put(addr.getHostAddress(), "/rack-" + i);
      } catch (UnknownHostException e) {
        //e.printStackTrace();
      }
      try {
        addr = InetAddress.getByName("s3g-" + i + ".s3g");
        currRacks.put(addr.getHostAddress(), "/rack-" + i);
      } catch (UnknownHostException e) {
        //e.printStackTrace();
      }
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
