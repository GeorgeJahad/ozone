package org.apache.hadoop.util;

import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class DummyTopology implements DNSToSwitchMapping {
  private Map<InetAddress, String> getCurrentDatanodes() {
    Map<InetAddress, String> currDatanodes = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      InetAddress addr = null;
      try {
        addr = InetAddress.getByName("datanode-" + i + ".datanode");
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      currDatanodes.put(addr, "/rack-" + i);
    }
    return currDatanodes;
  }
  @Override
  public List<String> resolve(List<String> list) {
    List<String> results = new ArrayList(list.size());
    Map<InetAddress, String> currDatanodes = getCurrentDatanodes();
    for (String addr: list) {
      try {
        InetAddress inetAddress = InetAddress.getByName(addr);
        if (currDatanodes.containsKey(inetAddress)) {
          results.add(currDatanodes.get(inetAddress));
        } else {
          results.add(NetworkTopology.DEFAULT_RACK);
        }
      } catch (UnknownHostException e) {
        e.printStackTrace();
        results.add(NetworkTopology.DEFAULT_RACK);
      }
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
