package org.apache.hadoop.util;

import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

// Implements interface that resolves rack location of each node
// The topology creates 4 racks, /rack-0 to /rack-3
// The first 3 will contain two node, datanode-x, and s3g-x
// Since there are 4 s3g nodes and only 3 data nodes, the final
// rack only has s3g-3
public class RackawareS3PocTopology implements DNSToSwitchMapping {

  // resolves a list of ip addresses into a list of racks
  @Override
  public List<String> resolve(List<String> list) {
    List<String> results = new ArrayList<>(list.size());
    Map<String, String> currRacks = getCurrentRacks();
    for (String addr: list) {
      results.add(currRacks.getOrDefault(addr, NetworkTopology.DEFAULT_RACK));
    }
    return results;
  }

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
  public void reloadCachedMappings() {

  }

  @Override
  public void reloadCachedMappings(List<String> list) {

  }
}
