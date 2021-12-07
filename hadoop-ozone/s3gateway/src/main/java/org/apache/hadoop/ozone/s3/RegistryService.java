package org.apache.hadoop.ozone.s3;

import com.google.common.collect.Streams;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.enterprise.inject.InjectionException;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Vetoed;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
@Vetoed
public class RegistryService {
  private DNSToSwitchMapping dnsToSwitchMapping;
  private static final String S3_REGISTRY_BUCKET_NAME = "s3-registry-bucket";
  private OzoneBucket registryBucket;
  private volatile Map<String, List<String>> registry;

  private static RegistryService singleton;
  private static boolean initializionDone;

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryService.class);

  private static OzoneConfiguration ozoneConfiguration;
  private static OzoneClient ozoneClient;
  private static boolean entryCreated;
  private RegistryService(){
    try {
      ozoneClient = OzoneClientFactory.getRpcClient(ozoneConfiguration);
    } catch (IOException e) {
      LOG.error("failed to create client: " + e.getMessage());
    }
  }

  public static RegistryService getRegistryService(
      OzoneConfiguration conf) {
    if (singleton == null) {
      ozoneConfiguration = conf;
      singleton = new RegistryService();
    }
    return singleton;
  }
  public static RegistryService getRegistryService() {
    if (singleton == null) {
      throw new InjectionException("Registry service does not exist yet.");
    } else {
      return singleton;
    }
  }

  public void start() {
    if (initializionDone) {
      return;
    }
    initializionDone = true;

    // set up rackawareness mapping
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        ozoneConfiguration.getClass(
            DFSConfigKeysLegacy.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, ozoneConfiguration);

    // create the registry bucket
    try {
      ozoneClient.getObjectStore().createS3Bucket(S3_REGISTRY_BUCKET_NAME);
    } catch (IOException e) {
      LOG.info("S3 registry creation failed: " + e.getMessage());
    }

    // Start updating the registry
    try {
      registryBucket = ozoneClient.getObjectStore().getS3Bucket(S3_REGISTRY_BUCKET_NAME);
    } catch (IOException e) {
      LOG.error("S3 registry get failed: " + e.getMessage());
    }
    new Thread(this::updateRegistryTask).start();
  }

  private Map<String, List<String>> updateRegistry() {
    Map<String, List<String>> registry;
    try {
      Iterator<? extends OzoneKey> ozoneKeyIterator;

      // read all the entries
      ozoneKeyIterator = registryBucket.listKeys(null);

      // convert into a map of s3g ip addresses per rack
      registry = Streams.stream(ozoneKeyIterator)
          .map(OzoneKey::getName)
          // group ip addresses by rack
          .collect(groupingBy(this::getRack));
    } catch (IOException e) {
      LOG.error("updateRegistry failed: " + e.getMessage());
      return null;
    }
    return registry;


  }

  @NotNull
  public String getRack(String addr) {
    return dnsToSwitchMapping.resolve(Arrays.asList(addr)).get(0);
  }

  private void updateRegistryTask() {
    while (true) {
      createEntry();
      registry = updateRegistry();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.error("registry task exiting.");
        break;
      }
    }
  }

  //  Create the key in the registry bucket
  private void createEntry() {
    if (entryCreated) {
      return;
    }
    try {
      String[] addr = InetAddress.getLocalHost().getHostAddress().split("/");
      OzoneOutputStream output =
          registryBucket.createKey(addr[addr.length - 1], 0);
      output.close();
      entryCreated = true;
    } catch (IOException e) {
      LOG.info("S3 registry entry creation failed: " + e.getMessage());
    }
  }

  // Get the list of s3g nodes on the rack, (or all if rack is null)
  public List<String> getS3GNodes(@Nullable String rack) {
    List<String> l = new ArrayList<>();
    if (rack != null) {
      return registry.getOrDefault(rack, l);
    }
    for (String k : registry.keySet()) {
      if (!k.equals(NetworkTopology.DEFAULT_RACK)) {
        for (String n : registry.get(k)) {
          l.add(n);
        }
      }
    }
    Collections.shuffle(l);
    return l;
  }

}
