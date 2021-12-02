package org.apache.hadoop.ozone.s3;

import com.google.common.collect.Streams;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.DNSToSwitchMapping;
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

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;

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

  public void init() {
    if (!initializionDone) {
      start();
    }
    initializionDone = true;
  }
  public void start() {
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        ozoneConfiguration.getClass(
            DFSConfigKeysLegacy.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, ozoneConfiguration);
    try {
      ozoneClient.getObjectStore().createS3Bucket(S3_REGISTRY_BUCKET_NAME);
    } catch (IOException e) {
      LOG.info("S3 registry creation failed: " + e.getMessage());
    }
    try {
      registryBucket = ozoneClient.getObjectStore().getS3Bucket(S3_REGISTRY_BUCKET_NAME);
    } catch (IOException e) {
      LOG.error("S3 registry get failed: " + e.getMessage());
    }
    new Thread(this::updateRegistryTask).start();
  }
  public Map<String, List<String>> getRegistry() {
    Map<String, List<String>> registry;
    try {
      Iterator<? extends OzoneKey> ozoneKeyIterator;
      ozoneKeyIterator = registryBucket.listKeys(null);
      registry = Streams.stream(ozoneKeyIterator)
          .map(OzoneKey::getName)
          // group ip addresses by rack
          .collect(groupingBy(this::getRack));
    } catch (IOException e) {
      LOG.error("getRegistry failed: " + e.getMessage());
      return null;
    }
    return registry;


  }

  @NotNull
  private String getRack(String addr) {
    return dnsToSwitchMapping.resolve(Arrays.asList(addr)).get(0);
  }

  private void updateRegistryTask() {
    while (true) {
      createEntry();
      registry = getRegistry();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.error("registry task exiting.");
        break;
      }
    }
  }

  private void createEntry() {
    if (entryCreated) {
      return;
    }
    try {
      LOG.info("gbj addr: " + InetAddress.getLocalHost().getHostAddress());
      String[] addr = InetAddress.getLocalHost().getHostAddress().split("/");
      OzoneOutputStream output =
          registryBucket.createKey(addr[addr.length - 1], 0);
      output.close();
      entryCreated = true;
    } catch (IOException e) {
      LOG.info("S3 registry entry creation failed: " + e.getMessage());
    }
  }
}
