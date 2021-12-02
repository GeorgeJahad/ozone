package org.apache.hadoop.ozone.s3;

import com.google.common.collect.Streams;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.endpoint.EndpointBase;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class RegistryService extends EndpointBase {
  private DNSToSwitchMapping dnsToSwitchMapping;
  private static final String S3_REGISTRY_BUCKET_NAME = "s3-registry-bucket";
  private String registryBucket;
  private volatile Map<String, List<String>> registry;

  private static RegistryService singleton;

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryService.class);



  @Inject
  private OzoneConfiguration ozoneConfiguration;

  //
  private RegistryService(){}

  public static RegistryService getRegistryService() {
    if (singleton == null) {
      singleton = new RegistryService();
    }
    return singleton;
  }

  public void init() {
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        ozoneConfiguration.getClass(
            DFSConfigKeysLegacy.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, ozoneConfiguration);
    this.dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));


    try {
      registryBucket = createS3Bucket(S3_REGISTRY_BUCKET_NAME);
    } catch (IOException | OS3Exception e) {
      LOG.error("S3 registry creation failed: " + e.getMessage());
    }
    try {
      LOG.info("gbj addr: " + InetAddress.getLocalHost().getHostAddress());
      String[] addr = InetAddress.getLocalHost().getHostAddress().split("/");
      OzoneBucket bucket = getBucket(S3_REGISTRY_BUCKET_NAME);
      OzoneOutputStream output = bucket.createKey(addr[addr.length - 1], 0);
      output.close();
    } catch (IOException | OS3Exception e) {
      LOG.error("S3 registry entry creation failed: " + e.getMessage());
    }
  }
  public Map<String, List<String>> getRegistry() {
    Map<String, List<String>> registry = new HashMap<>();
    OzoneBucket bucket = null;
    try {
      bucket = getBucket(S3_REGISTRY_BUCKET_NAME);
      Iterator<? extends OzoneKey> ozoneKeyIterator;
      ozoneKeyIterator = bucket.listKeys(null);
      registry = Streams.stream(ozoneKeyIterator)
          .map(OzoneKey::getName)
          .collect(
              groupingBy(k -> dnsToSwitchMapping.resolve(
                  Arrays.asList(k)).get(0)));
    } catch (OS3Exception | IOException e) {
      LOG.error("getRegistry failed: " + e.getMessage());
      return null;
    }
    return registry;


  }
  public void start() {
    new Thread(this::updateRegistryTask).start();
  }
  private void updateRegistryTask() {
    while (true) {
      registry = getRegistry();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.error("registry task exiting.");
        break;
      }
    }
  }



}
