package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

public class OmSnapshot implements Auditable {
  private String volume;
  private String bucket;
  private String path;
  private final String name;

  public OmSnapshot(String name, String mask) {
    String[] names = mask.split(OM_KEY_PREFIX);
    this.name = name;
    if (names.length > 0) {
      volume = names[0];
    }
    if (names.length > 1) {
      bucket = names[1];
    }

    if (names.length > 2) {
      path = String.join(OM_KEY_PREFIX,
          Arrays.copyOfRange(names, 2, names.length));
    }
  }
  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  public String getPath() {
    return path;
  }

  public String getBucketResourceName() {
    return bucket + OM_KEY_PREFIX + "snapshot";
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volume);
    auditMap.put(OzoneConsts.BUCKET, this.bucket);
    auditMap.put(OzoneConsts.OM_SNAPSHOT_PATH, this.path);
    auditMap.put(OzoneConsts.OM_SNAPSHOT_NAME, this.name);
    return auditMap;
  }
}

