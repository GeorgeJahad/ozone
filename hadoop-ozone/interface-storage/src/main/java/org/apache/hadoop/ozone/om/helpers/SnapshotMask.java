package org.apache.hadoop.ozone.om.helpers;

import java.util.Arrays;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

public class SnapshotMask {
  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  public String getPath() {
    return path;
  }

  private String volume;
  private String bucket;
  private String path;

  public SnapshotMask(String mask) {
    String[] names = mask.split(OM_KEY_PREFIX);
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
}

