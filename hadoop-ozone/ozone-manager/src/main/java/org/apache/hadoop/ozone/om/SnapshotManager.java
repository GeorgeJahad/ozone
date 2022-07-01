package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;

import java.io.IOException;

public class SnapshotManager {

  private static String getSnapshotDirName(String name, String mask) {
    return "-" + mask.replaceAll("/", "-") + "_" + name;
  }

  public static DBCheckpoint createSnapshot(OMMetadataManager omMetadataManager, String name, String mask)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    return store.getSnapshot(getSnapshotDirName(name, mask));
  }

}
