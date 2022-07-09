package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

import java.io.IOException;

public class SnapshotManager {

  public static DBCheckpoint createSnapshot(OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    return store.getSnapshot(snapshotInfo.getCheckpointDirName());
  }

}
