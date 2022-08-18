/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.*;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.security.acl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.hadoop.hdds.utils.HAUtils.getScmContainerClient;


/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager {
  private static final Map<String, OmSnapshot> snapshotManagerCache = new HashMap<>();

  public static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);


  /**
   * Creates snapshot checkpoint that corresponds with SnapshotInfo.
   * @param omMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createOmSnapshotCheckpoint(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    return store.getSnapshot(snapshotInfo.getCheckpointDirName());
  }

  // Create the snapshot manager by finding the corresponding RocksDB instance,
  //  creating an OmMetadataManagerImpl instance based on that
  //  and creating the other manager instances based on that metadataManager
  public static synchronized OmSnapshot createOmSnapshot(OzoneManager ozoneManager, String volumeName, String bucketName, String snapshotName)
      throws IOException {
    if (snapshotName == null || snapshotName.isEmpty()) {
      return null;
    }
    String fullName = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);
    SnapshotInfo snapshotInfo;
    try {
      snapshotInfo = ozoneManager.getMetadataManager().getSnapshotInfoTable()
          .get(fullName);
    } catch (IOException e) {
      LOG.error("Snapshot {}: not found: {}", fullName, e);
      throw e;
    }
    if (snapshotInfo == null) {
      throw new FileNotFoundException(fullName + " does not exist");
    }
    OmMetadataManagerImpl smm = null;
    if (snapshotManagerCache.containsKey(fullName)) {
      return snapshotManagerCache.get(fullName);
    }
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    try {
      smm = OmMetadataManagerImpl.createSnapshotMetadataManager(conf, snapshotInfo.getCheckpointDirName());
    } catch (IOException e) {
      LOG.error("Failed to retrieve snapshot: {}, {}", fullName, e);
      throw e;
    }
    PrefixManagerImpl pm = new PrefixManagerImpl(smm, false);
    KeyManagerImpl km = new KeyManagerImpl(null, ozoneManager.getScmClient(), smm, conf, null,
        ozoneManager.getBlockTokenSecretManager(), ozoneManager.getKmsProvider(), pm );
    OmSnapshot s = new OmSnapshot(km, pm, smm, ozoneManager, volumeName, bucketName, snapshotName);
    snapshotManagerCache.put(fullName, s);
    return s;
  }

  // Get OmSnapshot based on keyname
  public static IOmMReader checkForSnapshot(OzoneManager ozoneManager,  String volumeName, String bucketName, String keyname)
      throws IOException {
    if (keyname == null) {
      return ozoneManager;
    }
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 1) &&keyParts[0].compareTo(".snapshot") == 0) {
      return createOmSnapshot(ozoneManager, volumeName, bucketName, keyParts[1]);
    } else {
      return ozoneManager;
    }
  }

  // Remove snapshot indicator from keyname
  public static String normalizeKeyName(String keyname) {
    if (keyname == null) {
      return null;
    }
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 1) && (keyParts[0].compareTo(".snapshot") == 0)) {
      String normalizedKeyName = String.join("/", Arrays.copyOfRange(keyParts, 2, keyParts.length));
      if (keyname.endsWith("/")) {
        normalizedKeyName = normalizedKeyName + "/";
      }
      // GBJ todo: Is this right?
      if (normalizedKeyName.equals("/")) {
          return "";
      }
      return normalizedKeyName;
    }
    return keyname;
  }

  // Restore snapshot indicator to keyanme
  public static String denormalizeKeyName(String keyname, String snapshotName) {
    if (keyname == null) {
      return null;
    }
    return ".snapshot/" + snapshotName + "/" + keyname;
  }
}
