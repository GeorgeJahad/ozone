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
  public static synchronized OmSnapshot createOmSnapshot(OzoneManager ozoneManager, String volumeName, String bucketName, String snapshotName){
    if (snapshotName == null || snapshotName.isEmpty()) {
      return null;
    }
    String fullName = volumeName + "-" + bucketName + "_" + snapshotName;
    OmMetadataManagerImpl smm = null;
    if (snapshotManagerCache.containsKey(fullName)) {
      return snapshotManagerCache.get(fullName);
    }
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    try {
      smm = OmMetadataManagerImpl.createSnapshotMetadataManager(conf, fullName);
    } catch (IOException e) {
      // handle this
      e.printStackTrace();
    }
    PrefixManagerImpl pm = new PrefixManagerImpl(smm, false);
    KeyManagerImpl km = new KeyManagerImpl(null, ozoneManager.getScmClient(), smm, conf, null,
        ozoneManager.getBlockTokenSecretManager(), ozoneManager.getKmsProvider(), pm );
    OmSnapshot s = new OmSnapshot(km, pm, smm, ozoneManager);
    snapshotManagerCache.put(fullName, s);
    return s;
  }

  // Get OmSnapshot based on keyname
  public static IOmMReader checkForSnapshot(OzoneManager ozoneManager,  String volumeName, String bucketName, String keyname) {
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 1) &&keyParts[0].compareTo(".snapshot") == 0) {
      return createOmSnapshot(ozoneManager, volumeName, bucketName, keyParts[1]);
    } else {
      return ozoneManager;
    }
  }

  // Remove snapshot indicator from keyname
  public static String fixKeyName(String keyname) {
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 2) && (keyParts[0].compareTo(".snapshot") == 0)) {
      return String.join("/", Arrays.copyOfRange(keyParts, 2, keyParts.length));
    }
    return keyname;
  }
}
