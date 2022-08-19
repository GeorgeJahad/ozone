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

import com.amazonaws.services.directconnect.model.Loa;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
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
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.utils.HAUtils.getScmContainerClient;


/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager {
  private final OzoneManager ozoneManager;
  private final LoadingCache<String, OmSnapshot> snapshotCache;

  public static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);


  public static OmSnapshotManager getInstance(OzoneManager ozoneManager) {
    return new OmSnapshotManager(ozoneManager);
  }

  private OmSnapshotManager(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
    int cacheSize = ozoneManager.getConfiguration().getInt(
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE,
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT);

    CacheLoader<String, OmSnapshot> loader;
    loader = new CacheLoader<String, OmSnapshot>() {
      @Override
      // Create the snapshot manager by finding the corresponding RocksDB instance,
      //  creating an OmMetadataManagerImpl instance based on that
      //  and creating the other manager instances based on that metadataManager
      public OmSnapshot load(String snapshotTableKey) throws Exception {
        SnapshotInfo snapshotInfo;
        // see if the snapshot exists
        try {
          snapshotInfo = ozoneManager.getMetadataManager()
              .getSnapshotInfoTable()
              .get(snapshotTableKey);
        } catch (IOException e) {
          LOG.error("Snapshot {}: not found: {}", snapshotTableKey, e);
          throw e;
        }
        if (snapshotInfo == null) {
          throw new FileNotFoundException(snapshotTableKey + " does not exist");
        }

        // read in the snapshot
        OzoneConfiguration conf = ozoneManager.getConfiguration();
        OMMetadataManager snapshotMetadataManager;
        try {
          snapshotMetadataManager = OmMetadataManagerImpl.createSnapshotMetadataManager(
              conf, snapshotInfo.getCheckpointDirName());
        } catch (IOException e) {
          LOG.error("Failed to retrieve snapshot: {}, {}", snapshotTableKey, e);
          throw e;
        }

        // Create the metadata readers
        PrefixManagerImpl pm = new PrefixManagerImpl(snapshotMetadataManager, false);
        KeyManagerImpl km = new KeyManagerImpl(null,
            ozoneManager.getScmClient(), snapshotMetadataManager, conf, null,
            ozoneManager.getBlockTokenSecretManager(),
            ozoneManager.getKmsProvider(), pm );
        return new OmSnapshot(km, pm, snapshotMetadataManager, ozoneManager,
            snapshotInfo.getVolumeName(),
            snapshotInfo.getBucketName(),
            snapshotInfo.getName());
      }
    };

    // LRU
    snapshotCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize).build(loader);
    
  }
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

  // Get OmSnapshot if the keyname has the indicator
  public IOmMReader checkForSnapshot(String volumeName, String bucketName, String keyname)
      throws IOException {
    if (keyname == null) {
      return ozoneManager.getOmMReader();
    }
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 1) &&keyParts[0].compareTo(".snapshot") == 0) {
      return createOmSnapshot(volumeName, bucketName, keyParts[1]);
    } else {
      return ozoneManager.getOmMReader();
    }
  }

  private OmSnapshot createOmSnapshot(String volumeName, String bucketName, String snapshotName) {
    if (snapshotName == null || snapshotName.isEmpty()) {
      return null;
    }
    String snapshotTableKey = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);
    return snapshotCache.getUnchecked(snapshotTableKey);
  }

  
}
