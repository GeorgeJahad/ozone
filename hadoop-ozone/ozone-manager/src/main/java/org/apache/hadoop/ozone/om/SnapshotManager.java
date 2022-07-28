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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.*;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmBlockClient;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmContainerClient;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;
import static org.apache.hadoop.ozone.om.KeyManagerImpl.getRemoteUser;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OzoneManager.getS3Auth;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;


/**
 * This class is used to manage/create OM snapshots.
 */
public final class SnapshotManager {

  private static final Map<String, SnapshotManager> snapshotManagerCache = new HashMap<>();

  public static final Logger LOG =
      LoggerFactory.getLogger(SnapshotManager.class);

  private final OmMReader omMReader;
  // private so as not to bypass cache
  public SnapshotManager(KeyManager keyManager,
                          PrefixManager prefixManager,
                          OMMetadataManager omMetadataManager,
                          OzoneManager ozoneManager) {
    omMReader = new OmMReader(keyManager, prefixManager, omMetadataManager, ozoneManager, LOG);
  }

  /**
   * Creates snapshot checkpoint that corresponds with SnapshotInfo.
   * @param omMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createSnapshot(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();
    return store.getSnapshot(snapshotInfo.getCheckpointDirName());
  }

  // Create the snapshot manager by finding the corresponding RocksDB instance,
  //  creating an OmMetadataManagerImpl instance based on that
  //  and creating the other manager instances based on that metadataManager
  public static synchronized SnapshotManager createSnapshotManager(OzoneManager ozoneManager, String snapshotName){
    if (snapshotName == null || snapshotName.isEmpty()) {
      return null;
    }
    OmMetadataManagerImpl smm = null;
    if (snapshotManagerCache.containsKey(snapshotName)) {
      return snapshotManagerCache.get(snapshotName);
    }
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    try {
      smm = OmMetadataManagerImpl.createSnapshotMetadataManager(conf, "testgbj-bucket1_" + snapshotName);
    } catch (IOException e) {
      // handle this
      e.printStackTrace();
    }
    PrefixManagerImpl pm = new PrefixManagerImpl(smm, false);
    KeyManagerImpl km = new KeyManagerImpl(null, ozoneManager.getScmClient(), smm, conf, null,
        ozoneManager.getBlockTokenSecretManager(), ozoneManager.getKmsProvider(), pm );
    SnapshotManager sm = new SnapshotManager(km, pm, smm, ozoneManager);
    snapshotManagerCache.put(snapshotName, sm);
    return sm;
  }

  // Get SnapshotManager based on keyname
  public static SnapshotManager getSnapshotManager(OzoneManager ozoneManager,  String keyname) {
    SnapshotManager sm = null;
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 2) &&keyParts[0].compareTo(".snapshot") == 0) {
      sm = SnapshotManager.createSnapshotManager(ozoneManager, keyParts[1]);
    }
    return sm;
  }

  // Remove snapshot indicator from keyname
  public static String fixKeyname(String keyname) {
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 2) && (keyParts[0].compareTo(".snapshot") == 0)) {
      return String.join("/", Arrays.copyOfRange(keyParts, 2, keyParts.length));
    }
    return keyname;
  }

  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
//    LOG.info("gbjLookupKey");
    return omMReader.lookupKey(args);
  }

  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
                                          String startKey, long numEntries)
      throws IOException {
    return listStatus(args, recursive, startKey, numEntries, false);
  }

  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    return omMReader.listStatus(args, recursive, startKey, numEntries, allowPartialPrefixes);
  }

  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    return omMReader.getFileStatus(args);
  }

  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    return omMReader.lookupFile(args);
  }

  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    return omMReader.listKeys(volumeName, bucketName, startKey, keyPrefix, maxKeys);
  }

  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return omMReader.getAcl(obj);
  }

  

}
