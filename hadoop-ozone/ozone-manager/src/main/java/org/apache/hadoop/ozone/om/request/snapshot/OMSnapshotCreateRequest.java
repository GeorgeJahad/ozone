/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.snapshot;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;


/**
 * Handles CreateSnapshot Request.
 */
public class OMSnapshotCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotCreateRequest.class);

  private final String snapshotPath;
  private final String volumeName;
  private final String bucketName;
  private final String name;
  private final String dirName;

  public OMSnapshotCreateRequest(OMRequest omRequest) {
    super(omRequest);
    CreateSnapshotRequest createSnapshotRequest = omRequest
        .getCreateSnapshotRequest();
    snapshotPath = createSnapshotRequest.getSnapshotPath();
    String possibleName = createSnapshotRequest.getName();
    SnapshotInfo snapshotInfo =
        SnapshotInfo.newInstance(possibleName, snapshotPath);
    name = snapshotInfo.getName();
    volumeName = snapshotInfo.getVolumeName();
    bucketName = snapshotInfo.getBucketName();
    dirName = snapshotInfo.getDirName();
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);
    //  For now only support bucket snapshots
    if (volumeName == null || bucketName == null || dirName != null) {
      LOG.debug("Bad snapshotPath: {}", snapshotPath);
      throw new OMException("Bad Snapshot path",
          OMException.ResultCodes.INVALID_SNAPSHOT_ERROR);
    }

    // Verify name
    OmUtils.validateSnapshotName(name);

    UserGroupInformation ugi = createUGI();
    String bucketOwner = ozoneManager.getOmMReader().getBucketOwner(volumeName, bucketName,
        IAccessAuthorizer.ACLType.READ, OzoneObj.ResourceType.BUCKET);
    if (!ozoneManager.isAdmin(ugi) &&
        !ozoneManager.isOwner(ugi, bucketOwner)) {
      throw new OMException(
          "Only bucket owners/admins can create snapshots",
          OMException.ResultCodes.PERMISSION_DENIED);
    }
    return omRequest;
  }
  
  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotCreates();

    boolean acquiredBucketLock = false, acquiredSnapshotLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    SnapshotInfo snapshotInfo = SnapshotInfo
        .newInstance(name, snapshotPath);
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    String key = SnapshotInfo.getTableKey(name, snapshotPath);
    try {
      // Lock bucket so it doesn't
      //  get deleted while creating snapshot
      acquiredBucketLock =
          omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK,
              volumeName, bucketName);

      acquiredSnapshotLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, snapshotInfo.getSnapshotLockResourceName());

      //Check if snapshot already exists
      if (omMetadataManager.getSnapshotInfoTable().isExist(key)) {
        LOG.debug("snapshot: {} already exists ", key);
        throw new OMException("Snapshot already exists", FILE_ALREADY_EXISTS);
      }

      omMetadataManager.getSnapshotInfoTable()
          .addCacheEntry(new CacheKey<>(key),
            new CacheValue<>(Optional.of(snapshotInfo), transactionLogIndex));

      omResponse.setCreateSnapshotResponse(
          CreateSnapshotResponse.newBuilder()
          .setSnapshotInfo(snapshotInfo.getProtobuf()));
      omClientResponse = new OMSnapshotCreateResponse(
          omResponse.build(), name, snapshotPath);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMSnapshotCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredSnapshotLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            snapshotInfo.getSnapshotLockResourceName());
      }
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_SNAPSHOT,
        snapshotInfo.toAuditMap(), exception, userInfo));
    
    if (exception == null) {
      LOG.info("created snapshot: name {} in snapshotPath: {}", name,
          snapshotPath);
    } else {
      omMetrics.incNumSnapshotCreateFails();
      LOG.error("Snapshot creation failed for name:{} in snapshotPath:{}",
          name, snapshotPath);
    }
    return omClientResponse;
  }
  
}