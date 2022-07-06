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

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmSnapshot;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;


/**
 * Handles CreateSnapshot Request.
 */
public class OMSnapshotCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotCreateRequest.class);

  private final String mask;
  private final String name;
  private final String volumeName;
  private final String bucketName;
  private final String path;
  private final OmSnapshot snapshot;
  public OMSnapshotCreateRequest(OMRequest omRequest) {
    super(omRequest);
    CreateSnapshotRequest createSnapshotRequest = omRequest
        .getCreateSnapshotRequest();
    mask = createSnapshotRequest.getMask();
    name = createSnapshotRequest.getName();
    snapshot = new OmSnapshot(name, mask);
    volumeName = snapshot.getVolume();
    bucketName = snapshot.getBucket();
    path = snapshot.getPath();
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);
    //  For now only support bucket snapshots
    if (volumeName == null || bucketName == null || path != null) {
      LOG.debug("Bad mask: {}", mask);
      throw new OMException("Bad Snapshot path", OMException.ResultCodes.INVALID_SNAPSHOT_ERROR);
    }
    UserGroupInformation ugi = createUGI();
    String bucketOwner = ozoneManager.getBucketOwner(volumeName, bucketName);
    if (!ozoneManager.isAdmin(ugi) &&
        !ozoneManager.isOwner(ugi, bucketOwner)) {
      throw new OMException(
          "Only bucket owners/admins can create snapshots",
          OMException.ResultCodes.PERMISSION_DENIED);
    }
    // Verify name
    OmUtils.validateSnapshotName(name);
    return omRequest;
  }
  
  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotCreates();

    boolean acquiredBucketLock = false, acquiredSnapshotLock = false;
    Exception exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    try {
      // Need this to be sure the bucket doesn't get deleted while creating snapshot
      acquiredBucketLock =
          omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK,
              volumeName, bucketName);

      acquiredSnapshotLock =
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, snapshot.getBucketResourceName());

      // TODO Once the snapshot table code is ready:
      //  Check that the snapshot doesn't exist already/add to table cache
      omResponse.setCreateSnapshotResponse(
          CreateSnapshotResponse.newBuilder().setMask(mask).setName(name));
      omClientResponse = new OMSnapshotCreateResponse(omResponse.build(), name, mask);
    } catch (Exception ex) {
      exception = ex;
      omClientResponse = new OMSnapshotCreateResponse(
          createErrorOMResponse(omResponse, (IOException) exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredSnapshotLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            snapshot.getBucketResourceName());
      }
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_SNAPSHOT,
        snapshot.toAuditMap(), exception, userInfo));

    // return response.
    return omClientResponse;
  }
}