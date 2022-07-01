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

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotMask;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;


/**
 * Handles CreateSnapshot Request.
 */
public class OMSnapshotCreateRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotCreateRequest.class);

  public OMSnapshotCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumSnapshotCreates();
    boolean acquiredBucketLock = false, acquiredVolumeLock = false;
    Exception exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();



    // TODO update snapshot metadata table cache
    
    CreateSnapshotRequest createSnapshotRequest = getOmRequest()
        .getCreateSnapshotRequest();
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = null;
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    String maskString = createSnapshotRequest.getMask();
    String name = createSnapshotRequest.getName();
    SnapshotMask mask = new SnapshotMask(maskString);
    String volumeName = mask.getVolume();
    String bucketName = mask.getBucket();
    String path = mask.getPath();
    try {
    //  For now only support bucket snapshots
      if (volumeName == null || bucketName == null || path != null) {
        LOG.debug("Bad mask: {}", maskString);
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
      // acquire lock
      acquiredVolumeLock =
          omMetadataManager.getLock().acquireReadLock(VOLUME_LOCK, volumeName);
      acquiredBucketLock =
          omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK,
              volumeName, bucketName);

      // TODO Once the snapshot table code is ready:
      //  Check that the snapshot doesn't exist already/add to table cache
      //  Add audit log
      omResponse.setCreateSnapshotResponse(
          CreateSnapshotResponse.newBuilder().setMask(maskString).setName(name));
      omClientResponse = new OMSnapshotCreateResponse(omResponse.build(), name, maskString);
    } catch (Exception ex) {
      exception = ex;
      omClientResponse = new OMSnapshotCreateResponse(
          createErrorOMResponse(omResponse, (IOException) exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseReadLock(VOLUME_LOCK, volumeName);
      }

    }

    // return response.
    return omClientResponse;
  }
}