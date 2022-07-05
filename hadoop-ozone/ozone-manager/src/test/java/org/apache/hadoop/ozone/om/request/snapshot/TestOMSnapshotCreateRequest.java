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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.snapshot;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.*;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmSnapshot;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.*;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .StorageTypeProto;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotCreateRequest class, which handles CreateSnapshot request.
 */
public class TestOMSnapshotCreateRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;

  private String volumeName, bucketName, name, mask;

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });

  @Before
  public void setup() throws Exception {

    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    name = UUID.randomUUID().toString();
    mask = volumeName + OM_KEY_PREFIX + bucketName;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager);

  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(ozoneManager.isAdmin((UserGroupInformation) any())).thenReturn(true);
    OMClientResponse omClientResponse = doValidateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
  }

  @Test
  public void testValidateNotOwner() throws Exception {
    OMClientResponse omClientResponse = doValidateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.PERMISSION_DENIED,
        omResponse.getStatus());
  }

  @Test
  public void testValidateInvalidMask() throws Exception {
    mask = "";
    OMClientResponse omClientResponse = doValidateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_SNAPSHOT_ERROR,
        omResponse.getStatus());
  }

  private OMClientResponse doValidateAndUpdateCache() throws Exception {
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(name, mask);

    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    
    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateSnapshotResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreateSnapshot,
        omResponse.getCmdType());
    return omClientResponse;
  }

  private OMSnapshotCreateRequest doPreExecute(String name,
      String mask) throws Exception {
    OMRequest originalRequest = OMRequestTestUtils.createSnapshotRequest(name, mask);
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        new OMSnapshotCreateRequest(originalRequest);

    OMRequest modifiedRequest = omSnapshotCreateRequest.preExecute(ozoneManager);
    return new OMSnapshotCreateRequest(modifiedRequest);
  }

}