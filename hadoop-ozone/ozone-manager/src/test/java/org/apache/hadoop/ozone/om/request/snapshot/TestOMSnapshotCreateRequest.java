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

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });

  private static UserGroupInformation user1 = UserGroupInformation
      .createUserForTesting("user1", new String[] {"test1"});


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
    InetAddress address = mock(InetAddress.class);
    when(address.getHostName()).thenReturn("dummy");
    when(address.getHostAddress()).thenReturn("dummy");
    InetSocketAddress rpcAddress = mock(InetSocketAddress.class);
    when(rpcAddress.getAddress()).thenReturn(address);
    when(ozoneManager.getOmRpcServerAddr()).thenReturn(rpcAddress);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    UserGroupInformation.setLoginUser(user1);
  }

  @After
  public void stop() {
    UserGroupInformation.reset();
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String name = UUID.randomUUID().toString();
    String mask = volumeName + OM_KEY_PREFIX + bucketName;

    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(name, mask);

    doValidateAndUpdateCache(name, mask,
        omSnapshotCreateRequest);

  }

  // @Test
  // public void testValidateAndUpdateCacheWithNoVolume() throws Exception {
  //   String volumeName = UUID.randomUUID().toString();
  //   String bucketName = UUID.randomUUID().toString();

  //   OMRequest originalRequest = OMRequestTestUtils.createBucketRequest(
  //       bucketName, volumeName, false, StorageTypeProto.SSD);

  //   OMSnapshotCreateRequest omSnapshotCreateRequest =
  //       new OMSnapshotCreateRequest(originalRequest);

  //   String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

  //   // As we have not still called validateAndUpdateCache, get() should
  //   // return null.

  //   Assert.assertNull(omMetadataManager.getBucketTable().get(bucketKey));

  //   OMClientResponse omClientResponse =
  //       omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
  //           ozoneManagerDoubleBufferHelper);

  //   OMResponse omResponse = omClientResponse.getOMResponse();
  //   Assert.assertNotNull(omResponse.getCreateSnapshotResponse());
  //   Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
  //       omResponse.getStatus());

  //   // As request is invalid bucket table should not have entry.
  //   Assert.assertNull(omMetadataManager.getBucketTable().get(bucketKey));
  // }

  // @Test
  // public void testValidateAndUpdateCacheWithBucketAlreadyExists()
  //     throws Exception {
  //   String volumeName = UUID.randomUUID().toString();
  //   String bucketName = UUID.randomUUID().toString();

  //   OMSnapshotCreateRequest omSnapshotCreateRequest =
  //       doPreExecute(volumeName, bucketName);

  //   doValidateAndUpdateCache(volumeName, bucketName,
  //       omSnapshotCreateRequest.getOmRequest());

  //   // Try create same bucket again
  //   OMClientResponse omClientResponse =
  //       omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 2,
  //           ozoneManagerDoubleBufferHelper);

  //   OMResponse omResponse = omClientResponse.getOMResponse();
  //   Assert.assertNotNull(omResponse.getCreateSnapshotResponse());
  //   Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_ALREADY_EXISTS,
  //       omResponse.getStatus());
  // }

  // @Test
  // public void testValidateAndUpdateCacheVerifyBucketLayout() throws Exception {
  //   String volumeName = UUID.randomUUID().toString();
  //   String bucketName = UUID.randomUUID().toString();
  //   String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

  //   OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(volumeName,
  //       bucketName);

  //   doValidateAndUpdateCache(volumeName, bucketName,
  //       omSnapshotCreateRequest.getOmRequest());

  //   Assert.assertEquals(BucketLayout.LEGACY,
  //       omMetadataManager.getBucketTable().get(bucketKey).getBucketLayout());
  // }

  private OMSnapshotCreateRequest doPreExecute(String name,
      String mask) throws Exception {
    OmSnapshot snapshot = new OmSnapshot(name, mask);
    String volumeName = snapshot.getVolume();
    String bucketName = snapshot.getBucket();
    createVolume(volumeName);
    createBucket(volumeName, bucketName);
    OMRequest originalRequest = OMRequestTestUtils.createSnapshotRequest(name, mask);
    // OzoneManagerProtocolProtos.UserInfo userInfo = OzoneManagerProtocolProtos.UserInfo.newBuilder().setUserName("test1").build();
    // omRequest.toBuilder().setUserInfo(userInfo).build();
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        new OMSnapshotCreateRequest(originalRequest);

    OMRequest modifiedRequest = omSnapshotCreateRequest.preExecute(ozoneManager);
    return new OMSnapshotCreateRequest(modifiedRequest);
  }

  private void doValidateAndUpdateCache(String name, String mask,
    OMSnapshotCreateRequest request) throws Exception {

    OMClientResponse omClientResponse =
        request.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    verifySuccessCreateSnapshotResponse(omClientResponse.getOMResponse());
  }

  public static void verifySuccessCreateSnapshotResponse(OMResponse omResponse) {
    Assert.assertNotNull(omResponse.getCreateSnapshotResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreateSnapshot,
        omResponse.getCmdType());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
  }

  public void createVolume(String volumeName) throws Exception {
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(UUID.randomUUID().toString())
            .setOwnerName(UUID.randomUUID().toString()).build();
    OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
  }

  public void createBucket(String volumeName, String bucketName)
    throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();

    OMRequestTestUtils.addBucketToOM(omMetadataManager, bucketInfo);
  }
}
