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

package org.apache.hadoop.ozone.om.response.snapshot;

import java.io.File;
import java.util.UUID;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.SnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateSnapshotResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * This class tests OMSnapshotCreateResponse.
 */
public class TestOMSnapshotCreateResponse {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;
  private String path;
  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    path = folder.newFolder().getAbsolutePath();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        path);
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @After
  public void tearDown() {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String name = UUID.randomUUID().toString();
    String mask = volumeName + OM_KEY_PREFIX + bucketName;
    SnapshotInfo snapshotInfo = SnapshotInfo.newBuilder()
        .setSnapshotPath(mask)
        .setName(name).build();
    OMSnapshotCreateResponse omSnapshotCreateResponse =
        new OMSnapshotCreateResponse(OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateSnapshot)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateSnapshotResponse(
                CreateSnapshotResponse.newBuilder().setSnapshotInfo(snapshotInfo.getProtobuf())
                    .build()).build(), name, mask);

    omSnapshotCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    // Confirm snapshot directory was created
    String snapshotDir = path + "/db.snapshots/om.db"+
        SnapshotManager.getDirName (name, mask);
    Assert.assertTrue((new File(snapshotDir)).exists());
  }
 }
