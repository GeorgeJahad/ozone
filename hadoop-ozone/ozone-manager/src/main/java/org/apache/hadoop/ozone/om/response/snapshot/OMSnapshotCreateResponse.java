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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointManager;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.SnapshotManager;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Response for OMSnapshotCreateResponse.
 */
@CleanupTableInfo(cleanupTables = {})
public class OMSnapshotCreateResponse extends OMClientResponse {

  private String mask;
  private String name;
  @SuppressWarnings("checkstyle:parameternumber")
  public OMSnapshotCreateResponse(@Nonnull OMResponse omResponse,
    @Nonnull String name, @Nonnull String mask
  ) {
    super(omResponse);
    this.mask = mask;
    this.name = name;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMSnapshotCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    final Logger LOG =
        LoggerFactory.getLogger(OMSnapshotCreateResponse.class);
    SnapshotManager.createSnapshot(omMetadataManager, SnapshotInfo.newSnapshotInfo(name, mask));
    SnapshotInfo snapshotInfo =
      SnapshotInfo.getFromProtobuf(getOMResponse().getCreateSnapshotResponse().getSnapshotInfo());
    String key = SnapshotInfo.getKey(name, mask);
    omMetadataManager.getSnapshotInfoTable().putWithBatch(batchOperation,
        key, snapshotInfo);
  }
}
