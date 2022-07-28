/*
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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotInfoEntry;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotStatusProto;

import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.junit.Assert;

import java.util.UUID;

/**
 * Tests SnapshotInfo metadata data structure holding state info for
 * object storage snapshots.
 */
public class TestOmSnapshotInfo {

  private static final String SNAPSHOT_ID = UUID.randomUUID().toString();
  private static final String NAME = "snapshot1";
  private static final SnapshotStatus SNAPSHOT_STATUS =
      SnapshotStatus.SNAPSHOT_ACTIVE;
  private static final long CREATION_TIME = Time.now();
  private static final long DELETION_TIME = -1;
  private static final String PATH_PREVIOUS_SNAPSHOT_ID =
      UUID.randomUUID().toString();
  private static final String GLOBAL_PREVIOUS_SNAPSHOT_ID =
      PATH_PREVIOUS_SNAPSHOT_ID;
  private static final String SNAPSHOT_PATH = "test/path";
  private static final String CHECKPOINT_DIR = "checkpoint.testdir";

  private SnapshotInfo createSnapshotInfo() {
    return new SnapshotInfo.Builder()
        .setSnapshotID(SNAPSHOT_ID)
        .setName(NAME)
        .setSnapshotStatus(SNAPSHOT_STATUS)
        .setCreationTime(CREATION_TIME)
        .setDeletionTime(DELETION_TIME)
        .setPathPreviousSnapshotID(PATH_PREVIOUS_SNAPSHOT_ID)
        .setGlobalPreviousSnapshotID(GLOBAL_PREVIOUS_SNAPSHOT_ID)
        .setSnapshotPath(SNAPSHOT_PATH)
        .setCheckpointDir(CHECKPOINT_DIR)
        .build();
  }

  private SnapshotInfoEntry createSnapshotInfoProto() {
    return SnapshotInfoEntry.newBuilder()
        .setSnapshotID(SNAPSHOT_ID)
        .setName(NAME)
        .setSnapshotStatus(SnapshotStatusProto.SNAPSHOT_ACTIVE)
        .setCreationTime(CREATION_TIME)
        .setDeletionTime(DELETION_TIME)
        .setPathPreviousSnapshotID(PATH_PREVIOUS_SNAPSHOT_ID)
        .setGlobalPreviousSnapshotID(GLOBAL_PREVIOUS_SNAPSHOT_ID)
        .setSnapshotPath(SNAPSHOT_PATH)
        .setCheckpointDir(CHECKPOINT_DIR)
        .build();
  }

  @Test
  public void testSnapshotStatusProtoToObject() {
    SnapshotInfoEntry snapshotInfoEntry = createSnapshotInfoProto();
    Assert.assertEquals(SNAPSHOT_STATUS,
        SnapshotStatus.valueOf(snapshotInfoEntry.getSnapshotStatus()));
  }

  @Test
  public void testSnapshotInfoToProto() {
    SnapshotInfo snapshotInfo = createSnapshotInfo();
    SnapshotInfoEntry snapshotInfoEntryExpected = createSnapshotInfoProto();

    SnapshotInfoEntry snapshotInfoEntryActual = snapshotInfo.getProtobuf();
    Assert.assertEquals(snapshotInfoEntryExpected.getSnapshotID(),
        snapshotInfoEntryActual.getSnapshotID());
    Assert.assertEquals(snapshotInfoEntryExpected.getName(),
        snapshotInfoEntryActual.getName());
    Assert.assertEquals(snapshotInfoEntryExpected.getSnapshotStatus(),
        snapshotInfoEntryActual.getSnapshotStatus());

  }

  @Test
  public void testSnapshotInfoProtoToSnapshotInfo() {
    SnapshotInfo snapshotInfoExpected = createSnapshotInfo();
    SnapshotInfoEntry snapshotInfoEntry = createSnapshotInfoProto();

    SnapshotInfo snapshotInfoActual = SnapshotInfo
        .getFromProtobuf(snapshotInfoEntry);
    Assert.assertEquals(snapshotInfoExpected, snapshotInfoActual);
  }

  @Test
  public void testGenerateName() {
    // GMT: Sunday, July 10, 2022 7:56:55.001 PM
    long millis = 1657483015001L;
    String name = SnapshotInfo.generateName(millis);
    Assert.assertEquals("s20220710-195655.001", name);
  }
}