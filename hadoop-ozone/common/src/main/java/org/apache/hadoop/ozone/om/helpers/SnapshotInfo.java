package org.apache.hadoop.ozone.om.helpers;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotInfoEntry;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotStatusProto;
import com.google.common.base.Preconditions;
import org.apache.hadoop.util.Time;

import java.time.*;
import java.time.chrono.ChronoLocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * This class is used for storing info related to Snapshots.
 *
 * Each snapshot created has an associated SnapshotInfo entry
 * containing the snapshotid, snapshot path,
 * snapshot checkpoint directory, previous snapshotid
 * for the snapshot path & global amongst other necessary fields.
 */
public final class SnapshotInfo implements Auditable {

  /**
   * SnapshotStatus enum composed of
   * active, deleted and reclaimed statues.
   */
  public enum SnapshotStatus {
    SNAPSHOT_ACTIVE,
    SNAPSHOT_DELETED,
    SNAPSHOT_RECLAIMED;

    public static final SnapshotStatus DEFAULT = SNAPSHOT_ACTIVE;

    public SnapshotStatusProto toProto() {
      switch (this) {
      case SNAPSHOT_ACTIVE:
        return SnapshotStatusProto.SNAPSHOT_ACTIVE;
      case SNAPSHOT_DELETED:
        return SnapshotStatusProto.SNAPSHOT_DELETED;
      case SNAPSHOT_RECLAIMED:
        return SnapshotStatusProto.SNAPSHOT_RECLAIMED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid SnapshotStatus, found status=" + this);
      }
    }

    public static SnapshotStatus valueOf(SnapshotStatusProto status) {
      switch (status) {
      case SNAPSHOT_ACTIVE:
        return SNAPSHOT_ACTIVE;
      case SNAPSHOT_DELETED:
        return SNAPSHOT_DELETED;
      case SNAPSHOT_RECLAIMED:
        return SNAPSHOT_RECLAIMED;
      default:
        throw new IllegalStateException(
            "BUG: missing valid SnapshotStatus, found status=" + status);
      }
    }
  }

  private final String snapshotID;  // UUID
  private String name;
  private SnapshotStatus snapshotStatus;
  private final long creationTime;
  private long deletionTime;
  private String pathPreviousSnapshotID;
  private String globalPreviousSnapshotID;
  private String snapshotPath; // snapshot mask
  private String checkpointDir;

  /**
   * Private constructor, constructed via builder.
   * @param snapshotID - Snapshot UUID.
   * @param name - snapshot name.
   * @param snapshotStatus - status: SNAPSHOT_ACTIVE, SNAPSHOT_DELETED,
   *                      SNAPSHOT_RECLAIMED
   * @param creationTime - Snapshot creation time.
   * @param deletionTime - Snapshot deletion time.
   * @param pathPreviousSnapshotID - Snapshot path previous snapshot id.
   * @param globalPreviousSnapshotID - Snapshot global previous snapshot id.
   * @param snapshotPath - Snapshot path, bucket .snapshot path.
   * @param checkpointDir - Snapshot checkpoint directory.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private SnapshotInfo(String snapshotID,
                       String name,
                       SnapshotStatus snapshotStatus,
                       long creationTime,
                       long deletionTime,
                       String pathPreviousSnapshotID,
                       String globalPreviousSnapshotID,
                       String snapshotPath,
                       String checkpointDir) {
    this.snapshotID = snapshotID;
    this.name = name;
    this.snapshotStatus = snapshotStatus;
    this.creationTime = creationTime;
    this.deletionTime = deletionTime;
    this.pathPreviousSnapshotID = pathPreviousSnapshotID;
    this.globalPreviousSnapshotID = globalPreviousSnapshotID;
    this.snapshotPath = snapshotPath;
    this.checkpointDir = checkpointDir;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setSnapshotStatus(SnapshotStatus snapshotStatus) {
    this.snapshotStatus = snapshotStatus;
  }

  public void setDeletionTime(long delTime) {
    this.deletionTime = delTime;
  }

  public void setPathPreviousSnapshotID(String pathPreviousSnapshotID) {
    this.pathPreviousSnapshotID = pathPreviousSnapshotID;
  }

  public void setGlobalPreviousSnapshotID(String globalPreviousSnapshotID) {
    this.globalPreviousSnapshotID = globalPreviousSnapshotID;
  }

  public void setSnapshotPath(String snapshotPath) {
    this.snapshotPath = snapshotPath;
  }

  public void setCheckpointDir(String checkpointDir) {
    this.checkpointDir = checkpointDir;
  }

  public String getSnapshotID() {
    return snapshotID;
  }

  public String getName() {
    return name;
  }

  public SnapshotStatus getSnapshotStatus() {
    return snapshotStatus;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getDeletionTime() {
    return deletionTime;
  }

  public String getPathPreviousSnapshotID() {
    return pathPreviousSnapshotID;
  }

  public String getGlobalPreviousSnapshotID() {
    return globalPreviousSnapshotID;
  }

  public String getSnapshotPath() {
    return snapshotPath;
  }

  public String getCheckpointDir() {
    return checkpointDir;
  }

  public static SnapshotInfo.Builder newBuilder() {
    return new SnapshotInfo.Builder();
  }

  public SnapshotInfo.Builder toBuilder() {
    return new SnapshotInfo.Builder()
        .setSnapshotID(snapshotID)
        .setSnapshotStatus(snapshotStatus)
        .setCreationTime(creationTime)
        .setDeletionTime(deletionTime)
        .setPathPreviousSnapshotID(pathPreviousSnapshotID)
        .setGlobalPreviousSnapshotID(globalPreviousSnapshotID)
        .setSnapshotPath(snapshotPath)
        .setCheckpointDir(checkpointDir);
  }

  /**
   * Builder of SnapshotInfo.
   */
  public static class Builder {
    private String snapshotID;
    private String name;
    private SnapshotStatus snapshotStatus;
    private long creationTime;
    private long deletionTime;
    private String pathPreviousSnapshotID;
    private String globalPreviousSnapshotID;
    private String snapshotPath;
    private String checkpointDir;

    public Builder() {
      // default values
      this.snapshotStatus = SnapshotStatus.DEFAULT;
    }

    public Builder setSnapshotID(String snapshotID) {
      this.snapshotID = snapshotID;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setSnapshotStatus(SnapshotStatus snapshotStatus) {
      this.snapshotStatus = snapshotStatus;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setDeletionTime(long delTime) {
      this.deletionTime = delTime;
      return this;
    }

    public Builder setPathPreviousSnapshotID(String pathPreviousSnapshotID) {
      this.pathPreviousSnapshotID = pathPreviousSnapshotID;
      return this;
    }

    public Builder setGlobalPreviousSnapshotID(
        String globalPreviousSnapshotID) {
      this.globalPreviousSnapshotID = globalPreviousSnapshotID;
      return this;
    }

    public Builder setSnapshotPath(String snapshotPath) {
      this.snapshotPath = snapshotPath;
      return this;
    }

    public Builder setCheckpointDir(String checkpointDir) {
      this.checkpointDir = checkpointDir;
      return this;
    }

    public SnapshotInfo build() {
      Preconditions.checkNotNull(name);
      return new SnapshotInfo(
          snapshotID,
          name,
          snapshotStatus,
          creationTime,
          deletionTime,
          pathPreviousSnapshotID,
          globalPreviousSnapshotID,
          snapshotPath,
          checkpointDir
      );
    }
  }

  /**
   * Creates SnapshotInfo protobuf from OmBucketInfo.
   */
  public SnapshotInfoEntry getProtobuf() {
    SnapshotInfoEntry.Builder sib = SnapshotInfoEntry.newBuilder()
        .setName(name)
        .setSnapshotStatus(snapshotStatus.toProto())
        .setSnapshotPath(snapshotPath)
        .setCreationTime(creationTime);
    /*    
        sib.setSnapshotID(snapshotID)
        sib.setDeletionTime(deletionTime)
        sib.setPathPreviousSnapshotID(pathPreviousSnapshotID)
        sib.setGlobalPreviousSnapshotID(globalPreviousSnapshotID)
        sib.setCheckpointDir(checkpointDir);
    */
    return sib.build();
  }

  /**
   * Parses SnapshotInfoEntry protobuf and creates SnapshotInfo.
   * @param snapshotInfoEntry
   * @return instance of SnapshotInfo
   */
  public static SnapshotInfo getFromProtobuf(
      SnapshotInfoEntry snapshotInfoEntry) {
    SnapshotInfo.Builder osib = SnapshotInfo.newBuilder()
        .setSnapshotID(snapshotInfoEntry.getSnapshotID())
        .setName(snapshotInfoEntry.getName())
        .setSnapshotStatus(SnapshotStatus.valueOf(snapshotInfoEntry
            .getSnapshotStatus()))
        .setCreationTime(snapshotInfoEntry.getCreationTime())
        .setDeletionTime(snapshotInfoEntry.getDeletionTime())
        .setPathPreviousSnapshotID(snapshotInfoEntry.
            getPathPreviousSnapshotID())
        .setGlobalPreviousSnapshotID(snapshotInfoEntry.
            getGlobalPreviousSnapshotID())
        .setSnapshotPath(snapshotInfoEntry.getSnapshotPath())
        .setCheckpointDir(snapshotInfoEntry.getCheckpointDir());

    return osib.build();
  }

  public String getVolumeName() {
    String volumeName = null;
    String[] names = snapshotPath.split(OM_KEY_PREFIX);
    if (names.length > 0) {
      volumeName = names[0];
    }
    return volumeName;
  }

  public String getBucketName() {
    String bucketName = null;
    String[] names = snapshotPath.split(OM_KEY_PREFIX);
    if (names.length > 1) {
      bucketName = names[1];
    }
    return bucketName;
  }

  // Snapshot on directories is not supported yet
  //  this is only used currently to confirm that snapshotPath doesn't
  //  contain a directory
  public String getDirName() {
    String dirName = null;
    String[] names = snapshotPath.split(OM_KEY_PREFIX);
    if (names.length > 2) {
      dirName = String.join(OM_KEY_PREFIX,
          Arrays.copyOfRange(names, 2, names.length));
    }
    return dirName;
  }

  public String getSnapshotLockResourceName() {
    return getBucketName() + OM_KEY_PREFIX + "snapshot";
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, getVolumeName());
    auditMap.put(OzoneConsts.BUCKET, getBucketName());
    auditMap.put(OzoneConsts.OM_SNAPSHOT_NAME, this.name);
    return auditMap;
  }


  public static String getCheckpointDirName(String name, String snapshotPath) {
    return "-" + getKey(name, snapshotPath);
  }

  public String getCheckpointDirName() {
    return getCheckpointDirName(name, snapshotPath);
  }
  public static String getKey(String name, String snapshotPath) {
    return snapshotPath.replaceAll(OM_KEY_PREFIX, "-") + "_" + name;
  }

  private static String generateName(long initialTime) {
    String timePattern = "yyyyMMdd-HHmmss.SSS";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
    Instant instant = Instant.ofEpochMilli(initialTime);
    return "s" + formatter.format(ZonedDateTime.ofInstant(instant,
        ZoneId.systemDefault()));
  }
  
  public static SnapshotInfo newSnapshotInfo(String name, String snapshotPath) {
    String id = UUID.randomUUID().toString();
    SnapshotInfo.Builder builder = new SnapshotInfo.Builder();
    long initialTime = Time.now();
    if (StringUtils.isBlank(name)) {
      name = generateName(initialTime); 
    }
    builder.setCreationTime(initialTime)
        .setName(name)
        .setSnapshotPath(snapshotPath)
        .setSnapshotID(id)
      .setCheckpointDir(getCheckpointDirName(name, snapshotPath));
    return builder.build();
  }

}
