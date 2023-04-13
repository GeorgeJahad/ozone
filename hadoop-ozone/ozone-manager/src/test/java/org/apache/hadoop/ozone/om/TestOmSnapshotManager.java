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

package org.apache.hadoop.ozone.om;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_CANDIDATE_DIR;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.getINode;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test om snapshot manager.
 */
public class TestOmSnapshotManager {

  private OzoneManager om;
  private File testDir;
  private static final String CANDIDATE_DIR_NAME = OM_DB_NAME + SNAPSHOT_CANDIDATE_DIR;
  @Before
  public void init() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    testDir = GenericTestUtils.getRandomizedTestDir();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.toString());

    // Only allow one entry in cache so each new one causes an eviction
    configuration.setInt(
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE, 1);

    OmTestManagers omTestManagers = new OmTestManagers(configuration);
    om = omTestManagers.getOzoneManager();
  }

  @After
  public void cleanup() throws Exception {
    om.stop();
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void testCloseOnEviction() throws IOException {

    // set up db table
    SnapshotInfo first = createSnapshotInfo();
    SnapshotInfo second = createSnapshotInfo();
    Table<String, SnapshotInfo> snapshotInfoTable = mock(Table.class);
    when(snapshotInfoTable.get(first.getTableKey())).thenReturn(first);
    when(snapshotInfoTable.get(second.getTableKey())).thenReturn(second);
    HddsWhiteboxTestUtils.setInternalState(
        om.getMetadataManager(), "snapshotInfoTable", snapshotInfoTable);

    // create the first snapshot checkpoint
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        first);

    // retrieve it and setup store mock
    OmSnapshotManager omSnapshotManager = om.getOmSnapshotManager();
    OmSnapshot firstSnapshot = (OmSnapshot) omSnapshotManager
        .checkForSnapshot(first.getVolumeName(),
        first.getBucketName(), getSnapshotPrefix(first.getName()));
    DBStore firstSnapshotStore = mock(DBStore.class);
    HddsWhiteboxTestUtils.setInternalState(
        firstSnapshot.getMetadataManager(), "store", firstSnapshotStore);

    // create second snapshot checkpoint (which will be used for eviction)
    OmSnapshotManager.createOmSnapshotCheckpoint(om.getMetadataManager(),
        second);

    // confirm store not yet closed
    verify(firstSnapshotStore, times(0)).close();

    // read in second snapshot to evict first
    omSnapshotManager
        .checkForSnapshot(second.getVolumeName(),
        second.getBucketName(), getSnapshotPrefix(second.getName()));

    // confirm store was closed
    verify(firstSnapshotStore, timeout(3000).times(1)).close();
  }

  @Test
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH"})
  public void testHardLinkCreation() throws IOException {
    byte[] dummyData = {0};

    // Create dummy files to be linked to.
    File candidateDir = new File(testDir.toString(),
        CANDIDATE_DIR_NAME);
    candidateDir.mkdirs();
    Files.write(Paths.get(candidateDir.toString(), "f1"), dummyData);

    File snapDir1 = new File(candidateDir.toString(),
        OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "dir1");
    if (!snapDir1.mkdirs()) {
      throw new IOException("failed to make directory: " + snapDir1);
    }
    Files.write(Paths.get(snapDir1.toString(), "s1"), dummyData);

    File snapDir2 = new File(candidateDir.toString(),
        OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX + "dir2");
    if (!snapDir2.mkdirs()) {
      throw new IOException("failed to make directory: " + snapDir2);
    }

    // Create map of links to dummy files.
    File checkpointDir1 = new File(testDir.toString(),
        OM_CHECKPOINT_DIR + OM_KEY_PREFIX + "dir1");
    Map<Path, Path> hardLinkFiles = new HashMap<>();
    hardLinkFiles.put(fixPath(snapDir2.toString(), "f1"),
        fixPath(checkpointDir1.toString(), "f1"));
    hardLinkFiles.put(fixPath(snapDir2.toString(), "s1"),
        fixPath(snapDir1.toString(), "s1"));

    // Create link list.
    Path hardLinkList =
        OmSnapshotUtils.createHardLinkList(
            testDir.toString().length() + 1, hardLinkFiles);
    Files.move(hardLinkList, Paths.get(candidateDir.toString(),
        OM_HARDLINK_FILE));

    // Create links from list.
    OmSnapshotUtils.createHardLinks(candidateDir.toPath());

    // Confirm expected links.
    for (Map.Entry<Path, Path> entry : hardLinkFiles.entrySet()) {
      Path key = restorePath(entry.getKey());
      Assert.assertTrue(key.toFile().exists());
      Path value = restorePath(entry.getValue());
      // Convert checkpoint path to om.db.
      if (value.toString().contains(OM_CHECKPOINT_DIR)) {
        value = Paths.get(candidateDir.toString(),
                          value.getFileName().toString());
      }
      Assert.assertEquals("link matches original file",
          getINode(key), getINode(value));
    }
  }

  private Path fixPath(String dirString, String fileName) {
    String fixedDirstring = dirString.replace(
        CANDIDATE_DIR_NAME + OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR,
        OM_SNAPSHOT_CHECKPOINT_DIR);
    return Paths.get(fixedDirstring, fileName);
  }

  private Path restorePath(Path p) {
    return Paths.get(p.toString().replace(
        OM_SNAPSHOT_CHECKPOINT_DIR,
        CANDIDATE_DIR_NAME + OM_KEY_PREFIX +
            OM_SNAPSHOT_CHECKPOINT_DIR));
  }


  private SnapshotInfo createSnapshotInfo() {
    String snapshotName = UUID.randomUUID().toString();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    return SnapshotInfo.newInstance(volumeName,
        bucketName,
        snapshotName,
        snapshotId);
  }

}
