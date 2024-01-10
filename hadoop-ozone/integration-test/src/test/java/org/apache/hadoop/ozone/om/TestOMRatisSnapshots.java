/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes.createKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the Ratis snapshots feature in OM.
 */
@Timeout(5000)
public class TestOMRatisSnapshots {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;
  private OzoneBucket ozoneBucket;
  private String volumeName;
  private String bucketName;

  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;
  // This test depends on direct RocksDB checks that are easier done with OBS
  // buckets.
  private static final BucketLayout TEST_BUCKET_LAYOUT =
      BucketLayout.OBJECT_STORE;
  private static final String SNAPSHOT_NAME_PREFIX = "snapshot";
  private OzoneClient client;

  /**
   * Create a MiniOzoneCluster for testing. The cluster initially has one
   * inactive OM. So at the start of the cluster, there will be 2 active and 1
   * inactive OM.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init(TestInfo testInfo) throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16,
        StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.
        OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    long snapshotThreshold = SNAPSHOT_THRESHOLD;
    // TODO: refactor tests to run under a new class with different configs.
    if (testInfo.getTestMethod().isPresent() &&
        testInfo.getTestMethod().get().getName()
            .equals("testInstallSnapshot")) {
      snapshotThreshold = SNAPSHOT_THRESHOLD * 10;
    }
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        snapshotThreshold);

    OzoneManagerRatisServerConfig omRatisConf =
        conf.getObject(OzoneManagerRatisServerConfig.class);
    omRatisConf.setLogAppenderWaitTimeMin(10);
    conf.setFromObject(omRatisConf);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfActiveOMs(2)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    objectStore = client.getObjectStore();

    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + RandomStringUtils.randomNumeric(5))
        .setAdmin("admin" + RandomStringUtils.randomNumeric(5))
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(TEST_BUCKET_LAYOUT).build());
    ozoneBucket = retVolumeinfo.getBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {100})
  // tried up to 1000 snapshots and this test works, but some of the
  //  timeouts have to be increased.
  public void testInstallSnapshot(int numSnapshotsToCreate, @TempDir Path tempDir) throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    List<Set<String>> sstSetList = new ArrayList<>();
    FaultInjector faultInjector =
        new SnapshotMaxSizeInjector(leaderOM,
            followerOM.getOmSnapshotProvider().getSnapshotDir(),
            sstSetList, tempDir);
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Create some snapshots, each with new keys
    int keyIncrement = 10;
    String snapshotNamePrefix = "snapshot";
    String snapshotName = "";
    List<String> keys = new ArrayList<>();
    SnapshotInfo snapshotInfo = null;
    for (int snapshotCount = 0; snapshotCount < numSnapshotsToCreate;
        snapshotCount++) {
      snapshotName = snapshotNamePrefix + snapshotCount;
      keys = writeKeys(keyIncrement);
      snapshotInfo = createOzoneSnapshot(leaderOM, snapshotName);
    }


    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 30_000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex);
    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm()).isGreaterThanOrEqualTo(leaderOMSnapshotTermIndex);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 30_000);

    assertLogCapture(logCapture,
        "Install Checkpoint is finished");

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    // TODO: Enable this part after RATIS-1481 used
    /*
    Assert.assertNotNull(followerOMMetaMngr.getKeyTable(
        TEST_BUCKET_LAYOUT).get(followerOMMetaMngr.getOzoneKey(
        volumeName, bucketName, newKeys.get(0))));
     */

    checkSnapshot(leaderOM, followerOM, snapshotName, keys, snapshotInfo);
    int sstFileCount = 0;
    Set<String> sstFileUnion = new HashSet<>();
    for (Set<String> sstFiles : sstSetList) {
      sstFileCount += sstFiles.size();
      sstFileUnion.addAll(sstFiles);
    }
    // Confirm that there were multiple tarballs.
    assertThat(sstSetList.size()).isGreaterThan(1);
    // Confirm that there was no overlap of sst files
    // between the individual tarballs.
    assertEquals(sstFileUnion.size(), sstFileCount);
  }

  private void checkSnapshot(OzoneManager leaderOM, OzoneManager followerOM,
                             String snapshotName,
                             List<String> keys, SnapshotInfo snapshotInfo)
      throws IOException {
    // Read back data from snapshot.
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(".snapshot/" + snapshotName + "/" +
            keys.get(keys.size() - 1)).build();
    OmKeyInfo omKeyInfo;
    omKeyInfo = followerOM.lookupKey(omKeyArgs);
    Assertions.assertNotNull(omKeyInfo);
    Assertions.assertEquals(omKeyInfo.getKeyName(), omKeyArgs.getKeyName());

    // Confirm followers snapshot hard links are as expected
    File followerMetaDir = OMStorage.getOmDbDir(followerOM.getConfiguration());
    Path followerActiveDir = Paths.get(followerMetaDir.toString(), OM_DB_NAME);
    Path followerSnapshotDir =
        Paths.get(getSnapshotPath(followerOM.getConfiguration(), snapshotInfo));
    File leaderMetaDir = OMStorage.getOmDbDir(leaderOM.getConfiguration());
    Path leaderActiveDir = Paths.get(leaderMetaDir.toString(), OM_DB_NAME);
    Path leaderSnapshotDir =
        Paths.get(getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo));
    // Get the list of hardlinks from the leader.  Then confirm those links
    //  are on the follower
    int hardLinkCount = 0;

    printNotExistingFiles(followerActiveDir, followerSnapshotDir, "follower", true);

    try (Stream<Path>list = Files.list(leaderSnapshotDir)) {
      for (Path leaderSnapshotSST: list.collect(Collectors.toList())) {
        String fileName = leaderSnapshotSST.getFileName().toString();
        if (fileName.toLowerCase().endsWith(".sst")) {

          Path leaderActiveSST =
              Paths.get(leaderActiveDir.toString(), fileName);
          // Skip if not hard link on the leader
          if (!leaderActiveSST.toFile().exists()) {
            continue;
          }
          // If it is a hard link on the leader, it should be a hard
          // link on the follower
          if (OmSnapshotUtils.getINode(leaderActiveSST)
              .equals(OmSnapshotUtils.getINode(leaderSnapshotSST))) {
            Path followerSnapshotSST =
                Paths.get(followerSnapshotDir.toString(), fileName);
            Path followerActiveSST =
                Paths.get(followerActiveDir.toString(), fileName);
            Assertions.assertEquals(
                OmSnapshotUtils.getINode(followerActiveSST),
                OmSnapshotUtils.getINode(followerSnapshotSST),
                "Snapshot sst file is supposed to be a hard link");
            hardLinkCount++;
          }
        }
      }
    }
    assertThat(hardLinkCount).withFailMessage("No hard links were found")
        .isGreaterThan(0);
  }

  public static void printNotExistingFiles(Path activeDir, Path snapshotDir, String rolePrefix, boolean print) {
    // START: Get directories
    System.out.println();
    System.out.println();

    List<Path> activeDirFiles = new LinkedList<>();
    try (Stream<Path> paths = Files.walk(activeDir)) {
      paths.filter(Files::isRegularFile)
          .forEach(f -> {
            activeDirFiles.add(f);
            if (print) {
              System.out.println("xbis: " + rolePrefix + " ActiveDir: " + f);
            }
          });
    } catch (IOException e) {
      System.out.println("xbis: e:" + e);
    }

    System.out.println();
    System.out.println();

    List<Path> snapshotDirFiles = new LinkedList<>();
    try (Stream<Path> paths = Files.walk(snapshotDir)) {
      paths.filter(Files::isRegularFile)
          .forEach(f -> {
            snapshotDirFiles.add(f);
            if (print) {
              System.out.println("xbis: " + rolePrefix + " SnapshotDir: " + f);
            }
          });
    } catch (IOException e) {
      System.out.println("xbis: e:" + e);
    }

    System.out.println();
    System.out.println();
    // END

    // Create an empty list.
    LinkedList<Path> extraHardLinks = new LinkedList<>();

    // Go over all paths in the snapshot dir.
    for (Path snapshotSST : snapshotDirFiles) {

      // For every file in the snapshot dir
      String fileName = snapshotSST.getFileName().toString();

      // If it's an sst file
      if (fileName.toLowerCase().endsWith(".sst")) {
        // Use the active dir path and the filename, to find if the file exists under the active dir.
        Path activeSST = Paths.get(activeDir.toString(), fileName);
        // If it doesn't exist under the active dir, add it to the list.
        if (!activeSST.toFile().exists()) {
//          System.out.println("xbis: sst: " +
//              "\nactiveSST exists: " + activeDirFiles.contains(activeSST) +
//              "\nactiveSST path: " + activeSST);
          extraHardLinks.add(activeSST);
        }
      }
    }

    if (extraHardLinks.size() > 0) {
      System.out.println("xbis: not existing hard links");
      for (Path path : extraHardLinks) {
        System.out.println("xbis: path: " + path);
      }
      System.out.println();
    } else {
      System.out.println("xbis: not existing hard links: [empty]");
    }
  }

  @Test
  @Timeout(300)
  public void testInstallIncrementalSnapshot(@TempDir Path tempDir)
      throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        80);

    SnapshotInfo snapshotInfo2 = createOzoneSnapshot(leaderOM, "snap80");
    followerOM.getConfiguration().setInt(
        OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
        KeyManagerImpl.DISABLE_VALUE);
    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 30_000);

    // Get two incremental tarballs, adding new keys/snapshot for each.
    IncrementData firstIncrement = getNextIncrementalTarball(160, 2, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);
    IncrementData secondIncrement = getNextIncrementalTarball(240, 3, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 30s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 1000, 30_000);

    assertEquals(3, followerOM.getOmSnapshotProvider().getNumDownloaded());
    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));

    for (String key : firstKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : firstIncrement.getKeys()) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    for (String key : secondIncrement.getKeys()) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify the metrics recording the incremental checkpoint at leader side
    DBCheckpointMetrics dbMetrics = leaderOM.getMetrics().
        getDBCheckpointMetrics();
    assertThat(dbMetrics.getLastCheckpointStreamingNumSSTExcluded()).isGreaterThan(0);
    assertEquals(2, dbMetrics.getNumIncrementalCheckpoints());

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 30_000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    GenericTestUtils.waitFor(() -> {
      try {
        return followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
            .get(followerOMMetaMngr.getOzoneKey(
                volumeName, bucketName, newKeys.get(0))) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 30_000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);

    checkSnapshot(leaderOM, followerOM, "snap80", firstKeys, snapshotInfo2);
    checkSnapshot(leaderOM, followerOM, "snap160", firstIncrement.getKeys(),
        firstIncrement.getSnapshotInfo());
    checkSnapshot(leaderOM, followerOM, "snap240", secondIncrement.getKeys(),
        secondIncrement.getSnapshotInfo());
    Assertions.assertEquals(
        followerOM.getOmSnapshotProvider().getInitCount(), 2,
        "Only initialized twice");
  }

  static class IncrementData {
    private List<String> keys;
    private SnapshotInfo snapshotInfo;
    public List<String> getKeys() {
      return keys;
    }
    public SnapshotInfo getSnapshotInfo() {
      return snapshotInfo;
    }
  }

  private IncrementData getNextIncrementalTarball(
      int numKeys, int expectedNumDownloads,
      OzoneManager leaderOM, OzoneManagerRatisServer leaderRatisServer,
      FaultInjector faultInjector, OzoneManager followerOM, Path tempDir)
      throws IOException, InterruptedException, TimeoutException {
    IncrementData id = new IncrementData();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    // Do some transactions, let leader OM take a new snapshot and purge the
    // old logs, so that follower must download the new increment.
    id.keys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        numKeys);

    id.snapshotInfo = createOzoneSnapshot(leaderOM, "snap" + numKeys);
    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the next install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() ->
        followerOM.getOmSnapshotProvider().getNumDownloaded() ==
        expectedNumDownloads, 1000, 30_000);

    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex())
        .isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // Now confirm tarball is just incremental and contains no unexpected
    //  files/links.
    Path increment = Paths.get(tempDir.toString(), "increment" + numKeys);
    assertTrue(increment.toFile().mkdirs());
    unTarLatestTarBall(followerOM, increment);
    List<String> sstFiles = HAUtils.getExistingSstFiles(increment.toFile());
    Path followerCandidatePath = followerOM.getOmSnapshotProvider().
        getCandidateDir().toPath();

    // Confirm that none of the files in the tarball match one in the
    // candidate dir.
    assertThat(sstFiles.size()).isGreaterThan(0);
    for (String s: sstFiles) {
      File sstFile = Paths.get(followerCandidatePath.toString(), s).toFile();
      assertFalse(sstFile.exists(),
          sstFile + " should not duplicate existing files");
    }

    // Confirm that none of the links in the tarballs hardLinkFile
    //  match the existing files
    Path hardLinkFile = Paths.get(increment.toString(), OM_HARDLINK_FILE);
    try (Stream<String> lines = Files.lines(hardLinkFile)) {
      int lineCount = 0;
      for (String line: lines.collect(Collectors.toList())) {
        lineCount++;
        String link = line.split("\t")[0];
        File linkFile = Paths.get(
            followerCandidatePath.toString(), link).toFile();
        assertFalse(linkFile.exists(),
            "Incremental checkpoint should not " +
                "duplicate existing links");
      }
      assertThat(lineCount).isGreaterThan(0);
    }
    return id;
  }

  @Test
  @Timeout(300)
  public void testInstallIncrementalSnapshotWithFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        80);

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 30_000);

    // Do some transactions, let leader OM take a new snapshot and purge the
    // old logs, so that follower must download the new snapshot again.
    List<String> secondKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        160);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the tarball install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 2;
    }, 1000, 30_000);

    // Corrupt the mixed checkpoint in the candidate DB dir
    File followerCandidateDir = followerOM.getOmSnapshotProvider().
        getCandidateDir();
    List<String> sstList = HAUtils.getExistingSstFiles(followerCandidateDir);
    assertThat(sstList.size()).isGreaterThan(0);
    Collections.shuffle(sstList);
    List<String> victimSstList = sstList.subList(0, sstList.size() / 3);
    for (String sst: victimSstList) {
      File victimSst = new File(followerCandidateDir, sst);
      Assertions.assertTrue(victimSst.delete());
    }

    // Resume the follower thread, it would download the full snapshot again
    // as the installation will fail for the corruption detected.
    faultInjector.resume();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 1000, 30_000);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    for (String key : firstKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : secondKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify the metrics
    /* HDDS-8876
    GenericTestUtils.waitFor(() -> {
      DBCheckpointMetrics dbMetrics =
          leaderOM.getMetrics().getDBCheckpointMetrics();
      return dbMetrics.getLastCheckpointStreamingNumSSTExcluded() == 0;
    }, 100, 30_000);

    GenericTestUtils.waitFor(() -> {
      DBCheckpointMetrics dbMetrics =
          leaderOM.getMetrics().getDBCheckpointMetrics();
      return dbMetrics.getNumIncrementalCheckpoints() >= 1;
    }, 100, 30_000);

    GenericTestUtils.waitFor(() -> {
      DBCheckpointMetrics dbMetrics =
          leaderOM.getMetrics().getDBCheckpointMetrics();
      return dbMetrics.getNumCheckpoints() >= 3;
    }, 100, 30_000);
    */

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 30_000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    GenericTestUtils.waitFor(() -> {
      try {
        return followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
            .get(followerOMMetaMngr.getOzoneKey(
                volumeName, bucketName, newKeys.get(0))) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 30_000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);
  }

  @Test
  public void testInstallSnapshotWithClientWrite() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // Continuously create new keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<List<String>> writeFuture = executor.submit(() -> {
      return writeKeys(200);
    });
    List<String> newKeys = writeFuture.get();

    // Wait checkpoint installation to finish
    Thread.sleep(5000);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 30_000);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);
    assertLogCapture(logCapture, "Install Checkpoint is finished");

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex);
    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm()).isGreaterThanOrEqualTo(leaderOMSnapshotTermIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMgr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMgr.getVolumeTable().get(
        followerOMMetaMgr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMgr.getBucketTable().get(
        followerOMMetaMgr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    OMMetadataManager leaderOmMetaMgr = leaderOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(leaderOmMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    Thread.sleep(5000);
    followerOMMetaMgr = followerOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(followerOMMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    // Read newly created keys
    readKeys(newKeys);
    System.out.println("All data are replicated");
  }

  @Test
  public void testInstallSnapshotWithClientRead() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    // Get transaction Index
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // Continuously read keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> readFuture = executor.submit(() -> {
      try {
        getKeys(keys, 10);
        readKeys(keys);
      } catch (IOException e) {
        assertTrue(Fail.fail("Read Key failed", e));
      }
      return null;
    });
    readFuture.get();

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 30_000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertThat(followerOMLastAppliedIndex).isGreaterThanOrEqualTo(leaderOMSnapshotIndex);
    assertThat(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm()).isGreaterThanOrEqualTo(leaderOMSnapshotTermIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Wait installation finish
    Thread.sleep(5000);
    // Verify checkpoint installation was happened.
    assertLogCapture(logCapture, "Reloaded OM state");
    assertLogCapture(logCapture, "Install Checkpoint is finished");
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM and start it
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    OzoneManagerRatisServer followerRatisServer = followerOM.getOmRatisServer();

    // Do some transactions so that the log index increases on follower OM
    writeKeysToIncreaseLogIndex(followerRatisServer, 100);

    TermIndex leaderCheckpointTermIndex = leaderOM.getOmRatisServer()
        .getLastAppliedTermIndex();
    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);

    // Do some more transactions to increase the log index further on
    // follower OM such that it is more than the checkpoint index taken on
    // leader OM.
    writeKeysToIncreaseLogIndex(followerOM.getOmRatisServer(),
        leaderCheckpointTermIndex.getIndex() + 100);

    // Install the old checkpoint on the follower OM. This should fail as the
    // followerOM is already ahead of that transactionLogIndex and the OM
    // state should be reloaded.
    TermIndex followerTermIndex = followerRatisServer.getLastAppliedTermIndex();
    TermIndex newTermIndex = followerOM.installCheckpoint(
        leaderOMNodeId, leaderDbCheckpoint);

    String errorMsg = "Cannot proceed with InstallSnapshot as OM is at " +
        "TermIndex " + followerTermIndex + " and checkpoint has lower " +
        "TermIndex";
    assertLogCapture(logCapture, errorMsg);
    assertNull(newTermIndex,
        "OM installed checkpoint even though checkpoint " +
            "logIndex is less than it's lastAppliedIndex");
    assertEquals(followerTermIndex,
        followerRatisServer.getLastAppliedTermIndex());
    String msg = "OM DB is not stopped. Started services with Term: " +
        followerTermIndex.getTerm() + " and Index: " +
        followerTermIndex.getIndex();
    assertLogCapture(logCapture, msg);
  }

  @Test
  public void testInstallCorruptedCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    TransactionInfo leaderCheckpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(conf, leaderCheckpointLocation);

    // Corrupt the leader checkpoint and install that on the OM. The
    // operation should fail and OM should shutdown.
    boolean delete = true;
    for (File file : leaderCheckpointLocation.toFile()
        .listFiles()) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          file.delete();
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);
    followerOM.setExitManagerForTesting(new DummyExitManager());
    // Install corrupted checkpoint
    followerOM.installCheckpoint(leaderOMNodeId, leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    // Wait checkpoint installation to be finished.
    assertLogCapture(logCapture, "System Exit: " +
        "Failed to reload OM state and instantiate services.");
    String msg = "RPC server is stopped";
    assertLogCapture(logCapture, msg);
  }

  private SnapshotInfo createOzoneSnapshot(OzoneManager leaderOM, String name)
      throws IOException {
    objectStore.createSnapshot(volumeName, bucketName, name);

    String tableKey = SnapshotInfo.getTableKey(volumeName,
                                               bucketName,
                                               name);
    SnapshotInfo snapshotInfo = leaderOM.getMetadataManager()
        .getSnapshotInfoTable()
        .get(tableKey);
    // Allow the snapshot to be written to disk
    String fileName =
        getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo);
    File snapshotDir = new File(fileName);
    if (!RDBCheckpointUtils
        .waitForCheckpointDirectoryExist(snapshotDir)) {
      throw new IOException("snapshot directory doesn't exist");
    }
    return snapshotInfo;
  }

  private List<String> writeKeysToIncreaseLogIndex(
      OzoneManagerRatisServer omRatisServer, long targetLogIndex)
      throws IOException, InterruptedException {
    List<String> keys = new ArrayList<>();
    long logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      keys.add(createKey(ozoneBucket));
      Thread.sleep(100);
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
    return keys;
  }

  private List<String> writeKeys(long keyCount) throws IOException {
    List<String> keys = new ArrayList<>();
    long index = 0;
    while (index < keyCount) {
      keys.add(createKey(ozoneBucket));
      index++;
    }
    return keys;
  }

  private void getKeys(List<String> keys, int round) throws IOException {
    while (round > 0) {
      for (String keyName : keys) {
        OzoneKeyDetails key = ozoneBucket.getKey(keyName);
        assertEquals(keyName, key.getName());
      }
      round--;
    }
  }

  private void readKeys(List<String> keys) throws IOException {
    for (String keyName : keys) {
      OzoneInputStream inputStream = ozoneBucket.readKey(keyName);
      byte[] data = new byte[100];
      inputStream.read(data, 0, 100);
      inputStream.close();
    }
  }

  private void assertLogCapture(GenericTestUtils.LogCapturer logCapture,
                              String msg)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      return logCapture.getOutput().contains(msg);
    }, 100, 30_000);
  }

  // Returns temp dir where tarball was untarred.
  private void unTarLatestTarBall(OzoneManager followerOm, Path tempDir)
      throws IOException {
    File snapshotDir = followerOm.getOmSnapshotProvider().getSnapshotDir();
    // Find the latest tarball.
    String tarBall = Arrays.stream(Objects.requireNonNull(snapshotDir.list())).
        filter(s -> s.toLowerCase().endsWith(".tar")).
        reduce("", (s1, s2) -> s1.compareToIgnoreCase(s2) > 0 ? s1 : s2);
    FileUtil.unTar(new File(snapshotDir, tarBall), tempDir.toFile());
  }

  private static class DummyExitManager extends ExitManager {
    @Override
    public void exitSystem(int status, String message, Throwable throwable,
        Logger log) {
      log.error("System Exit: " + message, throwable);
    }
  }

  private static class SnapshotPauseInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    SnapshotPauseInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() throws IOException {
      // Make sure injector pauses before resuming.
      try {
        ready.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        assertTrue(Fail.fail("resume interrupted"));
      }
      wait.countDown();
    }

    @Override
    public void reset() throws IOException {
      init();
    }
  }

  // Interrupts the tarball download process to test creation of
  // multiple tarballs as needed when the tarball size exceeds the
  // max.
  private static class SnapshotMaxSizeInjector extends FaultInjector {
    private final OzoneManager om;
    private int count;
    private final File snapshotDir;
    private final List<Set<String>> sstSetList;
    private final Path tempDir;
    SnapshotMaxSizeInjector(OzoneManager om, File snapshotDir,
                            List<Set<String>> sstSetList, Path tempDir) {
      this.om = om;
      this.snapshotDir = snapshotDir;
      this.sstSetList = sstSetList;
      this.tempDir = tempDir;
      init();
    }

    @Override
    public void init() {
    }

    @Override
    // Pause each time a tarball is received, to process it.
    public void pause() throws IOException {
      count++;
      File tarball = getTarball(snapshotDir);
      // First time through, get total size of sst files and reduce
      // max size config.  That way next time through, we get multiple
      // tarballs.
      if (count == 1) {
        long sstSize = getSizeOfSstFiles(tarball);
        om.getConfiguration().setLong(
            OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY, sstSize / 2);
        // Now empty the tarball to restart the download
        // process from the beginning.
        createEmptyTarball(tarball);
      } else {
        // Each time we get a new tarball add a set of
        // its sst file to the list, (i.e. one per tarball.)
        sstSetList.add(getSstFilenames(tarball));
      }
    }

    // Get Size of sstfiles in tarball.
    private long getSizeOfSstFiles(File tarball) throws IOException {
      FileUtil.unTar(tarball, tempDir.toFile());
      List<Path> sstPaths = Files.walk(tempDir).filter(
          path -> path.toString().endsWith(".sst")).
          collect(Collectors.toList());
      long sstSize = 0;
      for (Path sstPath : sstPaths) {
        sstSize += Files.size(sstPath);
      }
      return sstSize;
    }

    private void createEmptyTarball(File dummyTarFile)
        throws IOException {
      FileOutputStream fileOutputStream = new FileOutputStream(dummyTarFile);
      TarArchiveOutputStream archiveOutputStream =
          new TarArchiveOutputStream(fileOutputStream);
      archiveOutputStream.close();
    }

    // Return a list of sst files in tarball.
    private Set<String> getSstFilenames(File tarball)
        throws IOException {
      Set<String> sstFilenames = new HashSet<>();
      try (TarArchiveInputStream tarInput =
           new TarArchiveInputStream(new FileInputStream(tarball))) {
        TarArchiveEntry entry;
        while ((entry = tarInput.getNextTarEntry()) != null) {
          String name = entry.getName();
          if (name.toLowerCase().endsWith(".sst")) {
            sstFilenames.add(entry.getName());
          }
        }
      }
      return sstFilenames;
    }

    // Find the tarball in the dir.
    private File getTarball(File dir) {
      File[] fileList = dir.listFiles();
      assertNotNull(fileList);
      for (File f : fileList) {
        if (f.getName().toLowerCase().endsWith(".tar")) {
          return f;
        }
      }
      return null;
    }

    @Override
    public void resume() throws IOException {
    }

    @Override
    public void reset() throws IOException {
      init();
    }
  }
}
