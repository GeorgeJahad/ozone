package org.apache.hadoop.ozone.om;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.TestOmSnapshotFileSystem.createKey;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static java.nio.charset.StandardCharsets.UTF_8;


@RunWith(Parameterized.class)
public class TestOmSnapshot {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static String volumeName;
  private static String bucketName;
  private static OzoneManagerProtocol writeClient;
  private static BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static boolean enabledFileSystemPaths;
  private static ObjectStore store;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmSnapshot.class);



  @Rule
  public Timeout timeout = new Timeout(1200000);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
                         new Object[]{OBJECT_STORE, false},
                         new Object[]{FILE_SYSTEM_OPTIMIZED, false},
                         new Object[]{BucketLayout.LEGACY, true});
  }

  public TestOmSnapshot(BucketLayout newBucketLayout, boolean newEnableFileSystemPaths)
      throws Exception {
    // Checking whether 'newBucketLayout' and 'newEnableFileSystemPaths' flags represents next
    // parameter index values. This is to ensure that initialize
    // init() function will be invoked only at the
    // beginning of every new set of Parameterized.Parameters.
    if (TestOmSnapshot.enabledFileSystemPaths != newEnableFileSystemPaths ||
            TestOmSnapshot.bucketLayout != newBucketLayout) {
      TestOmSnapshot.enabledFileSystemPaths = newEnableFileSystemPaths;
      TestOmSnapshot.bucketLayout = newBucketLayout;
      tearDown();
      init();
    }
  }

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  private void init() throws Exception {
      conf = new OzoneConfiguration();
      clusterId = UUID.randomUUID().toString();
      scmId = UUID.randomUUID().toString();
      omId = UUID.randomUUID().toString();
      conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, enabledFileSystemPaths);
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          bucketLayout.name());
      cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
          .setScmId(scmId).setOmId(omId).build();
      cluster.waitForClusterToBeReady();
      // create a volume and a bucket to be used by OzoneFileSystem
      OzoneBucket bucket = TestDataUtil
          .createVolumeAndBucket(cluster, bucketLayout);
      volumeName = bucket.getVolumeName();
      bucketName = bucket.getName();

      OzoneClient client = cluster.getClient();
      store = client.getObjectStore();
      writeClient = store.getClientProxy().getOzoneManagerClient();
      OzoneManager ozoneManager = cluster.getOzoneManager();
      KeyManagerImpl keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");

      // stop the deletion services so that keys can still be read
      keyManager.stop();


  }

  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testListKey()
      throws IOException, InterruptedException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    volB.createBucket(bucketA);
    volB.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);
    OzoneBucket volBbucketA = volB.getBucket(bucketA);
    OzoneBucket volBbucketB = volB.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      four.write(value);
      four.close();
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      four.write(value);
      four.close();
    }


    String snapshotPath = createSnapshot(volumeA, bucketA);

    Iterator<? extends OzoneKey> volABucketAIter =
        volAbucketA.listKeys(snapshotPath + "key-");
    int volABucketAKeyCount = 0;
    while (volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    Assert.assertEquals(20, volABucketAKeyCount);

    snapshotPath = createSnapshot(volumeA, bucketB);
    Iterator<? extends OzoneKey> volABucketBIter =
        volAbucketB.listKeys(snapshotPath + "key-");
    int volABucketBKeyCount = 0;
    while (volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    Assert.assertEquals(20, volABucketBKeyCount);

    snapshotPath = createSnapshot(volumeB, bucketA);
    Iterator<? extends OzoneKey> volBBucketAIter =
        volBbucketA.listKeys(snapshotPath + "key-");
    int volBBucketAKeyCount = 0;
    while (volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    Assert.assertEquals(20, volBBucketAKeyCount);

    snapshotPath = createSnapshot(volumeB, bucketB);
    Iterator<? extends OzoneKey> volBBucketBIter =
        volBbucketB.listKeys(snapshotPath + "key-");
    int volBBucketBKeyCount = 0;
    while (volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    Assert.assertEquals(20, volBBucketBKeyCount);

    snapshotPath = createSnapshot(volumeA, bucketA);
    Iterator<? extends OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys(snapshotPath + "key-a-");
    int volABucketAKeyACount = 0;
    while (volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    Assert.assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys(snapshotPath + "key-b-");
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith(snapshotPath + "key-b-" + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
  }

  @Test
  public void testListKeyOnEmptyBucket()
      throws IOException, InterruptedException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    String snapshotPath = createSnapshot(volume, bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys(snapshotPath);
    while (keys.hasNext()) {
      fail();
    }
  }

  private OmKeyArgs genKeyArgs(String keyName) {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .setLocationInfoList(new ArrayList<>())
        .build();
  }

  @Test
  public void checkKey() throws Exception {
    OzoneVolume ozoneVolume = store.getVolume(volumeName);
    assertTrue(ozoneVolume.getName().equals(volumeName));
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertTrue(ozoneBucket.getName().equals(bucketName));

    String s = "testData";
    String key1 = "checkKey/key1";
    createKey(ozoneBucket, key1, s.length(), s.getBytes(
        StandardCharsets.UTF_8) );

    OmKeyInfo originalKeyInfo = writeClient.lookupKey(genKeyArgs(key1));
    
    String snapshotPath = createSnapshot();

    OmKeyArgs keyArgs = genKeyArgs(snapshotPath + key1);
    
    OmKeyInfo omKeyInfo = writeClient.lookupKey(keyArgs);
    assertEquals(omKeyInfo.getKeyName(), snapshotPath + key1);
    
    OmKeyInfo fileInfo = writeClient.lookupFile(keyArgs);
    assertEquals(fileInfo.getKeyName(), snapshotPath + key1);

    OzoneFileStatus ozoneFileStatus = writeClient.getFileStatus(keyArgs);
    assertEquals(ozoneFileStatus.getKeyInfo().getKeyName(), snapshotPath + key1);
  }

  private String createSnapshot() throws IOException, InterruptedException {
    return createSnapshot(volumeName, bucketName);
  }
  private String createSnapshot(String vname, String bname) throws IOException, InterruptedException {
    String snapshotName = UUID.randomUUID().toString();
    writeClient = store.getClientProxy().getOzoneManagerClient();
    writeClient.createSnapshot(vname, bname, snapshotName);
    String snapshotPath = ".snapshot/" + snapshotName + "/";
    // TODO search for snapshot dir instead of sleep?
    Thread.sleep(4000);
    return snapshotPath;

  }


}
