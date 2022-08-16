package org.apache.hadoop.ozone.om;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestOmSnapshot {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static String volumeName;
  private static String bucketName;
  private static FileSystem fs;
  private static OzoneManagerProtocol writeClient;
  private static BucketLayout bucketLayout;
  private static boolean enabledFileSystemPaths;

  @Rule
  public Timeout timeout = new Timeout(1200000);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
                         new Object[]{BucketLayout.FILE_SYSTEM_OPTIMIZED, true},
                         new Object[]{BucketLayout.LEGACY, true});
  }

  public TestOmSnapshot(BucketLayout newBucketLayout, boolean newEnableFileSystemPaths) {
    // Checking whether 'newBucketLayout' and 'newEnableFileSystemPaths' flags represents next
    // parameter index values. This is to ensure that initialize
    // init() function will be invoked only at the
    // beginning of every new set of Parameterized.Parameters.
    if (enabledFileSystemPaths != newEnableFileSystemPaths ||
            bucketLayout != newBucketLayout) {
      enabledFileSystemPaths = newEnableFileSystemPaths;
      bucketLayout = newBucketLayout;
      try {
        tearDown();
        init();
      } catch (Exception e) {
        fail("Unexpected exception:" + e.getMessage());
      }
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

      String rootPath = String
          .format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
              bucket.getVolumeName());
      // Set the fs.defaultFS and start the filesystem
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
      // Set the number of keys to be processed during batch operate.
      conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
      fs = FileSystem.get(conf);

  }

  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testListKeysAtDifferentLevels() throws Exception {
    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    String keyc1 = "/a/b1/c1/c1.tx";
    String keyc2 = "/a/b1/c2/c2.tx";

    String keyd13 = "/a/b2/d1/d11.tx";
    String keyd21 = "/a/b2/d2/d21.tx";
    String keyd22 = "/a/b2/d2/d22.tx";
    String keyd31 = "/a/b2/d3/d31.tx";

    String keye11 = "/a/b3/e1/e11.tx";
    String keye21 = "/a/b3/e2/e21.tx";
    String keye31 = "/a/b3/e3/e31.tx";

    LinkedList<String> keys = new LinkedList<>();
    keys.add(keyc1);
    keys.add(keyc2);

    keys.add(keyd13);
    keys.add(keyd21);
    keys.add(keyd22);
    keys.add(keyd31);

    keys.add(keye11);
    keys.add(keye21);
    keys.add(keye31);

    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte)96);

    createKeys(ozoneBucket, keys);

    writeClient = objectStore.getClientProxy().getOzoneManagerClient();

    writeClient.createSnapshot("snap1", volumeName + OM_KEY_PREFIX + bucketName);
    String snapshotPath = ".snapshot/snap1/";
    //String snapshotPath = "";
    // TODO search for dir instead of sleep?
    deleteRootDir();
    Thread.sleep(4000);
    // Root level listing keys
    Iterator<? extends OzoneKey> ozoneKeyIterator =
      ozoneBucket.listKeys(snapshotPath, null);
//    verifyFullTreeStructure(ozoneKeyIterator);

    ozoneKeyIterator =
        ozoneBucket.listKeys(snapshotPath + "a/", null);
    verifyFullTreeStructure(ozoneKeyIterator);

    LinkedList<String> expectedKeys;

    // Intermediate level keyPrefix - 2nd level
    ozoneKeyIterator =
        ozoneBucket.listKeys(snapshotPath + "a/b2", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/");
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d2/");
    expectedKeys.add("a/b2/d3/");
    expectedKeys.add("a/b2/d1/d11.tx");
    expectedKeys.add("a/b2/d2/d21.tx");
    expectedKeys.add("a/b2/d2/d22.tx");
    expectedKeys.add("a/b2/d3/d31.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Intermediate level keyPrefix - 3rd level
    ozoneKeyIterator =
        ozoneBucket.listKeys(snapshotPath + "a/b2/d1", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary of a level
    ozoneKeyIterator =
        ozoneBucket.listKeys(snapshotPath + "a/b2/d2", snapshotPath + "a/b2/d2/d21.tx");
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d2/d22.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary case - last node in the depth-first-traversal
    ozoneKeyIterator =
        ozoneBucket.listKeys(snapshotPath + "a/b3/e3", snapshotPath + "a/b3/e3/e31.tx");
    expectedKeys = new LinkedList<>();
    checkKeyList(ozoneKeyIterator, expectedKeys);
  }

  private void verifyFullTreeStructure(Iterator<? extends OzoneKey> keyItr) {
    LinkedList<String> expectedKeys = new LinkedList<>();
    expectedKeys.add("a/");
    expectedKeys.add("a/b1/");
    expectedKeys.add("a/b2/");
    expectedKeys.add("a/b3/");
    expectedKeys.add("a/b1/c1/");
    expectedKeys.add("a/b1/c2/");
    expectedKeys.add("a/b1/c1/c1.tx");
    expectedKeys.add("a/b1/c2/c2.tx");
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d2/");
    expectedKeys.add("a/b2/d3/");
    expectedKeys.add("a/b2/d1/d11.tx");
    expectedKeys.add("a/b2/d2/d21.tx");
    expectedKeys.add("a/b2/d2/d22.tx");
    expectedKeys.add("a/b2/d3/d31.tx");
    expectedKeys.add("a/b3/e1/");
    expectedKeys.add("a/b3/e2/");
    expectedKeys.add("a/b3/e3/");
    expectedKeys.add("a/b3/e1/e11.tx");
    expectedKeys.add("a/b3/e2/e21.tx");
    expectedKeys.add("a/b3/e3/e31.tx");
    checkKeyList(keyItr, expectedKeys);
  }

  private void checkKeyList(Iterator<? extends OzoneKey > ozoneKeyIterator,
      List<String> keys) {

    LinkedList<String> outputKeys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      String keyName = ozoneKey.getName();
      // GBJ should this be here?  seems like it should
      if (keyName.startsWith(".snapshot/snap1/")) {
          keyName = keyName.substring(".snapshot/snap1/".length());
      }
      outputKeys.add(keyName);
    }
    keys.sort(String::compareTo);
    outputKeys.sort(String::compareTo);
    Assert.assertEquals(keys, outputKeys);
  }

  private void createKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {
    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte) 96);
    for (String key : keys) {
      createKey(ozoneBucket, key, 10, input);
    }
  }

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
      byte[] input) throws Exception {

    OzoneOutputStream ozoneOutputStream =
            ozoneBucket.createKey(key, length);

    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();

    // Read the key with given key name.
    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(key);
    byte[] read = new byte[length];
    ozoneInputStream.read(read, 0, length);
    ozoneInputStream.close();

    String inputString = new String(input, StandardCharsets.UTF_8);
    Assert.assertEquals(inputString, new String(read, StandardCharsets.UTF_8));

    // Read using filesystem.
    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME,
            bucketName, volumeName, StandardCharsets.UTF_8);
    OzoneFileSystem o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath),
            conf);
    FSDataInputStream fsDataInputStream = o3fs.open(new Path(key));
    read = new byte[length];
    fsDataInputStream.read(read, 0, length);
    ozoneInputStream.close();

    Assert.assertEquals(inputString, new String(read, StandardCharsets.UTF_8));
  }

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  private void deleteRootDir() throws IOException {
    Path root = new Path("/");
    FileStatus[] fileStatuses = fs.listStatus(root);

    if (fileStatuses == null) {
      return;
    }

    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }

    fileStatuses = fs.listStatus(root);
    if (fileStatuses != null) {
      Assert.assertEquals("Delete root failed!", 0, fileStatuses.length);
    }
  }

  
}
