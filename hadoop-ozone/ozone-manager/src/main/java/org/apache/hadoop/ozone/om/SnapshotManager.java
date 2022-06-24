package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import static org.apache.hadoop.hdds.utils.HAUtils.getScmBlockClient;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmContainerClient;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;
import static org.apache.hadoop.ozone.om.KeyManagerImpl.getRemoteUser;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OzoneManager.getACLAuthorizerInstance;
import static org.apache.hadoop.ozone.om.OzoneManager.getS3Auth;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

public class SnapshotManager {
  private final KeyManagerImpl keyManager;
  private final PrefixManagerImpl prefixManager;
  private final VolumeManagerImpl volumeManager;
  private final BucketManagerImpl bucketManager;
  private final OmMetadataManagerImpl smMetadataManager;
  private final OzoneConfiguration configuration;
  private final boolean isAclEnabled;
  private final IAccessAuthorizer accessAuthorizer;
  private final boolean allowListAllVolumes;
  private final boolean isNativeAuthorizerEnabled;
  private final InetSocketAddress omRpcAddress;

  public static final Logger LOG =
      LoggerFactory.getLogger(SnapshotManager.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private static final Map<String, SnapshotManager> snapshotManagerCache = new HashMap<>();

  // private so as not to bypass cache
  private SnapshotManager(KeyManagerImpl keyManager,
                          PrefixManagerImpl prefixManager,
                          VolumeManagerImpl volumeManager,
                          BucketManagerImpl bucketManager,
                          OmMetadataManagerImpl smMetadataManager,
                          OzoneConfiguration conf,
                          InetSocketAddress omRpcAddress) {
    this.keyManager = keyManager;
    this.bucketManager = bucketManager;
    this.volumeManager = volumeManager;
    this.prefixManager = prefixManager;
    this.smMetadataManager = smMetadataManager;
    this.configuration = conf;
    this.omRpcAddress = omRpcAddress;
    this.isAclEnabled = configuration.getBoolean(OZONE_ACL_ENABLED,
        OZONE_ACL_ENABLED_DEFAULT);
    this.allowListAllVolumes = configuration.getBoolean(
        OZONE_OM_VOLUME_LISTALL_ALLOWED,
        OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT);
    if (isAclEnabled) {
      accessAuthorizer = getACLAuthorizerInstance(configuration);
      if (accessAuthorizer instanceof OzoneNativeAuthorizer) {
        OzoneNativeAuthorizer authorizer =
            (OzoneNativeAuthorizer) accessAuthorizer;
        isNativeAuthorizerEnabled = true;
        authorizer.setVolumeManager(volumeManager);
        authorizer.setBucketManager(bucketManager);
        authorizer.setKeyManager(keyManager);
        authorizer.setPrefixManager(prefixManager);
        authorizer.setOzoneAdmins(conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS));
        authorizer.setAllowListAllVolumes(allowListAllVolumes);
      } else {
        isNativeAuthorizerEnabled = false;
      }
    } else {
      accessAuthorizer = null;
      isNativeAuthorizerEnabled = false;
    }
  }

  // Create the snapshot manager by finding the corresponding RocksDB instance,
  //  creating an OmMetadataManagerImpl instance based on that
  //  and creating the other manager instances based on that metadataManager
  public static synchronized SnapshotManager createSnapshotManager(OzoneManager ozoneManager, String snapshotName){
    if (snapshotName == null || snapshotName.isEmpty()) {
      return null;
    }
    OmMetadataManagerImpl smm = null;
    if (snapshotManagerCache.containsKey(snapshotName)) {
      return snapshotManagerCache.get(snapshotName);
    }
    OzoneConfiguration conf = ozoneManager.getConfiguration();
    try {
      smm = OmMetadataManagerImpl.createSnapshotMetadataManager(ozoneManager.getConfiguration(), snapshotName + "_checkpoint_");
    } catch (IOException e) {
      // handle this
      e.printStackTrace();
    }
    PrefixManagerImpl pm = new PrefixManagerImpl(smm, false);
    VolumeManagerImpl vm = new VolumeManagerImpl(smm, conf);
    BucketManagerImpl bm = new BucketManagerImpl(smm);
    StorageContainerLocationProtocol
        scmContainerClient = getScmContainerClient(conf);
    KeyManagerImpl km = new KeyManagerImpl(null, ozoneManager.getScmClient(), smm, conf, null,
        ozoneManager.getBlockTokenSecretManager(), ozoneManager.getKmsProvider(), pm );
    SnapshotManager sm = new SnapshotManager(km, pm, vm, bm, smm, conf, ozoneManager.getOmRpcServerAddr());
    snapshotManagerCache.put(snapshotName, sm);
    return sm;
  }

  // Get SnapshotManager based on keyname
  public static SnapshotManager getSnapshotManager(OzoneManager ozoneManager,  String keyname) {
    SnapshotManager sm = null;
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 2) &&keyParts[0].compareTo(".snapshot") == 0) {
      sm = SnapshotManager.createSnapshotManager(ozoneManager, keyParts[1]);
    }
    return sm;
  }

  // Remove snapshot indicator from keyname
  public static String fixKeyname(String keyname) {
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 2) && (keyParts[0].compareTo(".snapshot") == 0)) {
      return String.join("/", Arrays.copyOfRange(keyParts, 2, keyParts.length));
    }
    return keyname;
  }
}
