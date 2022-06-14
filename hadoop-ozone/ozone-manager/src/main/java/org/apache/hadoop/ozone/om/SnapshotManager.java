package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.*;
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
import java.util.*;

import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmBlockClient;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmContainerClient;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;
import static org.apache.hadoop.ozone.om.KeyManagerImpl.getRemoteUser;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OzoneManager.getS3Auth;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

public class SnapshotManager {
  private KeyManagerImpl keyManager;
  private PrefixManagerImpl prefixManager;
  private VolumeManagerImpl volumeManager;
  private BucketManagerImpl bucketManager;
  private OmMetadataManagerImpl smMetadataManager;
  private OzoneConfiguration configuration;
  private boolean isAclEnabled;
  private IAccessAuthorizer accessAuthorizer;
  private boolean allowListAllVolumes;

  public static final Logger LOG =
      LoggerFactory.getLogger(SnapshotManager.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private static final Map<String, SnapshotManager> snapshotManagerCache = new HashMap<>();

  private SnapshotManager(KeyManagerImpl keyManager,
                          PrefixManagerImpl prefixManager,
                          VolumeManagerImpl volumeManager,
                          BucketManagerImpl bucketManager,
                          OmMetadataManagerImpl smMetadataManager,
                          OzoneConfiguration conf) {
    this.keyManager = keyManager;
    this.bucketManager = bucketManager;
    this.volumeManager = volumeManager;
    this.prefixManager = prefixManager;
    this.smMetadataManager = smMetadataManager;
    this.configuration = conf;
    this.isAclEnabled = configuration.getBoolean(OZONE_ACL_ENABLED,
        OZONE_ACL_ENABLED_DEFAULT);
  }

  // Create the snapshot manager by finding the corresponding RocksDB instance,
  //  creating an OmMetadataManagerImpl instance based on that
  //  and creating the other manager instances based on that metadataManager
  public static SnapshotManager createSnapshotManager(OzoneConfiguration conf, String snapshotName){
    OmMetadataManagerImpl smm = null;
    if (snapshotManagerCache.containsKey(snapshotName)) {
      return snapshotManagerCache.get(snapshotName);
    }
    try {
      smm = OmMetadataManagerImpl.createSnapshotMetadataManager(conf, snapshotName + "_checkpoint_");
    } catch (IOException e) {
      // handle this
      e.printStackTrace();
    }
    PrefixManagerImpl pm = new PrefixManagerImpl(smm, false);
    VolumeManagerImpl vm = new VolumeManagerImpl(smm, conf);
    BucketManagerImpl bm = new BucketManagerImpl(smm);
    StorageContainerLocationProtocol
        scmContainerClient = getScmContainerClient(conf);
    ScmBlockLocationProtocol
        scmBlockClient = getScmBlockClient(conf);
    ScmClient scmClient = new ScmClient(scmBlockClient, scmContainerClient);

    KeyManagerImpl km = new KeyManagerImpl(null, scmClient, smm, conf, null, null, null, pm );
    SnapshotManager sm = new SnapshotManager(km, pm, vm, bm, smm, conf);
    snapshotManagerCache.put(snapshotName, sm);
    return sm;
  }

  // Get SnapshotManager based on keyname
  public static SnapshotManager getSnapshotManager(OzoneConfiguration conf,  String keyname) {
    SnapshotManager sm = null;
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 2) &&keyParts[0].compareTo(".snapshot") == 0) {
      sm = SnapshotManager.createSnapshotManager(conf, keyParts[1]);
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

  // This is a copy of lookupKey() from OzoneManager.java
  // ACL's and metrics are commented out because they aren't working yet
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    // Acls not working yet
    if (isAclEnabled) {
      // checkAcls(OzoneObj.ResourceType.KEY, OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.READ,
      //     bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      // metrics not working yet
      //metrics.incNumKeyLookups();
      return keyManager.lookupKey(args, getClientAddress());
    } catch (Exception ex) {
      //metrics.incNumKeyLookupFails();
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
           auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
             auditMap));
      }
    }
  }


  // Temp hack because ACL's not working yet.
  // It doesn't actually resolve the link, just returns the original
  public ResolvedBucket resolveBucketLink(OmKeyArgs args)
      throws IOException {
    return new ResolvedBucket(
        Pair.of(args.getVolumeName(), args.getBucketName()),
        Pair.of(args.getVolumeName(), args.getBucketName()));
  }


  //  All code below this is an identical copy from OzoneManager.java
  //   which should be moved into a library shared by both
  private static String getClientAddress() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) { //not a RPC client
      clientMachine = "";
    }
    return clientMachine;
  }

  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
                                                  Map<String, String> auditMap) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }

  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable)
        .build();
  }
}
