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
import java.net.InetSocketAddress;
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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
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
  private boolean isNativeAuthorizerEnabled;
  private InetSocketAddress omRpcAddress;

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
        try {
          authorizer.setOzoneAdmins(OzoneManager.getOzoneAdminsFromConfig(configuration));
        } catch (IOException e) {
          // handle this
          e.printStackTrace();
        }
        authorizer.setAllowListAllVolumes(allowListAllVolumes);
      }
    } else {
      accessAuthorizer = null;
    }
  }

  // Create the snapshot manager by finding the corresponding RocksDB instance,
  //  creating an OmMetadataManagerImpl instance based on that
  //  and creating the other manager instances based on that metadataManager
  public static SnapshotManager createSnapshotManager(OzoneManager ozoneManager, String snapshotName){
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
    ScmBlockLocationProtocol
        scmBlockClient = getScmBlockClient(conf);
    ScmClient scmClient = new ScmClient(scmBlockClient, scmContainerClient);

    KeyManagerImpl km = new KeyManagerImpl(null, scmClient, smm, conf, null, null, null, pm );
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

  // This is a copy of lookupKey() from OzoneManager.java
  // ACL's and metrics are commented out because they aren't working yet
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    // Acls not working yet
    if (isAclEnabled) {
      checkAcls(OzoneObj.ResourceType.KEY, OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
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


  public ResolvedBucket resolveBucketLink(OmKeyArgs args)
      throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()));
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested)
      throws IOException {

    Pair<String, String> resolved;
    if (isAclEnabled) {
      UserGroupInformation ugi = getRemoteUser();
      if (getS3Auth() != null) {
        ugi = UserGroupInformation.createRemoteUser(
            OzoneAclUtils.accessIdToUserPrincipal(getS3Auth().getAccessId()));
      }
      InetAddress remoteIp = Server.getRemoteIp();
      resolved = resolveBucketLink(requested, new HashSet<>(),
          ugi,
          remoteIp, 
          remoteIp != null ? remoteIp.getHostName() :
               omRpcAddress.getHostName());
    } else {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          null, null, null);
    }
    return new ResolvedBucket(requested, resolved);
  }

  /**
   * Resolves bucket symlinks. Read permission is required for following links.
   *
   * @param volumeAndBucket the bucket to be resolved (if it is a link)
   * @param visited collects link buckets visited during the resolution to
   *   avoid infinite loops
   * @param {@link UserGroupInformation}
   * @param remoteAddress
   * @param hostName
   * @return bucket location possibly updated with its actual volume and bucket
   *   after following bucket links
   * @throws IOException (most likely OMException) if ACL check fails, bucket is
   *   not found, loop is detected in the links, etc.
   */
  private Pair<String, String> resolveBucketLink(
      Pair<String, String> volumeAndBucket,
      Set<Pair<String, String>> visited,
      UserGroupInformation userGroupInformation,
      InetAddress remoteAddress,
      String hostName) throws IOException {

    String volumeName = volumeAndBucket.getLeft();
    String bucketName = volumeAndBucket.getRight();
    OmBucketInfo info = bucketManager.getBucketInfo(volumeName, bucketName);
    if (!info.isLink()) {
      return volumeAndBucket;
    }

    if (!visited.add(volumeAndBucket)) {
      throw new OMException("Detected loop in bucket links",
          DETECTED_LOOP_IN_BUCKET_LINKS);
    }

    if (isAclEnabled) {
      final IAccessAuthorizer.ACLType type = IAccessAuthorizer.ACLType.READ;
      checkAcls(OzoneObj.ResourceType.BUCKET, OzoneObj.StoreType.OZONE, type,
          volumeName, bucketName, null, userGroupInformation,
          remoteAddress, hostName, true,
          getVolumeOwner(volumeName, type, OzoneObj.ResourceType.BUCKET));
    }

    return resolveBucketLink(
        Pair.of(info.getSourceVolume(), info.getSourceBucket()),
        visited, userGroupInformation, remoteAddress, hostName);
  }
  private void checkAcls(OzoneObj.ResourceType resType, OzoneObj.StoreType store,
                         IAccessAuthorizer.ACLType acl, String vol, String bucket, String key)
      throws IOException {
    UserGroupInformation user;
    if (getS3Auth() != null) {
      String principal =
          OzoneAclUtils.accessIdToUserPrincipal(getS3Auth().getAccessId());
      user = UserGroupInformation.createRemoteUser(principal);
    } else {
      user = ProtobufRpcEngine.Server.getRemoteUser();
    }

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    String volumeOwner = getVolumeOwner(vol, acl, resType);
    String bucketOwner = getBucketOwner(vol, bucket, acl, resType);

    checkAllAcls(this, resType, store, acl,
        vol, bucket, key, volumeOwner, bucketOwner,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp : omRpcAddress.getAddress(),
        remoteIp != null ? remoteIp.getHostName() : omRpcAddress.getHostName());
  }
  
  @SuppressWarnings("parameternumber")
  public boolean checkAcls(OzoneObj.ResourceType resType, OzoneObj.StoreType storeType,
                           IAccessAuthorizer.ACLType aclType, String vol, String bucket, String key,
                           UserGroupInformation ugi, InetAddress remoteAddress, String hostName,
                           boolean throwIfPermissionDenied, String owner)
      throws OMException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(resType)
        .setStoreType(storeType)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setKeyName(key).build();
    RequestContext context = RequestContext.newBuilder()
        .setClientUgi(ugi)
        .setIp(remoteAddress)
        .setHost(hostName)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(aclType)
        .setOwnerName(owner)
        .build();

    return checkAcls(obj, context, throwIfPermissionDenied);
  }

  /**
   * CheckAcls for the ozone object.
   *
   * @return true if permission granted, false if permission denied.
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied
   *                     and throwOnPermissionDenied set to true.
   */
  public boolean checkAcls(OzoneObj obj, RequestContext context,
                           boolean throwIfPermissionDenied)
      throws OMException {

    if (!accessAuthorizer.checkAccess(obj, context)) {
      if (throwIfPermissionDenied) {
        String volumeName = obj.getVolumeName() != null ?
                "Volume:" + obj.getVolumeName() + " " : "";
        String bucketName = obj.getBucketName() != null ?
                "Bucket:" + obj.getBucketName() + " " : "";
        String keyName = obj.getKeyName() != null ?
                "Key:" + obj.getKeyName() : "";
        LOG.warn("User {} doesn't have {} permission to access {} {}{}{}",
            context.getClientUgi().getUserName(), context.getAclRights(),
            obj.getResourceType(), volumeName, bucketName, keyName);
        throw new OMException("User " + context.getClientUgi().getUserName() +
            " doesn't have " + context.getAclRights() +
            " permission to access " + obj.getResourceType() + " " +
            volumeName  + bucketName + keyName, OMException.ResultCodes.PERMISSION_DENIED);
      }
      return false;
    } else {
      return true;
    }
  }



  public String getVolumeOwner(String vol, IAccessAuthorizer.ACLType type, OzoneObj.ResourceType resType)
      throws OMException {
    String volOwnerName = null;
    if (!vol.equals(OzoneConsts.OZONE_ROOT) &&
        !(type == IAccessAuthorizer.ACLType.CREATE && resType == OzoneObj.ResourceType.VOLUME)) {
      volOwnerName = getVolumeOwner(vol);
    }
    return volOwnerName;
  }

  private String getVolumeOwner(String volume) throws OMException {
    Boolean lockAcquired = smMetadataManager.getLock().acquireReadLock(
        VOLUME_LOCK, volume);
    String dbVolumeKey = smMetadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs = null;
    try {
      volumeArgs = smMetadataManager.getVolumeTable().get(dbVolumeKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException)ioe;
      } else {
        throw new OMException("getVolumeOwner for Volume " + volume + " failed",
            OMException.ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        smMetadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
      }
    }
    if (volumeArgs != null) {
      return volumeArgs.getOwnerName();
    } else {
      throw new OMException("Volume " + volume + " is not found",
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }
  }

  /**
   * Return the owner of a given bucket.
   *
   * @return String
   */
  public String getBucketOwner(String volume, String bucket, IAccessAuthorizer.ACLType type,
       OzoneObj.ResourceType resType) throws OMException {
    String bucketOwner = null;
    if ((resType != OzoneObj.ResourceType.VOLUME) &&
        !(type == IAccessAuthorizer.ACLType.CREATE && resType == OzoneObj.ResourceType.BUCKET)) {
      bucketOwner = getBucketOwner(volume, bucket);
    }
    return bucketOwner;
  }

  private String getBucketOwner(String volume, String bucket)
      throws OMException {

    Boolean lockAcquired = smMetadataManager.getLock().acquireReadLock(
            BUCKET_LOCK, volume, bucket);
    String dbBucketKey = smMetadataManager.getBucketKey(volume, bucket);
    OmBucketInfo bucketInfo = null;
    try {
      bucketInfo = smMetadataManager.getBucketTable().get(dbBucketKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException)ioe;
      } else {
        throw new OMException("getBucketOwner for Bucket " + volume + "/" +
            bucket  + " failed: " + ioe.getMessage(),
            OMException.ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        smMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
      }
    }
    if (bucketInfo != null) {
      return bucketInfo.getOwner();
    } else {
      throw new OMException("Bucket not found", OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
  }
  private IAccessAuthorizer getACLAuthorizerInstance(OzoneConfiguration conf) {
    Class<? extends IAccessAuthorizer> clazz = conf.getClass(
        OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthorizer.class,
        IAccessAuthorizer.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  @SuppressWarnings("parameternumber")
  private static void checkAllAcls(SnapshotManager snapshotManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key, String volOwner,
      String bucketOwner, UserGroupInformation user, InetAddress remoteAddress,
      String hostName) throws IOException {

    boolean isVolOwner = isOwner(user, volOwner);

    IAccessAuthorizer.ACLType parentAclRight = aclType;

    //OzoneNativeAuthorizer differs from Ranger Authorizer as Ranger requires
    // only READ access on parent level access. OzoneNativeAuthorizer has
    // different parent level access based on the child level access type
    if (snapshotManager.isNativeAuthorizerEnabled()) {
      if (aclType == IAccessAuthorizer.ACLType.CREATE ||
          aclType == IAccessAuthorizer.ACLType.DELETE ||
          aclType == IAccessAuthorizer.ACLType.WRITE_ACL) {
        parentAclRight = IAccessAuthorizer.ACLType.WRITE;
      } else if (aclType == IAccessAuthorizer.ACLType.READ_ACL ||
          aclType == IAccessAuthorizer.ACLType.LIST) {
        parentAclRight = IAccessAuthorizer.ACLType.READ;
      }
    } else {
      parentAclRight =  IAccessAuthorizer.ACLType.READ;
    }

    switch (resType) {
    //For Volume level access we only need to check {OWNER} equal
    // to Volume Owner.
    case VOLUME:
      snapshotManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
          user, remoteAddress, hostName, true,
          volOwner);
      break;
    case BUCKET:
    case KEY:
    //For Bucket/Key/Prefix level access, first we need to check {OWNER} equal
    // to volume owner on parent volume. Then we need to check {OWNER} equals
    // volume owner if current ugi user is volume owner else we need check
    //{OWNER} equals bucket owner for bucket/key/prefix.
    case PREFIX:
      snapshotManager.checkAcls(OzoneObj.ResourceType.VOLUME, storeType,
          parentAclRight, vol, bucket, key, user,
          remoteAddress, hostName, true,
          volOwner);
      if (isVolOwner) {
        snapshotManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            volOwner);
      } else {
        snapshotManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            bucketOwner);
      }
      break;
    default:
      throw new OMException("Unexpected object type:" +
              resType, INVALID_REQUEST);
    }
  }

  private static boolean isOwner(UserGroupInformation callerUgi,
      String ownerName) {
    if (ownerName == null) {
      return false;
    }
    if (callerUgi.getUserName().equals(ownerName) ||
        callerUgi.getShortUserName().equals(ownerName)) {
      return true;
    }
    return false;
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
  public boolean isNativeAuthorizerEnabled() {
    return isNativeAuthorizerEnabled;
  }
}
