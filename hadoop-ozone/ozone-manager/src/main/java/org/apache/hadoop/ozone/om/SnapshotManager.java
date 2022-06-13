package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneConsts;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
  private OzoneManager ozoneManager;
  private boolean allowListAllVolumes;

  public static final Logger LOG =
      LoggerFactory.getLogger(SnapshotManager.class);

  private SnapshotManager(KeyManagerImpl keyManager,
                          PrefixManagerImpl prefixManager,
                          VolumeManagerImpl volumeManager,
                          BucketManagerImpl bucketManager,
                          OmMetadataManagerImpl smMetadataManager,
                          OzoneManager ozoneManager,
                          OzoneConfiguration conf) {
    this.keyManager = keyManager;
    this.bucketManager = bucketManager;
    this.volumeManager = volumeManager;
    this.prefixManager = prefixManager;
    this.smMetadataManager = smMetadataManager;
    this.ozoneManager = ozoneManager;
    this.configuration = conf;
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
        authorizer.setVolumeManager(volumeManager);
        authorizer.setBucketManager(bucketManager);
        authorizer.setKeyManager(keyManager);
        authorizer.setPrefixManager(prefixManager);
        try {
          authorizer.setOzoneAdmins(ozoneManager.getOzoneAdminsFromConfig(configuration));
        } catch (IOException e) {
          e.printStackTrace();
        }
        authorizer.setAllowListAllVolumes(allowListAllVolumes);
      }
    } else {
      accessAuthorizer = null;
    }
  }
  private static SnapshotManager sm;
  public static SnapshotManager createSnapshotManager(OzoneManager om, OzoneConfiguration conf, String snapshotName){
    OmMetadataManagerImpl smm = null;
    //add cache
    if (sm != null)
      return sm;
    try {
      smm = OmMetadataManagerImpl.createSnapshotMetadataManager(conf, snapshotName + "_checkpoint_");
    } catch (IOException e) {
      e.printStackTrace();
    }
    PrefixManagerImpl pm = new PrefixManagerImpl(smm, false);
    VolumeManagerImpl vm = new VolumeManagerImpl(smm, conf);
    BucketManagerImpl bm = new BucketManagerImpl(smm);
    StorageContainerLocationProtocol
        scmContainerClient = getScmContainerClient(conf);
    // verifies that the SCM info in the OM Version file is correct.
    ScmBlockLocationProtocol
        scmBlockClient = getScmBlockClient(conf);
    ScmClient scmClient = new ScmClient(scmBlockClient, scmContainerClient);

    KeyManagerImpl km = new KeyManagerImpl(null, scmClient, smm, conf, null, null, null, pm );
    sm = new SnapshotManager(km, pm, vm, bm, smm, om, conf);
    return sm;
  }
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(OzoneObj.ResourceType.KEY, OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      //metrics.incNumKeyLookups();
      return keyManager.lookupKey(args, getClientAddress());
    } catch (Exception ex) {
      //metrics.incNumKeyLookupFails();
      auditSuccess = false;
      // AUDIT.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
      //     auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        // AUDIT.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
        //     auditMap));
      }
    }
  }

  public ResolvedBucket resolveBucketLink(
      OzoneManagerProtocolProtos.KeyArgs args,
      OMClientRequest omClientRequest) throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()), omClientRequest);
  }

  public ResolvedBucket resolveBucketLink(OmKeyArgs args)
      throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()));
  }

  public ResolvedBucket resolveBucketLink(Pair<String, String> requested,
                                          OMClientRequest omClientRequest)
      throws IOException {
    Pair<String, String> resolved;
    if (isAclEnabled) {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          omClientRequest.createUGI(), omClientRequest.getRemoteAddress(),
          omClientRequest.getHostName());
    } else {
      resolved = resolveBucketLink(requested, new HashSet<>(),
          null, null, null);
    }
    return new ResolvedBucket(requested, resolved);
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
          remoteIp, null);
          // remoteIp != null ? remoteIp.getHostName() :
          //     omRpcAddress.getHostName());
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

    // OzoneAclUtils.checkAllAcls(this, resType, store, acl,
    //     vol, bucket, key, volumeOwner, bucketOwner,
    //     user != null ? user : getRemoteUser(), null, null);
        // remoteIp != null ? remoteIp : omRpcAddress.getAddress(),
        // remoteIp != null ? remoteIp.getHostName() : omRpcAddress.getHostName());
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

  private static String getClientAddress() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) { //not a RPC client
      clientMachine = "";
    }
    return clientMachine;
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

}
