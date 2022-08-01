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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmBlockClient;
import static org.apache.hadoop.hdds.utils.HAUtils.getScmContainerClient;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.om.KeyManagerImpl.getRemoteUser;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OzoneManager.getS3Auth;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;

/**
 * OM Metadata Reading class for the OM and Snapshot managers.
 */
public class OmMReader implements Auditor {
  private final KeyManager keyManager;
  private final PrefixManager prefixManager;
  private final VolumeManager volumeManager;
  private final BucketManager bucketManager;
  private final OMMetadataManager metadataManager;
  private final OzoneManager ozoneManager;
  private final OzoneConfiguration configuration;
  private final boolean isAclEnabled;
  private final IAccessAuthorizer accessAuthorizer;
  private final boolean allowListAllVolumes;
  private final boolean isNativeAuthorizerEnabled;
  private final OmMReaderMetrics metrics;
  private final Logger log;
  private final AuditLogger audit;

  public OmMReader(KeyManager keyManager,
                          PrefixManager prefixManager,
                          OMMetadataManager metadataManager,
                   OzoneManager ozoneManager,
                   Logger log,
                   AuditLogger audit,
                   OmMReaderMetrics omMReaderMetrics) {
    this.keyManager = keyManager;
    this.bucketManager = ozoneManager.getBucketManager();
    this.volumeManager = ozoneManager.getVolumeManager();
    this.prefixManager = prefixManager;
    this.metadataManager = metadataManager;
    this.configuration = ozoneManager.getConfiguration();
    this.ozoneManager = ozoneManager;
    this.isAclEnabled = ozoneManager.getAclsEnabled();
    this.log = log;
    this.audit = audit;
    this.allowListAllVolumes = ozoneManager.getAllowListAllVolumes();
    metrics = omMReaderMetrics;
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
        authorizer.setOzoneAdmins(ozoneManager.getOmAdminUsernames());
        authorizer.setAllowListAllVolumes(allowListAllVolumes);
      } else {
        isNativeAuthorizerEnabled = false;
      }
    } else {
      accessAuthorizer = null;
      isNativeAuthorizerEnabled = false;
    }
  }

  /**
   * Lookup a key.
   *
   * @param args - attributes of the key.
   * @return OmKeyInfo - the info about the requested key.
   * @throws IOException
   */
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumKeyLookups();
      return keyManager.lookupKey(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumKeyLookupFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
            auditMap));
      }
    }
  }

  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
                                          String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {

    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(getResourceType(args), StoreType.OZONE, ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumListStatus();
      return keyManager.listStatus(args, recursive, startKey, numEntries,
              getClientAddress(), allowPartialPrefixes);
    } catch (Exception ex) {
      metrics.incNumListStatusFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_STATUS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.LIST_STATUS, auditMap));
      }
    }
  }
  
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumGetFileStatus();
      return keyManager.getFileStatus(args, getClientAddress());
    } catch (IOException ex) {
      metrics.incNumGetFileStatusFails();
      auditSuccess = false;
      audit.logReadFailure(
          buildAuditMessageForFailure(OMAction.GET_FILE_STATUS, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.GET_FILE_STATUS, auditMap));
      }
    }
  }

  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = resolveBucketLink(args);

    if (isAclEnabled) {
      checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
          bucket.realVolume(), bucket.realBucket(), args.getKeyName());
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      metrics.incNumLookupFile();
      return keyManager.lookupFile(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumLookupFileFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.LOOKUP_FILE,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.LOOKUP_FILE, auditMap));
      }
    }
  }

  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {

    ResolvedBucket bucket = resolveBucketLink(Pair.of(volumeName, bucketName));

    if (isAclEnabled) {
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.LIST,
          bucket.realVolume(), bucket.realBucket(), keyPrefix);
    }

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit();
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.KEY_PREFIX, keyPrefix);

    try {
      metrics.incNumKeyLists();
      return keyManager.listKeys(bucket.realVolume(), bucket.realBucket(),
          startKey, keyPrefix, maxKeys);
    } catch (IOException ex) {
      metrics.incNumKeyListFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_KEYS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_KEYS,
            auditMap));
      }
    }
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    boolean auditSuccess = true;

    try {
      if (isAclEnabled) {
        checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.READ_ACL,
            obj.getVolumeName(), obj.getBucketName(), obj.getKeyName());
      }
      metrics.incNumGetAcl();
      switch (obj.getResourceType()) {
      case VOLUME:
        return volumeManager.getAcl(obj);
      case BUCKET:
        return bucketManager.getAcl(obj);
      case KEY:
        return keyManager.getAcl(obj);
      case PREFIX:
        return prefixManager.getAcl(obj);

      default:
        throw new OMException("Unexpected resource type: " +
            obj.getResourceType(), INVALID_REQUEST);
      }
    } catch (Exception ex) {
      auditSuccess = false;
      audit.logReadFailure(
          buildAuditMessageForFailure(OMAction.GET_ACL, obj.toAuditMap(), ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.GET_ACL, obj.toAuditMap()));
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
              ozoneManager.getOmRpcServerAddr().getHostName());
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
  Pair<String, String> resolveBucketLink(
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
      final ACLType type = ACLType.READ;
      checkAcls(ResourceType.BUCKET, StoreType.OZONE, type,
          volumeName, bucketName, null, userGroupInformation,
          remoteAddress, hostName, true,
          getVolumeOwner(volumeName, type, ResourceType.BUCKET));
    }

    return resolveBucketLink(
        Pair.of(info.getSourceVolume(), info.getSourceBucket()),
        visited, userGroupInformation, remoteAddress, hostName);
  }

  public ResolvedBucket resolveBucketLink(KeyArgs args,
      OMClientRequest omClientRequest) throws IOException {
    return resolveBucketLink(
        Pair.of(args.getVolumeName(), args.getBucketName()), omClientRequest);
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

  /**
   * Checks if current caller has acl permissions.
   *
   * @param resType - Type of ozone resource. Ex volume, bucket.
   * @param store   - Store type. i.e Ozone, S3.
   * @param acl     - type of access to be checked.
   * @param vol     - name of volume
   * @param bucket  - bucket name
   * @param key     - key
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied.
   */
  void checkAcls(ResourceType resType, StoreType store,
      ACLType acl, String vol, String bucket, String key)
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

    OzoneAclUtils.checkAllAcls(this, resType, store, acl,
        vol, bucket, key, volumeOwner, bucketOwner,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp :
            ozoneManager.getOmRpcServerAddr().getAddress(),
        remoteIp != null ? remoteIp.getHostName() :
            ozoneManager.getOmRpcServerAddr().getHostName());
  }

  
  /**
   * CheckAcls for the ozone object.
   *
   * @return true if permission granted, false if permission denied.
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied
   *                     and throwOnPermissionDenied set to true.
   */
  @SuppressWarnings("parameternumber")
  public boolean checkAcls(ResourceType resType, StoreType storeType,
      ACLType aclType, String vol, String bucket, String key,
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
        .setAclType(ACLIdentityType.USER)
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
        log.warn("User {} doesn't have {} permission to access {} {}{}{}",
            context.getClientUgi().getUserName(), context.getAclRights(),
            obj.getResourceType(), volumeName, bucketName, keyName);
        throw new OMException("User " + context.getClientUgi().getUserName() +
            " doesn't have " + context.getAclRights() +
            " permission to access " + obj.getResourceType() + " " +
            volumeName  + bucketName + keyName, ResultCodes.PERMISSION_DENIED);
      }
      return false;
    } else {
      return true;
    }
  }


  public String getVolumeOwner(String vol, ACLType type, ResourceType resType)
      throws OMException {
    String volOwnerName = null;
    if (!vol.equals(OzoneConsts.OZONE_ROOT) &&
        !(type == ACLType.CREATE && resType == ResourceType.VOLUME)) {
      volOwnerName = getVolumeOwner(vol);
    }
    return volOwnerName;
  }

  private String getVolumeOwner(String volume) throws OMException {
    Boolean lockAcquired = metadataManager.getLock().acquireReadLock(
        VOLUME_LOCK, volume);
    String dbVolumeKey = metadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs = null;
    try {
      volumeArgs = metadataManager.getVolumeTable().get(dbVolumeKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException)ioe;
      } else {
        throw new OMException("getVolumeOwner for Volume " + volume + " failed",
            ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
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
  public String getBucketOwner(String volume, String bucket, ACLType type,
       ResourceType resType) throws OMException {
    String bucketOwner = null;
    if ((resType != ResourceType.VOLUME) &&
        !(type == ACLType.CREATE && resType == ResourceType.BUCKET)) {
      bucketOwner = getBucketOwner(volume, bucket);
    }
    return bucketOwner;
  }

  private String getBucketOwner(String volume, String bucket)
      throws OMException {

    Boolean lockAcquired = metadataManager.getLock().acquireReadLock(
            BUCKET_LOCK, volume, bucket);
    String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
    OmBucketInfo bucketInfo = null;
    try {
      bucketInfo = metadataManager.getBucketTable().get(dbBucketKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException)ioe;
      } else {
        throw new OMException("getBucketOwner for Bucket " + volume + "/" +
            bucket  + " failed: " + ioe.getMessage(),
            ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
      }
    }
    if (bucketInfo != null) {
      return bucketInfo.getOwner();
    } else {
      throw new OMException("Bucket not found", ResultCodes.BUCKET_NOT_FOUND);
    }
  }

  
  /**
   * Returns an instance of {@link IAccessAuthorizer}.
   * Looks up the configuration to see if there is custom class specified.
   * Constructs the instance by passing the configuration directly to the
   * constructor to achieve thread safety using final fields.
   *
   * @param conf
   * @return IAccessAuthorizer
   */
  private IAccessAuthorizer getACLAuthorizerInstance(OzoneConfiguration conf) {
    Class<? extends IAccessAuthorizer> clazz = conf.getClass(
        OZONE_ACL_AUTHORIZER_CLASS, OzoneAccessAuthorizer.class,
        IAccessAuthorizer.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

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

  private Map<String, String> buildAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }

  /**
   * Returns true if OzoneNativeAuthorizer is enabled and false if otherwise.
   *
   * @return if native authorizer is enabled.
   */
  public boolean isNativeAuthorizerEnabled() {
    return isNativeAuthorizerEnabled;
  }

  public IAccessAuthorizer getAccessAuthorizer() {
    return accessAuthorizer;
  }

  private ResourceType getResourceType(OmKeyArgs args) {
    if (args.getKeyName() == null || args.getKeyName().length() == 0) {
      return ResourceType.BUCKET;
    }
    return ResourceType.KEY;
  }

  
}
