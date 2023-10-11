/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.VolumeManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.util.ReflectionUtils.newInstance;


/**
 * Native (internal) implementation of {@link IAccessAuthorizer}.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public class SharedTmpDirAuthorizer implements IAccessAuthorizer {

  private static final Logger LOG =
      LoggerFactory.getLogger(SharedTmpDirAuthorizer.class);

  private static final Predicate<UserGroupInformation> NO_ADMIN = any -> false;

  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManager prefixManager;
  private Predicate<UserGroupInformation> adminCheck = NO_ADMIN;
  private Predicate<UserGroupInformation> readOnlyAdminCheck = NO_ADMIN;
  private boolean allowListAllVolumes;
  private OzoneNativeAuthorizer ozoneNativeAuthorizer;
  private IAccessAuthorizer rangerAuthorizer;
  public SharedTmpDirAuthorizer(VolumeManager volumeManager,
      BucketManager bucketManager, KeyManager keyManager,
      PrefixManager prefixManager, OzoneAdmins ozoneAdmins,
      OzoneConfiguration conf) {
    this.volumeManager = volumeManager;
    this.bucketManager = bucketManager;
    this.keyManager = keyManager;
    this.prefixManager = prefixManager;
    this.adminCheck = ozoneAdmins::isAdmin;
    ozoneNativeAuthorizer = new OzoneNativeAuthorizer(volumeManager, bucketManager, keyManager, prefixManager, ozoneAdmins);
    Class<? extends IAccessAuthorizer> clazz =
      conf.getClass(OZONE_ACL_AUTHORIZER_CLASS,
                    OzoneAccessAuthorizer.class,
                    IAccessAuthorizer.class);
    rangerAuthorizer = newInstance(clazz, conf);
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(IOzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);
    OzoneObjInfo objInfo;
    boolean isACLTypeDelete = (context.getAclRights() == ACLType.DELETE);

    if (ozObject instanceof OzoneObjInfo) {
      objInfo = (OzoneObjInfo) ozObject;
      if (objInfo.getVolumeName().equals("tmp") &&
          objInfo.getBucketName().equals("tmp") &&
          StringUtils.isNotEmpty(objInfo.getKeyName())
          && isACLTypeDelete) {
        return ozoneNativeAuthorizer.checkAccess(ozObject, context);
      }
    }
    return rangerAuthorizer.checkAccess(ozObject, context);
  }

  public void setVolumeManager(VolumeManager volumeManager) {
    this.volumeManager = volumeManager;
  }

  public void setBucketManager(BucketManager bucketManager) {
    this.bucketManager = bucketManager;
  }

  public void setKeyManager(KeyManager keyManager) {
    this.keyManager = keyManager;
  }

  public void setPrefixManager(PrefixManager prefixManager) {
    this.prefixManager = prefixManager;
  }

  @VisibleForTesting
  void setOzoneAdmins(OzoneAdmins admins) {
    setAdminCheck(admins::isAdmin);
  }

  @VisibleForTesting
  void setOzoneReadOnlyAdmins(OzoneAdmins readOnlyAdmins) {
    setReadOnlyAdminCheck(readOnlyAdmins::isAdmin);
  }

  public void setAdminCheck(Predicate<UserGroupInformation> check) {
    adminCheck = Objects.requireNonNull(check, "admin check");
  }

  public void setReadOnlyAdminCheck(Predicate<UserGroupInformation> check) {
    readOnlyAdminCheck = Objects.requireNonNull(check, "read-only admin check");
  }

  public void setAllowListAllVolumes(boolean allowListAllVolumes) {
    this.allowListAllVolumes = allowListAllVolumes;
  }

  public boolean getAllowListAllVolumes() {
    return allowListAllVolumes;
  }

  private static boolean isOwner(UserGroupInformation ugi, String ownerName) {
    return ownerName != null && ownerName.equals(ugi.getShortUserName());
  }
}
