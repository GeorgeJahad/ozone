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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.VolumeManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.util.ReflectionUtils.newInstance;


/**
 * SharedTmp implementation of {@link IAccessAuthorizer}.
 */
public class SharedTmpDirAuthorizer implements IAccessAuthorizer {

  private static final Logger LOG =
      LoggerFactory.getLogger(SharedTmpDirAuthorizer.class);
  private final OzoneNativeAuthorizer ozoneNativeAuthorizer;
  private final IAccessAuthorizer rangerAuthorizer;

  public SharedTmpDirAuthorizer(OzoneManager om, OzoneConfiguration conf) {
    ozoneNativeAuthorizer = new OzoneNativeAuthorizer();
    ozoneNativeAuthorizer.setVolumeManager(om.getVolumeManager());
    ozoneNativeAuthorizer.setBucketManager(om.getBucketManager());
    ozoneNativeAuthorizer.setKeyManager(om.getKeyManager());
    ozoneNativeAuthorizer.setPrefixManager(om.getPrefixManager());
    ozoneNativeAuthorizer.setAdminCheck(om::isAdmin);
    ozoneNativeAuthorizer.setReadOnlyAdminCheck(om::isReadOnlyAdmin);
    ozoneNativeAuthorizer.setAllowListAllVolumes(om.getAllowListAllVolumes());

    Class<? extends IAccessAuthorizer> clazz =
      conf.getClass(OZONE_ACL_AUTHORIZER_CLASS,
                    OzoneAccessAuthorizer.class,
                    IAccessAuthorizer.class);
    LOG.info("Initing class {} from SharedTmpDirAuthorizer", clazz);
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

}
