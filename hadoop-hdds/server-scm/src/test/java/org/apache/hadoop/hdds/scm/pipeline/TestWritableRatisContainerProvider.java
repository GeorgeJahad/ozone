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
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.*;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests to validate the WritableRatisContainerProvider works correctly.
 */
public class TestWritableRatisContainerProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestWritableRatisContainerProvider.class);
  private static final String OWNER = "SCM";
  private PipelineManager pipelineManager, pipelineManagerSpy;
  private ContainerManager containerManager
      = Mockito.mock(ContainerManager.class);
  private PipelineChoosePolicy pipelineChoosingPolicy
      = new HealthyPipelineChoosePolicy();

  private OzoneConfiguration conf;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;
  private NodeManager nodeManager;
  private WritableContainerProvider provider;
  private ReplicationConfig repConfig;
  private int minPipelines;
  private Pipeline allocatedPipeline;
  private ContainerInfo container;

  public void setup() throws IOException {

    // Init the mock pipeline manager
    repConfig = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
    conf = new OzoneConfiguration();
    File testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    nodeManager = new MockNodeManager(true, 10);
    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);
    pipelineManagerSpy = spy(pipelineManager);

    // Throw on all pipeline creates, so no new pipelines can be created
    doThrow(SCMException.class).when(pipelineManagerSpy).createPipeline(any(), any(), anyList());
    provider = new WritableRatisContainerProvider(
        conf, pipelineManagerSpy, containerManager, pipelineChoosingPolicy);

    // Add a single pipeline to manager, (in the allocated state)
    allocatedPipeline = pipelineManager.createPipeline(repConfig);
    ((MockPipelineManager)pipelineManager).allocatePipeline(allocatedPipeline, false);

    // Assign a container to that pipeline
    container = createContainer(allocatedPipeline,
        repConfig, System.nanoTime());
    pipelineManager.addContainerToPipeline(
        allocatedPipeline.getId(), container.containerID());
    doReturn(container).when(containerManager).getMatchingContainer(anyLong(),
        anyString(), eq(allocatedPipeline), any());

  }

  @Test
  public void testWaitForAllocatedPipeline()
      throws IOException {
    setup();

    // Confirm there are no open pipelines and an allocated one
    assertTrue(pipelineManager.getPipelines(repConfig, OPEN).isEmpty());
    assertTrue(pipelineManager.getPipelines(repConfig, ALLOCATED)
        .contains(allocatedPipeline));

    ContainerInfo c =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    // Confirm that no open pipelines were found
    assertNull(c);

    // reset pipeline manager to the initial state
    setup();
    assertTrue(pipelineManager.getPipelines(repConfig, OPEN).isEmpty());
    assertTrue(pipelineManager.getPipelines(repConfig,  ALLOCATED).contains(allocatedPipeline));

    doAnswer(call -> {
      pipelineManager.openPipeline(allocatedPipeline.getId());
      return allocatedPipeline;
    }).when(pipelineManagerSpy).waitOnePipelineReady(any(), anyLong());
    c = provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertEquals(c, container);


  }

  private ContainerInfo createContainer(Pipeline pipeline,
      ReplicationConfig repConf, long containerID) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setOwner(OWNER)
        .setReplicationConfig(repConf)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId())
        .setNumberOfKeys(0)
        .setUsedBytes(0)
        .setSequenceId(0)
        .setDeleteTransactionId(0)
        .build();
  }

}
