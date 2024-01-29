/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.hadoop.ozone.recon.ReconControllerModule.ReconDaoBindingModule.RECON_DAO_LIST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.SQLException;

import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.junit.jupiter.api.Test;

/**
 * Class to test basic SQL schema setup.
 */
public class TestSqlSchemaSetup extends AbstractReconSqlDBTest {

  public TestSqlSchemaSetup() {
    super();
  }

  /**
   * Make sure schema was created correctly.
   * @throws SQLException
   */
  @Test
  public void testSchemaSetup() throws SQLException {
    assertNotNull(getInjector());
    assertNotNull(getConfiguration());
    assertNotNull(getDslContext());
    assertNotNull(getConnection());
    RECON_DAO_LIST.forEach(dao -> {
      assertNotNull(getDao(dao));
    });
    ReconTaskStatusDao dao = getDao(ReconTaskStatusDao.class);
    dao.insert(new ReconTaskStatus("TestTask", 1L, 2L));
    assertEquals(1, dao.findAll().size());
  }
}
