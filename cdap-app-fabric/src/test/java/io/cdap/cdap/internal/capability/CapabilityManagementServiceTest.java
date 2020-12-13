/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.capability;

import com.google.common.io.Files;
import com.google.gson.JsonObject;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class CapabilityManagementServiceTest extends AppFabricTestBase {

  private static ArtifactRepository artifactRepository;
  private static LocationFactory locationFactory;
  private static CConfiguration cConfiguration;
  private static CapabilityManagementService capabilityManagementService;

  @BeforeClass
  public static void setup() {
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    cConfiguration = getInjector().getInstance(CConfiguration.class);
    capabilityManagementService = getInjector().getInstance(CapabilityManagementService.class);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testCapabilityManagement() throws Exception {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, version);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    String externalConfigPath = tmpFolder.newFolder("capability-config-test").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR,
                       externalConfigPath);
    String fileName = "cap1.json";
    File testJson = new File(
      CapabilityManagementServiceTest.class.getResource(String.format("/%s", fileName)).getPath());
    Files.copy(testJson, new File(String.format("%s/%s", externalConfigPath, fileName)));
    capabilityManagementService.runTask();
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    artifactRepository.deleteArtifact(Id.Artifact.from(new Id.Namespace(namespace), appName, version));
  }

  void deployTestArtifact(String namespace, String appName, String version, Class<?> appClass) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(namespace), appName, version);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
  }
}
