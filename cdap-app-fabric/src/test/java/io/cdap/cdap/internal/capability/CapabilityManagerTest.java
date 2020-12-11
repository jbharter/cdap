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
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.SleepingWorkflowApp;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.ApplicationDetail;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class CapabilityManagerTest extends AppFabricTestBase {

  public static final String TEST_VERSION = "1.0.0";
  private static ApplicationLifecycleService applicationLifecycleService;
  private static ArtifactRepository artifactRepository;
  private static CapabilityManager capabilityManager;
  private static LocationFactory locationFactory;
  private static ProgramLifecycleService programLifecycleService;

  @BeforeClass
  public static void setup() {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    capabilityManager = getInjector().getInstance(CapabilityManager.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
    CConfiguration cConfiguration = getInjector().getInstance(CConfiguration.class);
    File file = new File(cConfiguration.get(Constants.Capability.DATA_DIR));
    file.mkdir();
    file.deleteOnExit();
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testGetAppsWithCapability() throws Exception {
    //Deploy application with capability
    Class<AppWithWorkflow> appWithWorkflowClass = AppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapabilities = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appWithWorkflowClass, appNameWithCapabilities);

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertNull(declaredAnnotation1);
    String appNameWithoutCapability = appNoCapabilityClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appNoCapabilityClass, appNameWithoutCapability);

    //verify that list applications return the application tagged with capability only
    for (String capability : declaredAnnotation.capabilities()) {
      EntityResult<ApplicationId> appsForCapability = capabilityManager
        .getApplications(NamespaceId.DEFAULT, capability, null, 0, 10);
      Set<ApplicationId> applicationIds = new HashSet<>(appsForCapability.getEntities());
      List<ApplicationDetail> appsReturned = new ArrayList<>(
        applicationLifecycleService.getAppDetails(applicationIds).values());
      appsReturned.
        forEach(
          applicationDetail -> Assert
            .assertEquals(appNameWithCapabilities, applicationDetail.getArtifact().getName()));
    }

    //delete the app and verify nothing is returned.
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapabilities, TEST_VERSION));
    for (String capability : declaredAnnotation.capabilities()) {
      Set<ApplicationId> applicationIds = new HashSet<>(capabilityManager
                                                          .getApplications(NamespaceId.DEFAULT, capability, null, 0, 10)
                                                          .getEntities());
      List<ApplicationDetail> appsReturned = new ArrayList<>(
        applicationLifecycleService.getAppDetails(applicationIds).values());
      Assert.assertTrue(appsReturned.isEmpty());
    }
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithoutCapability, TEST_VERSION));
  }

  @Test
  public void testGetAppsForCapabilityPagination() throws Exception {
    //Deploy two applications with capability
    Class<AppWithWorkflow> appWithWorkflowClass = AppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapability1 = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appWithWorkflowClass, appNameWithCapability1);
    String appNameWithCapability2 = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appWithWorkflowClass, appNameWithCapability2);

    //search with offset and limit
    String capability = declaredAnnotation.capabilities()[0];
    EntityResult<ApplicationId> appsForCapability = capabilityManager
      .getApplications(NamespaceId.DEFAULT, capability, null, 0, 1);
    Assert.assertEquals(1, appsForCapability.getEntities().size());
    //next search with pagination
    EntityResult<ApplicationId> appsForCapabilityNext = capabilityManager
      .getApplications(NamespaceId.DEFAULT, capability, appsForCapability.getCursor(), 1, 1);
    Assert.assertEquals(1, appsForCapabilityNext.getEntities().size());
    appsForCapabilityNext = capabilityManager
      .getApplications(NamespaceId.DEFAULT, capability, appsForCapability.getCursor(), 2, 1);
    Assert.assertEquals(0, appsForCapabilityNext.getEntities().size());

    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability1, TEST_VERSION));
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability2, TEST_VERSION));
  }

  @Test
  public void testCapabilityRefresh() throws Exception {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, version);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    //check that app is not available
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());

    //enable the capability
    CapabilityConfig config = getTestConfig();
    capabilityManager.apply(Collections.singletonList(config));
    //app should show up and program should have run
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    Assert.assertTrue(capabilityManager.isCapabilityPresent(config.getCapability()));
    Assert.assertEquals(CapabilityStatus.ENABLED, capabilityManager.getStatus(config.getCapability()));

    //disable capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig disabledConfig = changeConfigAction(config, CapabilityActionType.DISABLE);
    capabilityManager.apply(Collections.singletonList(disabledConfig));
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    Assert.assertTrue(capabilityManager.isCapabilityPresent(config.getCapability()));
    Assert.assertEquals(CapabilityStatus.DISABLED, capabilityManager.getStatus(config.getCapability()));
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //delete capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig deletedConfig = changeConfigAction(config, CapabilityActionType.DELETE);
    capabilityManager.apply(Collections.singletonList(deletedConfig));
    Assert.assertFalse(capabilityManager.isCapabilityPresent(config.getCapability()));
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
  }

  @Test
  public void testPluginCapabilityRefresh() throws Exception {
    String appName = SleepingWorkflowApp.NAME;
    Class<SleepingWorkflowApp> appClass = SleepingWorkflowApp.class;
    String version = "1.0.0";
    String namespace = "default";
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    //enable a capability with no system apps and programs
    CapabilityConfig enabledConfig = new CapabilityConfig("Enable healthcare", CapabilityActionType.ENABLE.name(),
                                                          "healthcare", Collections.emptyList(),
                                                          Collections.emptyList());
    capabilityManager.apply(Collections.singletonList(enabledConfig));
    String capability = enabledConfig.getCapability();
    Assert.assertTrue(capabilityManager.isCapabilityPresent(capability));
    Assert.assertEquals(CapabilityStatus.ENABLED, capabilityManager.getStatus(capability));

    //deploy an app with this capability and start a workflow
    ApplicationId applicationId = new ApplicationId(namespace, appName);
    Id.Artifact artifactId = Id.Artifact
      .from(new Id.Namespace(namespace), appName, version);
    ApplicationWithPrograms applicationWithPrograms = applicationLifecycleService
      .deployApp(new NamespaceId(namespace), appName, null, artifactId, null, op -> {
      });
    Iterable<ProgramDescriptor> programs = applicationWithPrograms.getPrograms();
    for (ProgramDescriptor program : programs) {
      programLifecycleService.start(program.getProgramId(), new HashMap<>(), false);
    }

    ProgramId programId = new ProgramId(applicationId, ProgramType.WORKFLOW,
                                        SleepingWorkflowApp.SleepWorkflow.class.getSimpleName());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //disable the capability -  the program that was started should stop
    CapabilityConfig disabledConfig = new CapabilityConfig("Enable healthcare", CapabilityActionType.DISABLE.name(),
                                                           "healthcare", Collections.emptyList(),
                                                           Collections.emptyList());
    capabilityManager.apply(Collections.singletonList(disabledConfig));
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    Assert.assertTrue(capabilityManager.isCapabilityPresent(capability));
    Assert.assertEquals(CapabilityStatus.DISABLED, capabilityManager.getStatus(capability));

    //delete the capability
    CapabilityConfig deleteConfig = new CapabilityConfig("Enable healthcare", CapabilityActionType.DELETE.name(),
                                                         "healthcare", Collections.emptyList(),
                                                         Collections.emptyList());
    capabilityManager.apply(Collections.singletonList(deleteConfig));
    Assert.assertFalse(capabilityManager.isCapabilityPresent(capability));
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
  }

  private CapabilityConfig changeConfigAction(CapabilityConfig original, CapabilityActionType type) {
    return new CapabilityConfig(original.getLabel(), type.name(), original.getCapability(), original.getApplications(),
                                original.getPrograms());
  }

  private CapabilityConfig getTestConfig() {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    String label = "Enable capability";
    String capability = "test";
    CapabilityActionType type = CapabilityActionType.ENABLE;
    ArtifactSummary artifactSummary = new ArtifactSummary(appName, version, ArtifactScope.SYSTEM);
    SystemApplication application = new SystemApplication(namespace, appName, version, artifactSummary, null);
    SystemProgram program = new SystemProgram(namespace, appName, ProgramType.SERVICE.name(),
                                              programName, version, null);
    return new CapabilityConfig(label, type.name(), capability, Collections.singletonList(application),
                                Collections.singletonList(program));
  }

  private void deployArtifactAndApp(Class<?> applicationClass, String appName) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appName, TEST_VERSION);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    deployTestArtifact(NamespaceId.DEFAULT.getNamespace(), appName, TEST_VERSION, applicationClass);
    //deploy app
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appName, TEST_VERSION, artifactId,
                 null, programId -> {
        });
  }

  void deployTestArtifact(String namespace, String appName, String version, Class<?> appclass) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(namespace), appName, version);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appclass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
  }
}

