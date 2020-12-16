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
import io.cdap.cdap.CapabilityAppWithWorkflow;
import io.cdap.cdap.CapabilitySleepingWorkflowApp;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.SystemProgramManagementService;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CapabilityApplierTest extends AppFabricTestBase {

  private static ApplicationLifecycleService applicationLifecycleService;
  private static ArtifactRepository artifactRepository;
  private static CapabilityApplier capabilityApplier;
  private static CapabilityReader capabilityReader;
  private static LocationFactory locationFactory;
  private static ProgramLifecycleService programLifecycleService;
  public static final String TEST_VERSION = "1.0.0";

  @BeforeClass
  public static void setup() {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    capabilityApplier = getInjector().getInstance(CapabilityApplier.class);
    capabilityReader = getInjector().getInstance(CapabilityReader.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
    getInjector().getInstance(SystemProgramManagementService.class).start();
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
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
    capabilityApplier.apply(Collections.singletonList(config));
    //app should show up and program should have run
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    String capability = config.getCapability();
    Assert.assertTrue(capabilityReader.isEnabled(capability));

    //disable capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    capabilityApplier.apply(Collections.singletonList(disabledConfig));
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    Assert.assertFalse(capabilityReader.isEnabled(capability));
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //delete capability. Program should stop, status should be disabled and app should still be present.
    capabilityApplier.apply(Collections.emptyList());
    Assert.assertNull(capabilityReader.getStatus(capability));
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace("system"), appName, version));
  }

  @Test
  public void testPluginCapabilityRefresh() throws Exception {
    String appName = CapabilitySleepingWorkflowApp.NAME;
    Class<CapabilitySleepingWorkflowApp> appClass = CapabilitySleepingWorkflowApp.class;
    String version = "1.0.0";
    String namespace = "default";
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    //enable a capability with no system apps and programs
    CapabilityConfig enabledConfig = new CapabilityConfig("Enable healthcare", CapabilityStatus.ENABLED.name(),
                                                          "healthcare", Collections.emptyList(),
                                                          Collections.emptyList());
    capabilityApplier.apply(Collections.singletonList(enabledConfig));
    String capability = enabledConfig.getCapability();
    Assert.assertTrue(capabilityReader.isEnabled(capability));

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
                                        CapabilitySleepingWorkflowApp.SleepWorkflow.class.getSimpleName());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //disable the capability -  the program that was started should stop
    CapabilityConfig disabledConfig = new CapabilityConfig("Disable healthcare", CapabilityStatus.DISABLED.name(),
                                                           "healthcare", Collections.emptyList(),
                                                           Collections.emptyList());
    capabilityApplier.apply(Collections.singletonList(disabledConfig));
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    Assert.assertFalse(capabilityReader.isEnabled(capability));
    //try starting programs
    for (ProgramDescriptor program : programs) {
      try {
        programLifecycleService.start(program.getProgramId(), new HashMap<>(), false);
      } catch (CapabilityNotAvailableException ex) {
        //expecting exception
      }
    }
    Assert.assertTrue(getAppList(namespace).isEmpty());

    capabilityApplier.apply(Collections.emptyList());
    Assert.assertNull(capabilityReader.getStatus(capability));
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(namespace), appName, version));
  }

  @Test
  public void testIsApplicationDisabled() throws Exception {
    //Deploy application with capability
    Class<CapabilityAppWithWorkflow> appWithWorkflowClass = CapabilityAppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    try {
      deployArtifactAndApp(appWithWorkflowClass, appNameWithCapability);
      Assert.fail("Expecting exception");
    } catch (CapabilityNotAvailableException ex) {
      //expected
    }

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertNull(declaredAnnotation1);
    String appNameWithOutCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appNoCapabilityClass, appNameWithOutCapability);

    //applications with no capabilities should not be disabled
    capabilityReader
      .ensureApplicationEnabled(NamespaceId.DEFAULT.getNamespace(), appNameWithOutCapability);

    //enable the capabilities
    List<CapabilityConfig> capabilityConfigs = Arrays.stream(declaredAnnotation.capabilities())
      .map(capability -> new CapabilityConfig("Test capability", CapabilityStatus.ENABLED.name(), capability,
                                              Collections.emptyList(), Collections.emptyList()))
      .collect(Collectors.toList());
    capabilityApplier.apply(capabilityConfigs);

    //deployment should go through now
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appNameWithCapability, TEST_VERSION);
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appNameWithCapability, TEST_VERSION, artifactId,
                 null, programId -> {
        });
    capabilityReader
      .ensureApplicationEnabled(NamespaceId.DEFAULT.getNamespace(), appNameWithCapability);

    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability, TEST_VERSION));
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithOutCapability, TEST_VERSION));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()),
                                              appNameWithCapability, TEST_VERSION));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()),
                                              appNameWithOutCapability, TEST_VERSION));
  }

  private CapabilityConfig changeConfigStatus(CapabilityConfig original, CapabilityStatus type) {
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
    ArtifactSummary artifactSummary = new ArtifactSummary(appName, version, ArtifactScope.SYSTEM);
    SystemApplication application = new SystemApplication(namespace, appName, version, artifactSummary, null);
    SystemProgram program = new SystemProgram(namespace, appName, ProgramType.SERVICE.name(),
                                              programName, version, null);
    return new CapabilityConfig(label, CapabilityStatus.ENABLED.name(), capability,
                                Collections.singletonList(application), Collections.singletonList(program));
  }

  private void deployArtifactAndApp(Class<?> applicationClass, String appName) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appName, TEST_VERSION);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
    //deploy app
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appName, TEST_VERSION, artifactId,
                 null, programId -> {
        });
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

