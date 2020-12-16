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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.SystemProgramManagementService;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class that applies capabilities
 */
public class CapabilityApplier {

  private static final Logger LOG = LoggerFactory.getLogger(CapabilityApplier.class);
  private static final Gson GSON = new Gson();
  private static final int RETRY_LIMIT = 5;
  private static final int RETRY_DELAY = 5;
  private static final ProgramTerminator NOOP_PROGRAM_TERMINATOR = programId -> {
    // no-op
  };
  private final SystemProgramManagementService systemProgramManagementService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final NamespaceAdmin namespaceAdmin;
  private final CapabilityReader capabilityReader;
  private final CapabilityWriter capabilityWriter;

  @Inject
  CapabilityApplier(CConfiguration cConf, SystemProgramManagementService systemProgramManagementService,
                    ApplicationLifecycleService applicationLifecycleService, NamespaceAdmin namespaceAdmin,
                    ProgramLifecycleService programLifecycleService, CapabilityReader capabilityChecker,
                    CapabilityWriter capabilityWriter) {
    this.systemProgramManagementService = systemProgramManagementService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.capabilityReader = capabilityChecker;
    this.capabilityWriter = capabilityWriter;
    this.namespaceAdmin = namespaceAdmin;
  }

  /**
   * Applies the given capability configurations
   *
   * @param capabilityConfigs
   */
  public void apply(List<CapabilityConfig> capabilityConfigs) throws Exception {
    List<CapabilityConfig> newConfigs = new ArrayList<>(capabilityConfigs);
    Set<CapabilityConfig> enableSet = new HashSet<>();
    Set<CapabilityConfig> disableSet = new HashSet<>();
    Set<CapabilityConfig> deleteSet = new HashSet<>();
    Map<String, CapabilityStatusRecord> currentCapabilities = capabilityReader.getAllCapabilities().stream().collect(
      Collectors.toMap(CapabilityStatusRecord::getCapability, capabilityStatusRecord -> capabilityStatusRecord));
    Map<String, CapabilityOperationRecord> currentOperations = capabilityReader.getCapabilityOperations().stream()
      .collect(Collectors.toMap(CapabilityOperationRecord::getCapability,
                                capabilityOperationRecord -> capabilityOperationRecord));
    for (CapabilityConfig newConfig : newConfigs) {
      String capability = newConfig.getCapability();
      if (currentOperations.containsKey(capability)) {
        LOG.debug("Capability {} config for status {} skipped because there is already an operation {} in progress.",
                  capability, newConfig.getStatus(), currentOperations.get(capability).getActionType());
        continue;
      }
      switch (newConfig.getStatus()) {
        case ENABLED:
          enableSet.add(newConfig);
          break;
        case DISABLED:
          disableSet.add(newConfig);
          break;
        default:
          break;
      }
      currentCapabilities.remove(capability);
    }
    //add all unfinished operations to retry
    for (CapabilityOperationRecord operationRecord : currentOperations.values()) {
      switch (operationRecord.getActionType()) {
        case ENABLE:
          enableSet.add(operationRecord.getCapabilityConfig());
          break;
        case DISABLE:
          disableSet.add(operationRecord.getCapabilityConfig());
          break;
        case DELETE:
          deleteSet.add(operationRecord.getCapabilityConfig());
          break;
        default:
          break;
      }
      currentCapabilities.remove(operationRecord.getCapability());
    }
    // find the ones that are not being applied or retried - these should be removed
    deleteSet.addAll(currentCapabilities.values().stream()
                       .map(CapabilityStatusRecord::getCapabilityConfig).collect(Collectors.toSet()));
    enableCapabilities(enableSet);
    disableCapabilities(disableSet);
    deleteCapabilities(deleteSet);
  }

  private void enableCapabilities(Set<CapabilityConfig> enableSet) throws Exception {
    Map<ProgramId, Arguments> enabledPrograms = new HashMap<>();
    for (CapabilityConfig capabilityConfig : enableSet) {
      //collect the enabled programs
      capabilityConfig.getPrograms().forEach(systemProgram -> enabledPrograms
        .put(getProgramId(systemProgram), new BasicArguments(systemProgram.getArgs())));
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = capabilityReader.getConfig(capability);
      if (capabilityConfig.equals(existingConfig)) {
        continue;
      }
      capabilityWriter.addOrUpdateCapabilityOperation(capability, CapabilityAction.ENABLE, capabilityConfig);
      LOG.debug("Enabling capability {}", capability);
      //If already deployed, will be ignored
      deployAllSystemApps(capability, capabilityConfig.getApplications());
    }
    //start all programs
    systemProgramManagementService.setProgramsEnabled(enabledPrograms);
    //mark all as enabled
    for (CapabilityConfig capabilityConfig : enableSet) {
      String capability = capabilityConfig.getCapability();
      capabilityWriter
        .addOrUpdateCapability(capability, CapabilityStatus.ENABLED, capabilityConfig);
      capabilityWriter.deleteCapabilityOperation(capability);
      LOG.debug("Enabled capability {}", capability);
    }
  }

  private void disableCapabilities(Set<CapabilityConfig> disableSet) throws Exception {
    for (CapabilityConfig capabilityConfig : disableSet) {
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = capabilityReader.getConfig(capability);
      if (capabilityConfig.equals(existingConfig)) {
        continue;
      }
      capabilityWriter.addOrUpdateCapabilityOperation(capability, CapabilityAction.DISABLE, capabilityConfig);
      LOG.debug("Disabling capability {}", capability);
      capabilityWriter
        .addOrUpdateCapability(capabilityConfig.getCapability(), CapabilityStatus.DISABLED, capabilityConfig);
      //stop all the programs having capability metadata. Services will be stopped by SystemProgramManagementService
      stopAllProgramsWithMetadata(capability);
      capabilityWriter.deleteCapabilityOperation(capability);
      LOG.debug("Disabled capability {}", capability);
    }
  }

  private void deleteCapabilities(Set<CapabilityConfig> deleteSet) throws Exception {
    for (CapabilityConfig capabilityConfig : deleteSet) {
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = capabilityReader.getConfig(capability);
      //already deleted
      if (existingConfig == null) {
        continue;
      }
      capabilityWriter.addOrUpdateCapabilityOperation(capability, CapabilityAction.DELETE, capabilityConfig);
      LOG.debug("Deleting capability {}", capability);
      if (existingConfig.getStatus() == CapabilityStatus.ENABLED) {
        //stop all the programs having capability metadata.
        stopAllProgramsWithMetadata(capability);
      }
      //remove all applications having capability metadata.
      deleteAllAppsWithMetadata(capability);
      //remove deployments of system applications
      for (SystemApplication application : capabilityConfig.getApplications()) {
        ApplicationId applicationId = getApplicationId(application);
        deleteAppWithRetry(applicationId);
      }
      capabilityWriter.deleteCapability(capability);
      capabilityWriter.deleteCapabilityOperation(capability);
      LOG.debug("Deleted capability {}", capability);
    }
  }

  private ApplicationId getApplicationId(SystemApplication application) {
    String version = application.getVersion() == null ? ApplicationId.DEFAULT_VERSION : application.getVersion();
    return new ApplicationId(application.getNamespace(), application.getName(), version);
  }

  private ProgramId getProgramId(SystemProgram program) {
    ApplicationId applicationId = new ApplicationId(program.getNamespace(), program.getApplication(),
                                                    program.getVersion());
    return new ProgramId(applicationId, ProgramType.valueOf(program.getType().toUpperCase()), program.getName());
  }

  private void deleteAllAppsWithMetadata(String capability) throws Exception {
    doForAllAppsWithMetadata(capability, this::deleteAppWithRetry);
  }

  private void stopAllProgramsWithMetadata(String capability) throws Exception {
    doForAllAppsWithMetadata(capability, this::stopAllRunningProgramsForApp);
  }

  private void deployAllSystemApps(String capability, List<SystemApplication> applications) throws Exception {
    if (applications.isEmpty()) {
      LOG.debug("Capability {} do not have apps associated with it", capability);
      return;
    }
    for (SystemApplication application : applications) {
      doWithRetry(application, this::retryableDeployApp);
    }
  }

  private void retryableDeployApp(SystemApplication application) throws Exception {
    ApplicationId applicationId = getApplicationId(application);
    LOG.debug("Deploying app {}", applicationId);
    try {
      if (isAppDeployed(applicationId)) {
        //Already deployed.
        LOG.debug("Application {} is already deployed", applicationId);
        return;
      }
      String configString = application.getConfig() == null ? null : GSON.toJson(application.getConfig());
      applicationLifecycleService
        .deployApp(applicationId.getParent(), applicationId.getApplication(), applicationId.getVersion(),
                   application.getArtifact(), configString, NOOP_PROGRAM_TERMINATOR, null, null);
    } catch (Exception ex) {
      checkForRetry(ex);
      throw new RetryableException(ex);
    }
  }

  private boolean isAppDeployed(ApplicationId applicationId) throws Exception {
    try {
      applicationLifecycleService.getAppDetail(applicationId);
      return true;
    } catch (ApplicationNotFoundException exception) {
      return false;
    }
  }

  //Find all applications for capability and call consumer for each
  private void doForAllAppsWithMetadata(String capability, CheckedConsumer<ApplicationId> consumer) throws Exception {
    for (NamespaceMeta namespaceMeta : namespaceAdmin.list()) {
      int offset = 0;
      int limit = 100;
      NamespaceId namespaceId = namespaceMeta.getNamespaceId();
      EntityResult<ApplicationId> results = capabilityReader.getApplications(namespaceId, capability, null,
                                                                             offset, limit);
      while (!results.getEntities().isEmpty()) {
        //call consumer for each entity
        for (ApplicationId entity : results.getEntities()) {
          consumer.accept(entity);
        }
        offset += limit;
        results = capabilityReader.getApplications(namespaceId, capability, results.getCursor(), offset, limit);
      }
    }
  }

  private void stopAllRunningProgramsForApp(ApplicationId applicationId) {
    try {
      doWithRetry(applicationId, this::retryableStopRunningPrograms);
    } catch (Exception ex) {
      LOG.error("Stopping programs for application {} failed with {}", applicationId, ex);
    }
  }

  private void retryableStopRunningPrograms(ApplicationId applicationId) throws Exception {
    try {
      programLifecycleService.stopAll(applicationId);
    } catch (Exception ex) {
      checkForRetry(ex);
      throw new RetryableException(ex);
    }
  }

  private void deleteAppWithRetry(ApplicationId applicationId) throws Exception {
    doWithRetry(applicationId, this::retryableDeleteApp);
  }

  private void retryableDeleteApp(ApplicationId applicationId) throws Exception {
    try {
      applicationLifecycleService.removeApplication(applicationId);
    } catch (Exception ex) {
      checkForRetry(ex);
      throw new RetryableException(ex);
    }
  }

  private <T> void doWithRetry(T argument, CheckedConsumer<T> consumer) throws Exception {
    Retries.callWithRetries(() -> {
      consumer.accept(argument);
      return null;
    }, RetryStrategies.limit(RETRY_LIMIT, RetryStrategies.fixDelay(RETRY_DELAY, TimeUnit.SECONDS)));
  }

  private void checkForRetry(Exception exception) throws Exception {
    if (exception instanceof UnauthorizedException ||
      exception instanceof InvalidArtifactException ||
      exception instanceof ArtifactNotFoundException) {
      throw exception;
    }
  }

  /**
   * Consumer functional interface that can throw exception
   *
   * @param <T>
   */
  @FunctionalInterface
  public interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }
}
