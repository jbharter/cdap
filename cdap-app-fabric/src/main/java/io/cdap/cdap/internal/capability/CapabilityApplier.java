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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
   * @param
   */
  public void apply(List<CapabilityConfig> capabilityConfigs) throws Exception {
    refreshCapabilities(new ArrayList<>(capabilityConfigs));
  }

  private void refreshCapabilities(List<CapabilityConfig> capabilityConfigs) throws Exception {
    //collect all programs to be enabled
    Map<ProgramId, Arguments> enabledPrograms = new HashMap<>();
    Set<String> enabledCapabilities = new HashSet<>();
    for (CapabilityConfig config : capabilityConfigs) {
      String capability = config.getCapability();
      LOG.debug("Applying {} action for capability {}", config.getType(), capability);
      switch (config.getType()) {
        case ENABLE:
          deployAllApps(capability, config.getApplications());
          config.getPrograms().forEach(systemProgram -> enabledPrograms
            .put(getProgramId(systemProgram), new BasicArguments(systemProgram.getArgs())));
          enabledCapabilities.add(capability);
          break;
        case DISABLE:
          disableCapability(capability);
          break;
        case DELETE:
          deleteCapability(config);
          break;
        default:
          LOG.error("Unknown capability action {} ", config.getType());
          break;
      }
    }
    systemProgramManagementService.setProgramsEnabled(enabledPrograms);
    enableAllCapabilities(enabledCapabilities);
  }

  private ProgramId getProgramId(SystemProgram program) {
    ApplicationId applicationId = new ApplicationId(program.getNamespace(), program.getApplication(),
                                                    program.getVersion());
    return new ProgramId(applicationId, ProgramType.valueOf(program.getType().toUpperCase()), program.getName());
  }

  private void enableAllCapabilities(Set<String> enabledCapabilities) throws IOException {
    for (String capability : enabledCapabilities) {
      capabilityWriter.addOrUpdateCapability(capability, CapabilityStatus.ENABLED);
      LOG.debug("Capability {} enabled.", capability);
    }
  }

  private void disableCapability(String capability) throws IOException {
    //mark as disabled to prevent further runs
    capabilityWriter.addOrUpdateCapability(capability, CapabilityStatus.DISABLED);
    //stop programs
    try {
      stopAllPrograms(capability);
    } catch (Exception ex) {
      LOG.error("Stopping programs failed for capability {} with exception.", capability, ex);
    }
    //programs(services) will be stopped by SystemProgramManagementService
    LOG.debug("Capability {} disabled.", capability);
  }

  private void deleteCapability(CapabilityConfig capabilityConfig) throws IOException {
    String capability = capabilityConfig.getCapability();
    if (capabilityReader.isEnabled(capability)) {
      LOG.error("Deleting capability {} failed. Capability should be disabled before deleting.", capability);
      return;
    }
    //delete programs
    try {
      deleteAllApps(capability);
    } catch (Exception ex) {
      LOG.error("Deleting programs failed for capability {} with exception.", capability, ex);
    }
    //delete applications
    for (SystemApplication application : capabilityConfig.getApplications()) {
      ApplicationId applicationId = new ApplicationId(application.getNamespace(), application.getName(),
                                                      application.getVersion());
      try {
        applicationLifecycleService.removeApplication(applicationId);
      } catch (Exception exception) {
        LOG.error("Deleting application {} failed with exception.", applicationId, exception);
      }
    }
    capabilityWriter.deleteCapability(capability);
    LOG.debug("Capability {} deleted.", capability);
  }

  private void deleteAllApps(String capability) throws Exception {
    doForAllApps(capability, this::deleteAppWithRetry);
  }

  private void stopAllPrograms(String capability) throws Exception {
    doForAllApps(capability, this::stopAllPrograms);
  }

  private void deployAllApps(String capability, List<SystemApplication> applications) throws Exception {
    if (applications.isEmpty()) {
      LOG.debug("Capability {} do not have apps associated with it", capability);
      return;
    }
    for (SystemApplication application : applications) {
      doWithRetry(application, this::deployApp);
    }
  }

  private void deployApp(SystemApplication application) throws Exception {
    String version = application.getVersion() == null ? ApplicationId.DEFAULT_VERSION : application.getVersion();
    ApplicationId applicationId = new ApplicationId(application.getNamespace(), application.getName(), version);
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
  private void doForAllApps(String capability, CheckedConsumer<ApplicationId> consumer) throws Exception {
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

  private void stopAllPrograms(ApplicationId applicationId) {
    try {
      doWithRetry(applicationId, this::stopPrograms);
    } catch (Exception ex) {
      LOG.error("Stopping programs for application {} failed with {}", applicationId, ex);
    }
  }

  private void stopPrograms(ApplicationId applicationId) throws Exception {
    try {
      programLifecycleService.stopAll(applicationId);
    } catch (Exception ex) {
      checkForRetry(ex);
      throw new RetryableException(ex);
    }
  }

  private void deleteAppWithRetry(ApplicationId applicationId) throws Exception {
    doWithRetry(applicationId, this::deleteApp);
  }

  private void deleteApp(ApplicationId applicationId) throws Exception {
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
   * @param <T>
   */
  @FunctionalInterface
  public interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }
}
