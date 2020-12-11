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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.SystemProgramManagementService;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.spi.metadata.SearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class with helpful methods for managing capabilities
 */
public class CapabilityManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(CapabilityManager.class);
  private static final Gson GSON = new Gson();
  private static final int RETRY_LIMIT = 5;
  private static final int RETRY_DELAY = 5;
  private static final String CAPABILITY = "capability:%s";
  private static final String APPLICATION = "application";
  private static final ProgramTerminator NOOP_PROGRAM_TERMINATOR = programId -> {
    // no-op
  };
  private final ConcurrentMap<String, CapabilityStatus> capabilityStatusMap;
  private final SystemProgramManagementService systemProgramManagementService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final NamespaceAdmin namespaceAdmin;
  private final MetadataSearchClient metadataSearchClient;
  private final CConfiguration cConf;
  private final Lock readLock;
  private final Lock writeLock;

  @Inject
  CapabilityManager(CConfiguration cConf, SystemProgramManagementService systemProgramManagementService,
                    ApplicationLifecycleService applicationLifecycleService, MetadataSearchClient metadataSearchClient,
                    NamespaceAdmin namespaceAdmin, ProgramLifecycleService programLifecycleService) {
    this.capabilityStatusMap = new ConcurrentHashMap<>();
    this.systemProgramManagementService = systemProgramManagementService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.metadataSearchClient = metadataSearchClient;
    this.namespaceAdmin = namespaceAdmin;
    this.cConf = cConf;
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    readLock = rwLock.readLock();
    writeLock = rwLock.writeLock();
  }

  @Override
  protected void startUp() {
    LOG.info("Starting {}", this.getClass().getSimpleName());
    //refresh the status of all capabilities
    systemProgramManagementService.start();
    refreshCapabilities();
  }

  @Override
  protected void shutDown() {
    systemProgramManagementService.stopAndWait();
    LOG.info("Stopping {}", this.getClass().getSimpleName());
  }

  /**
   * Return the current status for a capability. If capability is not present, throws {@link IllegalArgumentException}
   *
   * @param capability
   * @return {@link CapabilityStatus}
   */
  public CapabilityStatus getStatus(String capability) {
    if (!capabilityStatusMap.containsKey(capability)) {
      throw new IllegalArgumentException("Capability not found");
    }
    return capabilityStatusMap.get(capability);
  }

  /**
   * Returns boolean indicating whether the capability is present in the system
   *
   * @param capability
   * @return boolean indicating presence of capability
   */
  public boolean isCapabilityPresent(String capability) {
    return capabilityStatusMap.containsKey(capability);
  }

  /**
   * Applies the given capability configurations
   *
   * @param
   */
  public void apply(List<CapabilityConfig> capabilityConfigs) {
    try {
      writeLock.lock();
      for (CapabilityConfig capabilityConfig : capabilityConfigs) {
        String capability = capabilityConfig.getCapability();
        File file = new File(cConf.get(Constants.Capability.DATA_DIR), capability + ".json");
        try (FileWriter writer = new FileWriter(file)) {
          GSON.toJson(capabilityConfig, writer);
        } catch (IOException ex) {
          LOG.error("Saving capability {} config to file {} failed with {} ", capability, file, ex);
        }
      }
    } finally {
      writeLock.unlock();
    }
    refreshCapabilities();
  }

  /**
   * Apply capabilities based on the configuration files on disk
   */
  private void refreshCapabilities() {
    List<CapabilityConfig> allCapabilityConfigs = new ArrayList<>();
    File dataDir = new File(cConf.get(Constants.Capability.DATA_DIR));
    try {
      readLock.lock();
      for (File configFile : DirUtils.listFiles(dataDir)) {
        try (Reader reader = new FileReader(configFile)) {
          allCapabilityConfigs.add(GSON.fromJson(reader, CapabilityConfig.class));
        } catch (IOException ex) {
          LOG.error("Reading capability config file {} failed with {}", configFile, ex);
        }
      }
    } finally {
      readLock.unlock();
    }
    refreshCapabilities(allCapabilityConfigs);
  }

  private void refreshCapabilities(List<CapabilityConfig> capabilityConfigs) {
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
    enabledCapabilities.forEach(this::enableCapability);
  }

  /**
   * Returns the list of applications that are having metadata tagged with the capability
   *
   * @param namespace  Namespace for which applications should be listed
   * @param capability Capability by which to filter
   * @param cursor     Optional cursor from a previous response
   * @param offset     Offset from where to start
   * @param limit      Limit of records to fetch
   * @return
   * @throws IOException - Exception from meta data search if any
   */
  public EntityResult<ApplicationId> getApplications(NamespaceId namespace, String capability, @Nullable String cursor,
                                                     int offset, int limit) throws IOException {
    String capabilityTag = String.format(CAPABILITY, capability);
    SearchRequest searchRequest = SearchRequest.of(capabilityTag)
      .addNamespace(namespace.getNamespace())
      .addType(APPLICATION)
      .setScope(MetadataScope.SYSTEM)
      .setCursor(cursor)
      .setOffset(offset)
      .setLimit(limit)
      .build();
    MetadataSearchResponse searchResponse = metadataSearchClient.search(searchRequest);
    Set<ApplicationId> applicationIds = searchResponse.getResults().stream()
      .map(MetadataSearchResultRecord::getMetadataEntity)
      .map(this::getApplicationId)
      .collect(Collectors.toSet());
    return new EntityResult<>(applicationIds, getCursorResponse(searchResponse),
                              searchResponse.getOffset(), searchResponse.getLimit(),
                              searchResponse.getTotal());
  }

  private ProgramId getProgramId(SystemProgram program) {
    ApplicationId applicationId = new ApplicationId(program.getNamespace(), program.getApplication(),
                                                    program.getVersion());
    return new ProgramId(applicationId, ProgramType.valueOf(program.getType()), program.getName());
  }

  @Nullable
  private String getCursorResponse(MetadataSearchResponse searchResponse) {
    List<String> cursors = searchResponse.getCursors();
    if (cursors == null || cursors.isEmpty()) {
      return null;
    }
    return cursors.get(0);
  }

  private ApplicationId getApplicationId(MetadataEntity metadataEntity) {
    return new ApplicationId(metadataEntity.getValue(MetadataEntity.NAMESPACE),
                             metadataEntity.getValue(MetadataEntity.APPLICATION),
                             metadataEntity.getValue(MetadataEntity.VERSION));
  }

  private void enableCapability(String capability) {
    capabilityStatusMap.put(capability, CapabilityStatus.ENABLED);
    LOG.debug("Capability {} enabled.", capability);
  }

  private void disableCapability(String capability) {
    //mark as disabled to prevent further runs
    capabilityStatusMap.put(capability, CapabilityStatus.DISABLED);
    //stop pipelines
    try {
      stopPipelines(capability);
    } catch (Exception ex) {
      LOG.error("Stopping pipelines failed for capability {} with exception {}", capability, ex);
    }
    //programs(services) will be stopped by SystemProgramManagementService
    LOG.debug("Capability {} disabled.", capability);
  }

  private void deleteCapability(CapabilityConfig capabilityConfig) {
    String capability = capabilityConfig.getCapability();
    if (capabilityStatusMap.get(capability) == CapabilityStatus.ENABLED) {
      LOG.error("Deleting capability {} failed. Capability should be disabled before deleting.", capability);
      return;
    }
    //delete pipelines
    try {
      deletePipelines(capability);
    } catch (Exception ex) {
      LOG.error("Deleting pipelines failed for capability {} with exception {}", capability, ex);
    }
    //delete applications
    for (SystemApplication application : capabilityConfig.getApplications()) {
      ApplicationId applicationId = new ApplicationId(application.getNamespace(), application.getName(),
                                                      application.getVersion());
      try {
        applicationLifecycleService.removeApplication(applicationId);
      } catch (Exception exception) {
        LOG.error("Deleting application {} failed with exception {}", applicationId, exception);
      }
    }
    capabilityStatusMap.remove(capability);
    LOG.debug("Capability {} deleted.", capability);
  }

  private void deletePipelines(String capability) throws Exception {
    doForAllApps(capability, this::deleteAppWithRetry);
  }

  private void stopPipelines(String capability) throws Exception {
    doForAllApps(capability, this::stopPrograms);
  }

  private void deployAllApps(String capability, List<SystemApplication> applications) {
    if (applications.isEmpty()) {
      LOG.debug("Capability {} do not have apps associated with it", capability);
      return;
    }
    for (SystemApplication application : applications) {
      doWithRetry(application, this::deployApp);
    }
  }

  private void deployApp(SystemApplication application) {
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
  private void doForAllApps(String capability, Consumer<ApplicationId> consumer) throws Exception {
    for (NamespaceMeta namespaceMeta : namespaceAdmin.list()) {
      int offset = 0;
      int limit = 100;
      NamespaceId namespaceId = namespaceMeta.getNamespaceId();
      EntityResult<ApplicationId> results = getApplications(namespaceId, capability, null,
                                                            offset, limit);
      while (results.getEntities().size() > 0) {
        //call consumer for each entity
        results.getEntities().forEach(consumer);
        if (results.getEntities().size() < limit) {
          break;
        }
        offset += limit;
        results = getApplications(namespaceId, capability, results.getCursor(), offset, limit);
      }
    }
  }

  private void stopPrograms(ApplicationId applicationId) {
    try {
      List<ProgramRecord> programs = applicationLifecycleService.getAppDetail(applicationId).getPrograms();
      for (ProgramRecord programRecord : programs) {
        ProgramId programId = new ProgramId(applicationId, programRecord.getType(), programRecord.getName());
        doWithRetry(programId, this::stopProgram);
      }
    } catch (Exception ex) {
      LOG.error("Stopping programs for application {} failed with {}", applicationId, ex);
    }
  }

  private void stopProgram(ProgramId programId) {
    try {
      programLifecycleService.stop(programId);
    } catch (Exception ex) {
      throw new RetryableException(ex);
    }
  }

  private void deleteAppWithRetry(ApplicationId applicationId) {
    doWithRetry(applicationId, this::deleteApp);
  }

  private void deleteApp(ApplicationId applicationId) {
    try {
      applicationLifecycleService.removeApplication(applicationId);
    } catch (Exception ex) {
      throw new RetryableException(ex);
    }
  }

  private <T> void doWithRetry(T argument, Consumer<T> consumer) {
    Retries.callWithRetries(() -> {
      consumer.accept(argument);
      return null;
    }, RetryStrategies.limit(RETRY_LIMIT, RetryStrategies.fixDelay(RETRY_DELAY, TimeUnit.SECONDS)));
  }
}
