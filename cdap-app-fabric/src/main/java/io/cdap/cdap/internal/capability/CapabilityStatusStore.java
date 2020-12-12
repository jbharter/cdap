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

import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * CapabilityStatusStore which takes care of reading , writing capability status and provides additional helpful methods
 */
public class CapabilityStatusStore implements CapabilityReader, CapabilityWriter {

  private static final String CAPABILITY = "capability:%s";
  private static final String APPLICATION = "application";
  private final MetadataSearchClient metadataSearchClient;
  private final TransactionRunner transactionRunner;

  @Inject
  CapabilityStatusStore(DiscoveryServiceClient discoveryClient, TransactionRunner transactionRunner) {
    this.metadataSearchClient = new MetadataSearchClient(discoveryClient);
    this.transactionRunner = transactionRunner;
  }

  /**
   * Return the current status for a capability. If capability is not present, throws {@link IllegalArgumentException}
   *
   * @param capability
   * @return {@link CapabilityStatus}
   */
  public CapabilityStatus getStatus(String capability) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> keyField = Collections
        .singleton(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      Collection<String> returnField = Collections.singleton(StoreDefinition.CapabilitiesStore.STATUS_FIELD);
      Optional<StructuredRow> result = capabilityTable.read(keyField, returnField);
      return result.map(structuredRow -> CapabilityStatus
        .valueOf(structuredRow.getString(StoreDefinition.CapabilitiesStore.STATUS_FIELD).toUpperCase())).orElse(null);
    }, IOException.class);
  }

  @Override
  public boolean isEnabled(String capability) throws IOException {
    return getStatus(capability) == CapabilityStatus.ENABLED;
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

  @Override
  public void addOrUpdateCapability(String capability, CapabilityStatus status) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.STATUS_FIELD, status.name().toLowerCase()));
      fields.add(Fields.longField(StoreDefinition.CapabilitiesStore.UPDATED_TIME_FIELD, System.currentTimeMillis()));
      capabilityTable.upsert(fields);
    }, IOException.class);
  }

  @Override
  public void deleteCapability(String capability) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      capabilityTable.delete(fields);
    }, IOException.class);
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
}
