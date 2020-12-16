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

package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.internal.capability.CapabilityConfig;
import io.cdap.cdap.internal.capability.CapabilityOperationRecord;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.capability.CapabilityStatus;
import io.cdap.cdap.internal.capability.CapabilityStatusRecord;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * NoopCapabilityReader for preview. Does not do any capability checking.
 */
public class NoopCapabilityReader implements CapabilityReader {
  @Override
  public CapabilityStatus getStatus(String capability) {
    return CapabilityStatus.ENABLED;
  }

  @Override
  public boolean isEnabled(String capability) {
    return true;
  }

  @Override
  public CapabilityConfig getConfig(String capability) {
    return null;
  }

  @Override
  public List<CapabilityStatusRecord> getAllCapabilities() {
    return Collections.emptyList();
  }

  @Override
  public List<CapabilityOperationRecord> getCapabilityOperations() {
    return Collections.emptyList();
  }

  @Override
  public EntityResult<ApplicationId> getApplications(NamespaceId namespace, String capability, @Nullable String cursor,
                                                     int offset, int limit) {
    return new EntityResult<>(Collections.emptyList(), null, 0, 0, 0);
  }

  @Override
  public void ensureApplicationEnabled(String namespace, String appNameWithCapability) {
    //no-op
  }
}
