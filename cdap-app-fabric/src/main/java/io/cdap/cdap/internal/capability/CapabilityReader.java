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

import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * CapabilityReader interface with methods based on current capability status
 */
public interface CapabilityReader {

  /**
   * Returns the status of capability.
   *
   * @param capability
   * @return
   * @throws IOException
   */
  CapabilityStatus getStatus(String capability) throws IOException;

  /**
   * Return boolean indicating whether the capability is enabled
   *
   * @param capability
   * @return
   * @throws IOException
   */
  boolean isEnabled(String capability) throws IOException;

  /**
   * Return applications that is associated with the capability
   *
   * @param namespace
   * @param capability
   * @param cursor
   * @param offset
   * @param limit
   * @return
   * @throws IOException
   */
  EntityResult<ApplicationId> getApplications(NamespaceId namespace, String capability, @Nullable String cursor,
                                              int offset, int limit) throws IOException;

}
