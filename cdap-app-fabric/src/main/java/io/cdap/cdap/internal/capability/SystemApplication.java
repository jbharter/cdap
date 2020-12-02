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

import io.cdap.cdap.api.artifact.ArtifactSummary;
import org.json.JSONObject;

import javax.annotation.Nullable;

/**
 * Class with fields for requesting an Application deployment for a capability
 */
public class SystemApplication {

  private final String namespace;
  private final String name;
  private final String version;
  private final ArtifactSummary artifact;
  private final JSONObject config;

  public SystemApplication(String namespace, String applicationName, @Nullable String version,
                           ArtifactSummary artifact, @Nullable JSONObject config) {
    this.namespace = namespace;
    this.name = applicationName;
    this.version = version;
    this.artifact = artifact;
    this.config = config;
  }

  /**
   * @return namespace {@link String}
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return name {@link String}
   */
  public String getName() {
    return name;
  }

  /**
   * @return version {@link String}, could be null
   */
  @Nullable
  public String getVersion() {
    return version;
  }

  /**
   * @return {@link ArtifactSummary}
   */
  public ArtifactSummary getArtifact() {
    return artifact;
  }

  /**
   * @return {@link JSONObject} configuration
   */
  @Nullable
  public JSONObject getConfig() {
    return config;
  }
}
