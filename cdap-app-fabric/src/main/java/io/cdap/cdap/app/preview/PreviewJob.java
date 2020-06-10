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

package io.cdap.cdap.app.preview;

import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;

import java.util.Objects;

/**
 * Class representing Preview Job. Preview Job consist of Application
 * ID which identifies the Preview and corresponding application request.
 * Two preview jobs are considered equal if both have same application id.
 */
public class PreviewJob {
  private final ApplicationId applicationId;
  private final AppRequest appRequest;

  public PreviewJob(ApplicationId applicationId, AppRequest appRequest) {
    this.applicationId = applicationId;
    this.appRequest = appRequest;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public AppRequest getAppRequest() {
    return appRequest;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PreviewJob that = (PreviewJob) o;
    return applicationId.equals(that.applicationId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(applicationId);
  }
}
