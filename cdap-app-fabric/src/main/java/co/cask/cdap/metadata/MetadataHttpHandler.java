/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * HttpHandler for Metadata
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MetadataHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();
  private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() {
  }.getType();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() {
  }.getType();

  private static final Function<String, EntityTypeSimpleName> STRING_TO_TARGET_TYPE =
    input -> EntityTypeSimpleName.valueOf(input.toUpperCase());

  private final MetadataAdmin metadataAdmin;

  private static final Map<String, String> API_PART_TO_ENTITY_TYPE;

  // TODO(Rohit): Is there a better way to standardize this conversion of api parts to entities. Why do we have so many
  // different representation? ex EntityTypeSimpleName. Investigate and refactor.
  static {
    Map<String, String> map = new HashMap<>();
    map.put("namespaces", MetadataEntity.NAMESPACE);
    map.put("apps", MetadataEntity.APPLICATION);
    map.put("streams", MetadataEntity.STREAM);
    map.put("datasets", MetadataEntity.DATASET);
    map.put("versions", MetadataEntity.VERSION);
    map.put("artifacts", MetadataEntity.ARTIFACT);
    map.put("programs", MetadataEntity.PROGRAM);
    map.put("views", MetadataEntity.VIEW);
    API_PART_TO_ENTITY_TYPE = Collections.unmodifiableMap(map);
  }

  @Inject
  MetadataHttpHandler(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @GET
  @Path("/**/metadata/subparts")
  public void getSubparts(HttpRequest request, HttpResponder responder) throws NotFoundException, BadRequestException {
    //TODO(Rohit): Get the sub keys here and return that. This is the new API which we will support for browsing
  }

  @GET
  @Path("/**/metadata")
  public void getMetadata(HttpRequest request, HttpResponder responder,
                          @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getMetadata(metadataEntity, scope),
                                                          SET_METADATA_RECORD_TYPE));
  }

  @GET
  @Path("/**/metadata/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/properties");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getProperties(metadataEntity, scope)));
  }

  @GET
  @Path("/**/metadata/tags")
  public void getTags(HttpRequest request, HttpResponder responder,
                      @QueryParam("scope") String scope) throws NotFoundException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/tags");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getTags(metadataEntity, scope)));
  }

  @POST
  @Path("/**/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addProperties(FullHttpRequest request, HttpResponder responder) throws BadRequestException,
    NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/properties");
    metadataAdmin.addProperties(metadataEntity, readProperties(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s added successfully.", metadataEntity));
  }

  @POST
  @Path("/**/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addTags(FullHttpRequest request, HttpResponder responder) throws BadRequestException, NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/tags");
    metadataAdmin.addTags(metadataEntity, readArray(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s added successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata")
  public void removeMetadata(HttpRequest request, HttpResponder responder) throws NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata");
    metadataAdmin.removeMetadata(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/properties")
  public void removeProperties(HttpRequest request, HttpResponder responder) throws NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/properties");
    metadataAdmin.removeProperties(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/properties/{property}")
  public void removeProperty(HttpRequest request, HttpResponder responder,
                             @PathParam("property") String property) throws NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/properties");
    metadataAdmin.removeProperties(metadataEntity, property);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for %s deleted successfully.", property, metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags")
  public void removeTags(HttpRequest request, HttpResponder responder) throws NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/tags");
    metadataAdmin.removeTags(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags/{tag}")
  public void removeTag(HttpRequest request, HttpResponder responder,
                        @PathParam("tag") String tag) throws NotFoundException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), "/metadata/tags");
    metadataAdmin.removeTags(metadataEntity, tag);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tag %s for %s deleted successfully.", tag, metadataEntity));
  }

  @GET
  @Path("/namespaces/{namespace-id}/metadata/search")
  public void searchMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @QueryParam("query") String searchQuery,
                             @QueryParam("target") List<String> targets,
                             @QueryParam("sort") @DefaultValue("") String sort,
                             @QueryParam("offset") @DefaultValue("0") int offset,
                             // 2147483647 is Integer.MAX_VALUE
                             @QueryParam("limit") @DefaultValue("2147483647") int limit,
                             @QueryParam("numCursors") @DefaultValue("0") int numCursors,
                             @QueryParam("cursor") @DefaultValue("") String cursor,
                             @QueryParam("showHidden") @DefaultValue("false") boolean showHidden,
                             @Nullable @QueryParam("entityScope") String entityScope) throws Exception {
    if (searchQuery == null || searchQuery.isEmpty()) {
      throw new BadRequestException("query is not specified");
    }
    Set<EntityTypeSimpleName> types = Collections.emptySet();
    if (targets != null) {
      types = ImmutableSet.copyOf(targets.stream().map(STRING_TO_TARGET_TYPE).collect(Collectors.toList()));
    }
    SortInfo sortInfo = SortInfo.of(URLDecoder.decode(sort, "UTF-8"));
    if (SortInfo.DEFAULT.equals(sortInfo)) {
      if (!(cursor.isEmpty()) || 0 != numCursors) {
        throw new BadRequestException("Cursors are not supported when sort info is not specified.");
      }
    }
    try {
      MetadataSearchResponse response =
        metadataAdmin.search(namespaceId, URLDecoder.decode(searchQuery, "UTF-8"), types,
                             sortInfo, offset, limit, numCursors, cursor, showHidden, validateEntityScope(entityScope));
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response, MetadataSearchResponse.class));
    } catch (Exception e) {
      // if MetadataDataset throws an exception, it gets wrapped
      if (Throwables.getRootCause(e) instanceof BadRequestException) {
        throw new BadRequestException(e.getMessage(), e);
      }
      throw e;
    }
  }

  private MetadataEntity getMetadataEntityFromPath(String uri, String s) {
    String[] parts = uri.substring((uri.indexOf(Constants.Gateway.API_VERSION_3) +
      Constants.Gateway.API_VERSION_3.length() + 1), uri.lastIndexOf(s)).split("/");
    MetadataEntity metadataEntity = new MetadataEntity();

    int curIndex = 0;
    while (curIndex < parts.length - 1) {
      // the api part which we get for program does not have type keyword as part of the uri. It goes like
      // ../apps/appName/programType/ProgramName so handle that correctly
      if (curIndex >= 2 && parts[curIndex - 2].equalsIgnoreCase("apps")) {
        metadataEntity = metadataEntity.append(MetadataEntity.TYPE, ProgramType.valueOfCategoryName(parts[curIndex]).name());
        metadataEntity = metadataEntity.append(MetadataEntity.PROGRAM, parts[curIndex + 1]);
      } else {
        metadataEntity = metadataEntity.append(API_PART_TO_ENTITY_TYPE.get(parts[curIndex]), parts[curIndex + 1]);
      }
      curIndex += 2;
    }
    return metadataEntity;
  }

  private Map<String, String> readProperties(FullHttpRequest request) throws BadRequestException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new BadRequestException("Unable to read metadata properties from the request.");
    }

    Map<String, String> metadata;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      metadata = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read metadata properties from the request.", e);
    }

    if (metadata == null) {
      throw new BadRequestException("Null metadata was read from the request");
    }
    return metadata;
  }

  private String[] readArray(FullHttpRequest request) throws BadRequestException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new BadRequestException("Unable to read a list of tags from the request.");
    }

    List<String> toReturn;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      toReturn = GSON.fromJson(reader, LIST_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read a list of tags from the request.", e);
    }

    if (toReturn == null) {
      throw new BadRequestException("Null tags were read from the request.");
    }
    return toReturn.toArray(new String[toReturn.size()]);
  }

  private Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity,
                                          @Nullable String scope) throws NotFoundException, BadRequestException {
    return (scope == null) ?
      metadataAdmin.getMetadata(metadataEntity) :
      metadataAdmin.getMetadata(validateScope(scope), metadataEntity);
  }

  private Map<String, String> getProperties(MetadataEntity metadataEntity,
                                            @Nullable String scope) throws NotFoundException, BadRequestException {
    return (scope == null) ?
      metadataAdmin.getProperties(metadataEntity) :
      metadataAdmin.getProperties(validateScope(scope), metadataEntity);
  }

  private Set<String> getTags(MetadataEntity metadataEntity,
                              @Nullable String scope) throws NotFoundException, BadRequestException {
    return (scope == null) ?
      metadataAdmin.getTags(metadataEntity) :
      metadataAdmin.getTags(validateScope(scope), metadataEntity);
  }

  private MetadataScope validateScope(String scope) throws BadRequestException {
    try {
      return MetadataScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid metadata scope '%s'. Expected '%s' or '%s'",
                                                  scope, MetadataScope.USER, MetadataScope.SYSTEM));
    }
  }

  private Set<EntityScope> validateEntityScope(@Nullable String entityScope) throws BadRequestException {
    if (entityScope == null) {
      return EnumSet.allOf(EntityScope.class);
    }

    try {
      return EnumSet.of(EntityScope.valueOf(entityScope.toUpperCase()));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid entity scope '%s'. Expected '%s' or '%s' for entities " +
                                                    "from specified scope, or just omit the parameter to get " +
                                                    "entities from both scopes",
                                                  entityScope, EntityScope.USER, EntityScope.SYSTEM));
    }
  }
}
