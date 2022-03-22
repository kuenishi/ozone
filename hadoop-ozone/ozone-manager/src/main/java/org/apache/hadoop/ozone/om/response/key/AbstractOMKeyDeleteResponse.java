/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;

/**
 * Base class for responses that need to move keys from an arbitrary table to
 * the deleted table.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE})
public abstract class AbstractOMKeyDeleteResponse extends OmKeyResponse {

  protected RepeatedOmKeyInfo repeatedOmKeyInfo;

  public AbstractOMKeyDeleteResponse(
      @Nonnull OMResponse omResponse,
      @Nonnull RepeatedOmKeyInfo repeatedOmKeyInfo) {

    super(omResponse);
    this.repeatedOmKeyInfo = repeatedOmKeyInfo;
  }

  public AbstractOMKeyDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull RepeatedOmKeyInfo repeatedOmKeyInfo,
      BucketLayout bucketLayout) {

    super(omResponse, bucketLayout);
    this.repeatedOmKeyInfo = repeatedOmKeyInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public AbstractOMKeyDeleteResponse(@Nonnull OMResponse omResponse,
                                     @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public abstract void addToDBBatch(OMMetadataManager omMetadataManager,
        BatchOperation batchOperation) throws IOException;

  /**
   * Adds the operation of deleting the {@code keyName omKeyInfo} pair from
   * {@code fromTable} to the batch operation {@code batchOperation}. The
   * batch operation is not committed, so no changes are persisted to disk.
   * The log transaction index used will be retrieved by calling
   * {@link OmKeyInfo#getUpdateID} on {@code omKeyInfo}.
   */
  protected void addDeletionToBatch(
          OMMetadataManager omMetadataManager,
          BatchOperation batchOperation,
          Table<String, ?> fromTable,
          List<OmKeyInfo> omKeyInfoList) throws IOException {

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      String volumeName = omKeyInfo.getVolumeName();
      String bucketName = omKeyInfo.getBucketName();
      String keyName = omKeyInfo.getKeyName();

      String deleteKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
              keyName);

      fromTable.deleteWithBatch(batchOperation, deleteKey);
    }

    repeatedOmKeyInfo.clearGDPRdata();
    OmKeyInfo omKeyInfo = omKeyInfoList.get(0);
    String key = OmUtils.keyForDeleteTable(omKeyInfo);
    omMetadataManager.getDeletedTable().putWithBatch(
            batchOperation, key, repeatedOmKeyInfo);
  }
  /**
   * Check if the key is empty or not. Key will be empty if it does not have
   * blocks.
   *
   * @param keyInfo
   * @return if empty true, else false.
   */
  private boolean isKeyEmpty(@Nullable OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return true;
    }
    for (OmKeyLocationInfoGroup keyLocationList : keyInfo
            .getKeyLocationVersions()) {
      if (keyLocationList.getLocationListCount() != 0) {
        return false;
      }
    }
    return true;
  }
}
