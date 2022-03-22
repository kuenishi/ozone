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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Response for DeleteKey request.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE, DELETED_TABLE})
public class OMKeyDeleteResponse extends AbstractOMKeyDeleteResponse {

  private OmBucketInfo omBucketInfo;

  public OMKeyDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull RepeatedOmKeyInfo repeatedOmKeyInfo,
      @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, repeatedOmKeyInfo, omBucketInfo.getBucketLayout());
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyDeleteResponse(@Nonnull OMResponse omResponse, @Nonnull
                             BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // We can safely assume there is only one key in the repeatedOmKeyInfo
    OmKeyInfo omKeyInfo = repeatedOmKeyInfo.getOmKeyInfoList().get(0);

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    String ozoneKey = omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName());

    omMetadataManager.getKeyTable(getBucketLayout()).deleteWithBatch(
        batchOperation, ozoneKey);

    String key = OmUtils.keyForDeleteTable(omKeyInfo);
    omMetadataManager.getDeletedTable().putWithBatch(
        batchOperation, key, repeatedOmKeyInfo);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }

  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }
}
