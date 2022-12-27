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

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.DeleteTablePrefix;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;

/**
 * Class to test OMKeysDeleteResponse.
 */
public class TestOMKeysDeleteResponse extends TestOMKeyResponse {


  private List<OmKeyInfo> omKeyInfoList = new ArrayList<>();
  private List<String> ozoneKeys = new ArrayList<>();

  protected List<OmKeyInfo> getOmKeyInfoList() {
    return omKeyInfoList;
  }

  protected List<String> getOzoneKeys() {
    return ozoneKeys;
  }

  protected void createPreRequisities() throws Exception {
    String parent = "/user";
    String key = "key";

    String ozoneKey = "";
    for (int i = 0; i < 10; i++) {
      keyName = parent.concat(key + i);
      OMRequestTestUtils.addKeyToTable(false, volumeName,
          bucketName, keyName, 0L, RATIS, THREE, omMetadataManager);
      ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
      omKeyInfoList
          .add(omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey));
      ozoneKeys.add(ozoneKey);
    }
  }

  @Test
  public void testKeysDeleteResponse() throws Exception {

    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteKeys).setStatus(OK)
            .setSuccess(true)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
                .setStatus(true)).build();

    DeleteTablePrefix prefix = new DeleteTablePrefix(Time.now(), 360L);
    OMClientResponse omKeysDeleteResponse = getOmKeysDeleteResponse(
        omResponse, omBucketInfo);

    omKeysDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    for (String ozKey : ozoneKeys) {
      Assert.assertNull(
          omMetadataManager.getKeyTable(getBucketLayout()).get(ozKey));
    }

    // deleteKey had no block information associated with it, so it should have
    // been removed from the key table but not added to the delete table.
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      String deleteKey = prefix.buildKey(omKeyInfo);
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(deleteKey));
    }
  }

  protected OMClientResponse getOmKeysDeleteResponse(OMResponse omResponse,
      OmBucketInfo omBucketInfo) {
    DeleteTablePrefix prefix = new DeleteTablePrefix(Time.now(), 360L);

    return new OMKeysDeleteResponse(omResponse, prefix,
        omKeyInfoList, omBucketInfo);
  }

  @Test
  public void testKeysDeleteResponseFail() throws Exception {
    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteKeys).setStatus(KEY_NOT_FOUND)
            .setSuccess(false)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
                .setStatus(false)).build();

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setCreationTime(Time.now()).build();

    DeleteTablePrefix prefix = new DeleteTablePrefix(Time.now(), 360L);
    OMClientResponse omKeysDeleteResponse = new OMKeysDeleteResponse(
        omResponse, prefix, omKeyInfoList, omBucketInfo);

    omKeysDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    for (String ozKey : ozoneKeys) {
      Assert.assertNotNull(
          omMetadataManager.getKeyTable(getBucketLayout()).get(ozKey));
    }

    // On failure, deletion (insert to delete table) should not be committed.
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      String deleteKey = prefix.buildKey(omKeyInfo);
      Assert.assertFalse(omMetadataManager.getDeletedTable().isExist(deleteKey));
    }
  }
}
