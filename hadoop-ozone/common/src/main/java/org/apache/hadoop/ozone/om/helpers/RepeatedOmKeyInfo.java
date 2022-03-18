/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RepeatedKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyInfo;

/**
 * Args for deleted keys. This is written to om metadata deletedTable.
 * Once a key is deleted, it is moved to om metadata deletedTable. Having a
 * {label: List<OMKeyInfo>} ensures that if users create & delete keys with
 * exact same uri multiple times, all the delete instances are bundled under
 * the same key name. This is useful as part of GDPR compliance where an
 * admin wants to confirm if a given key is deleted from deletedTable metadata.
 */
public class RepeatedOmKeyInfo {
  private List<OmKeyInfo> omKeyInfoList;

  public RepeatedOmKeyInfo(List<OmKeyInfo> omKeyInfos) {
    this.omKeyInfoList = omKeyInfos;
  }

  public RepeatedOmKeyInfo(OmKeyInfo omKeyInfo) {
    this();
    this.omKeyInfoList.add(omKeyInfo);
  }

  public RepeatedOmKeyInfo() {
    this.omKeyInfoList = new ArrayList<>();
  }

  public void addOmKeyInfo(OmKeyInfo info) {
    this.omKeyInfoList.add(info);
  }

  public List<OmKeyInfo> getOmKeyInfoList() {
    return omKeyInfoList;
  }

  public static RepeatedOmKeyInfo getFromProto(RepeatedKeyInfo
                                                       repeatedKeyInfo) {
    List<OmKeyInfo> list = new ArrayList<>();
    for (KeyInfo k : repeatedKeyInfo.getKeyInfoList()) {
      list.add(OmKeyInfo.getFromProtobuf(k));
    }
    return new RepeatedOmKeyInfo.Builder().setOmKeyInfos(list).build();
  }

  /**
   * @param compact, true for persistence, false for network transmit
   * @return
   */
  public RepeatedKeyInfo getProto(boolean compact, int clientVersion) {
    if (omKeyInfoList.isEmpty()) {
      throw new RuntimeException("RepeatedKeyInfo shall not be encoded if empty");
    }

    List<KeyInfo> list = new ArrayList<>();
    for (OmKeyInfo k : omKeyInfoList) {
      list.add(k.getProtobuf(compact, clientVersion));
    }

    RepeatedKeyInfo.Builder builder = RepeatedKeyInfo.newBuilder()
            .addAllKeyInfo(list);
    return builder.build();
  }

  /**
   * Builder of RepeatedOmKeyInfo.
   */
  public static class Builder {
    private List<OmKeyInfo> omKeyInfos;

    public Builder() {
    }

    public Builder setOmKeyInfos(List<OmKeyInfo> infoList) {
      this.omKeyInfos = infoList;
      return this;
    }

    public RepeatedOmKeyInfo build() {
      return new RepeatedOmKeyInfo(omKeyInfos);
    }
  }

  public RepeatedOmKeyInfo copyObject() {
    return new RepeatedOmKeyInfo(new ArrayList<>(omKeyInfoList));
  }

  /**
   * Prepares key info to be moved to deletedTable.
   * 1. It strips GDPR metadata from key info
   * 2. For given object key, if the repeatedOmKeyInfo instance is null, it
   * implies that no entry for the object key exists in deletedTable so we
   * create a new instance to include this key, else we update the existing
   * repeatedOmKeyInfo instance.
   * 3. Set the updateID to the transactionLogIndex.
   *
   * @param trxnLogIndex For Multipart keys, this is the transactionLogIndex
   *                     of the MultipartUploadAbort request which needs to
   *                     be set as the updateID of the partKeyInfos.
   *                     For regular Key deletes, this value should be set to
   *                     the same updateID as is in keyInfo.
   */
  public void prepareKeyForDelete(long trxnLogIndex, boolean isRatisEnabled) {
    clearGDPRdata();
    setUpdateIDs(trxnLogIndex, isRatisEnabled);
  }

  public void setUpdateIDs(long trxnLogIndex, boolean isRatisEnabled) {
    for (OmKeyInfo keyInfo : omKeyInfoList) {
      // Set the updateID
      keyInfo.setUpdateID(trxnLogIndex, isRatisEnabled);
    }
  }

  public void clearGDPRdata(){
    // If this key is in a GDPR enforced bucket, then before moving
    // KeyInfo to deletedTable, remove the GDPR related metadata and
    // FileEncryptionInfo from KeyInfo.
    for (OmKeyInfo keyInfo : omKeyInfoList) {
      if (Boolean.valueOf(keyInfo.getMetadata().get(OzoneConsts.GDPR_FLAG))) {
        keyInfo.getMetadata().remove(OzoneConsts.GDPR_FLAG);
        keyInfo.getMetadata().remove(OzoneConsts.GDPR_ALGORITHM);
        keyInfo.getMetadata().remove(OzoneConsts.GDPR_SECRET);
        keyInfo.clearFileEncryptionInfo();
      }
    }
  }

  public long getUsedBytes() {
    long total = 0;
    for (OmKeyInfo keyInfo : omKeyInfoList) {
      total += keyInfo.getDataSize() *
              keyInfo.getReplicationConfig().getRequiredNodes();
    }
    return total;
  }
}
