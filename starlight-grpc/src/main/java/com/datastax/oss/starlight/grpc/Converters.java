/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.starlight.grpc;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.SubscriptionType;

public final class Converters {

  public static SubscriptionType toSubscriptionType(
      com.datastax.oss.starlight.grpc.proto.SubscriptionType type) {
    switch (type) {
      case SUBSCRIPTION_TYPE_EXCLUSIVE:
        return SubscriptionType.Exclusive;
      case SUBSCRIPTION_TYPE_FAILOVER:
        return SubscriptionType.Failover;
      case SUBSCRIPTION_TYPE_SHARED:
        return SubscriptionType.Shared;
      case SUBSCRIPTION_TYPE_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid subscription type");
    }
  }

  public static ConsumerCryptoFailureAction toConsumerCryptoFailureAction(
      com.datastax.oss.starlight.grpc.proto.ConsumerCryptoFailureAction action) {
    switch (action) {
      case CONSUMER_CRYPTO_FAILURE_ACTION_FAIL:
        return ConsumerCryptoFailureAction.FAIL;
      case CONSUMER_CRYPTO_FAILURE_ACTION_DISCARD:
        return ConsumerCryptoFailureAction.DISCARD;
      case CONSUMER_CRYPTO_FAILURE_ACTION_CONSUME:
        return ConsumerCryptoFailureAction.CONSUME;
      case CONSUMER_CRYPTO_FAILURE_ACTION_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid consumer crypto failure action");
    }
  }

  public static HashingScheme toHashingScheme(
      com.datastax.oss.starlight.grpc.proto.HashingScheme scheme) {
    switch (scheme) {
      case HASHING_SCHEME_JAVA_STRING_HASH:
        return HashingScheme.JavaStringHash;
      case HASHING_SCHEME_MURMUR3_32HASH:
        return HashingScheme.Murmur3_32Hash;
      case HASHING_SCHEME_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid hashing scheme");
    }
  }

  public static MessageRoutingMode toMessageRoutingMode(
      com.datastax.oss.starlight.grpc.proto.MessageRoutingMode mode) {
    switch (mode) {
      case MESSAGE_ROUTING_MODE_SINGLE_PARTITION:
        return MessageRoutingMode.SinglePartition;
      case MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION:
        return MessageRoutingMode.RoundRobinPartition;
      case MESSAGE_ROUTING_MODE_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid message routing mode");
    }
  }

  public static CompressionType toCompressionType(
      com.datastax.oss.starlight.grpc.proto.CompressionType type) {
    switch (type) {
      case COMPRESSION_TYPE_NONE:
        return CompressionType.NONE;
      case COMPRESSION_TYPE_LZ4:
        return CompressionType.LZ4;
      case COMPRESSION_TYPE_ZLIB:
        return CompressionType.ZLIB;
      case COMPRESSION_TYPE_DEFAULT:
        return null;
      default:
        throw new IllegalArgumentException("Invalid compression type");
    }
  }

  public static MessageId toMessageId(com.datastax.oss.starlight.grpc.proto.MessageId messageId) {
    switch (messageId) {
      case LATEST:
        return MessageId.latest;
      case EARLIEST:
        return MessageId.earliest;
      default:
        throw new IllegalArgumentException("Invalid message id");
    }
  }
}
