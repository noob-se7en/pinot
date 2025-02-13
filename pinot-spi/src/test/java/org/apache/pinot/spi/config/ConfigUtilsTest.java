/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.config;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ConfigUtilsTest {

  @Test
  public void testIndexing() {
    Map<String, String> environment =
        ImmutableMap.of("LOAD_MODE", "MMAP", "AWS_ACCESS_KEY", "default_aws_access_key", "AWS_SECRET_KEY",
            "default_aws_secret_key");
    testIndexingWithConfig(environment);
  }

  @Test
  public void testIndexingWithSystemProperties() {
    // Use default System properties
    System.setProperty("LOAD_MODE", "MMAP");
    System.setProperty("AWS_ACCESS_KEY", "default_aws_access_key");
    System.setProperty("AWS_SECRET_KEY", "default_aws_secret_key");
    testIndexingWithConfig(null);
    System.clearProperty("LOAD_MODE");
    System.clearProperty("AWS_ACCESS_KEY");
    System.clearProperty("AWS_SECRET_KEY");
  }

  private void testIndexingWithConfig(Map<String, String> configOverride) {
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setLoadMode("${LOAD_MODE}");
    indexingConfig.setAggregateMetrics(true);
    List<String> invertedIndexColumns = Arrays.asList("a", "b", "c");
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
    List<String> sortedColumn = Arrays.asList("d", "e", "f");
    indexingConfig.setSortedColumn(sortedColumn);
    List<String> onHeapDictionaryColumns = Arrays.asList("x", "y", "z");
    indexingConfig.setOnHeapDictionaryColumns(onHeapDictionaryColumns);
    List<String> bloomFilterColumns = Arrays.asList("a", "b");
    indexingConfig.setBloomFilterColumns(bloomFilterColumns);
    Map<String, String> noDictionaryConfig = new HashMap<>();
    noDictionaryConfig.put("a", "SNAPPY");
    noDictionaryConfig.put("b", "PASS_THROUGH");
    indexingConfig.setNoDictionaryConfig(noDictionaryConfig);
    List<String> varLengthDictionaryColumns = Arrays.asList("a", "x", "z");
    indexingConfig.setVarLengthDictionaryColumns(varLengthDictionaryColumns);

    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String defaultConsumerFactoryClass = "org.apache.pinot.plugin.stream.kafka20.StreamConsumerFactory";
    String defaultDecoderClass = "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder";

    String consumerFactoryClass = "${CONSUMER_FACTORY_CLASS:" + defaultConsumerFactoryClass + "}";
    String decoderClass = "${DECODER_CLASS:" + defaultDecoderClass + "}";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, "aws.accessKey"),
        "${AWS_ACCESS_KEY}");
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType, "aws.secretKey"),
        "${AWS_SECRET_KEY}");
    indexingConfig.setStreamConfigs(streamConfigMap);
    if (configOverride != null) {
      indexingConfig = ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(configOverride, indexingConfig);
    } else {
      indexingConfig = ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(indexingConfig);
    }
    assertEquals(indexingConfig.getLoadMode(), "MMAP");
    assertTrue(indexingConfig.isAggregateMetrics());
    assertEquals(indexingConfig.getInvertedIndexColumns(), invertedIndexColumns);
    assertEquals(indexingConfig.getSortedColumn(), sortedColumn);
    assertEquals(indexingConfig.getOnHeapDictionaryColumns(), onHeapDictionaryColumns);
    assertEquals(indexingConfig.getBloomFilterColumns(), bloomFilterColumns);
    assertEquals(indexingConfig.getNoDictionaryConfig(), noDictionaryConfig);
    assertEquals(indexingConfig.getVarLengthDictionaryColumns(), varLengthDictionaryColumns);

    // Mandatory values + defaults
    StreamConfig streamConfig = new StreamConfig(tableName, indexingConfig.getStreamConfigs());
    assertEquals(streamConfig.getType(), streamType);
    assertEquals(streamConfig.getTopicName(), topic);
    assertEquals(streamConfig.getConsumerFactoryClassName(), defaultConsumerFactoryClass);
    assertEquals(streamConfig.getDecoderClass(), defaultDecoderClass);
    assertEquals(streamConfig.getStreamConfigsMap().get("stream.fakeStream.aws.accessKey"), "default_aws_access_key");
    assertEquals(streamConfig.getStreamConfigsMap().get("stream.fakeStream.aws.secretKey"), "default_aws_secret_key");
    assertEquals(streamConfig.getDecoderProperties().size(), 0);
    assertEquals(streamConfig.getOffsetCriteria(), new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest());
    assertEquals(streamConfig.getConnectionTimeoutMillis(), StreamConfig.DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);
    assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
    assertEquals(streamConfig.getFlushThresholdRows(), -1);
    assertEquals(streamConfig.getFlushThresholdSegmentRows(), -1);
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), -1);
  }

  @Test
  public void testDefaultObfuscation() {
    Map<String, Object> map = new HashMap<>();
    map.put("username", "admin");
    map.put("password", "verysecret");
    map.put("my.authToken", "secrettoken");

    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("credentials", map);

    PinotConfiguration config = new PinotConfiguration(nestedMap);

    String configString = config.toString();
    assertTrue(configString.contains("credentials"));
    assertFalse(configString.contains("verysecret"));
    assertFalse(configString.contains("secrettoken"));
  }
}
