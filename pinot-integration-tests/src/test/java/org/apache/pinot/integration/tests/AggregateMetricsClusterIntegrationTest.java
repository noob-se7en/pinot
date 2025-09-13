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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that enables aggregate metrics for the LLC real-time table.
 */
public class AggregateMetricsClusterIntegrationTest extends BaseClusterIntegrationTestSet {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config with reduced number of columns and aggregate metrics on
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(getTableName()).addSingleValueDimension("Carrier", DataType.STRING)
            .addSingleValueDimension("Origin", DataType.STRING).addMetric("AirTime", DataType.LONG)
            .addMetric("ArrDelay", DataType.DOUBLE)
            .addDateTime("DaysSinceEpoch", DataType.INT, "1:DAYS:EPOCH", "1:DAYS").build();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    TableConfig tableConfigOffline = createOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setSortedColumn(Collections.singletonList("Carrier"));
    indexingConfig.setInvertedIndexColumns(Collections.singletonList("Origin"));
    indexingConfig.setNoDictionaryColumns(Arrays.asList("AirTime", "ArrDelay"));
    indexingConfig.setRangeIndexColumns(Collections.singletonList("DaysSinceEpoch"));
    indexingConfig.setBloomFilterColumns(Collections.singletonList("Origin"));
    indexingConfig.setAggregateMetrics(true);
    addTableConfig(tableConfig);
//    addTableConfig(tableConfigOffline);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testIngestFromURI()
      throws Exception {
//    File testCsvFile = new File(getClass().getClassLoader().getResource("test1.csv").getFile());
//    File file = Files.copy(testCsvFile.toPath(), FileUtils.getTempDirectory().toPath().resolve(testCsvFile.getName()))
//        .toFile();
    String batchConfigMapStr = "{"
        + "\"inputFormat\":\"csv\""
        + "}";
//      String fileURIStr = file.toURI().toString();

    String encodedBatchConfig = URLEncoder.encode(batchConfigMapStr, StandardCharsets.UTF_8);
    String encodedSourceURI = URLEncoder.encode("file:///var/folders/d5/vqqy7cq14q98r30nsnz3dh4w0000gp/T/preview_temp/658abfbe-076e-4708-91cd-8f685483f14b.csv", StandardCharsets.UTF_8);

    String queryParams = "tableNameWithType=" + getTableName()+"_OFFLINE" +
        "&batchConfigMapStr=" + encodedBatchConfig +
        "&sourceURIStr=" + encodedSourceURI;

//      addTableConfig();

    String str = sendPostRequest(getControllerBaseApiUrl() + "/ingestFromURI" + "?" + queryParams);
//    assert 200 == execute.getCode();
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    // NOTE: For aggregate metrics, we need to test the aggregation result instead of the document count because
    //       documents can be merged during ingestion.
    String sql = "SELECT SUM(AirTime), SUM(ArrDelay) FROM mytable";
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResult = postQuery(sql);
        JsonNode aggregationResults = queryResult.get("resultTable").get("rows").get(0);
        return aggregationResults.get(0).asInt() == -165429728 && aggregationResults.get(1).asInt() == -175625957;
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT SUM(AirTime), SUM(ArrDelay) FROM mytable";
    testQuery(query);
    query = "SELECT SUM(AirTime), DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch ORDER BY SUM(AirTime) DESC";
    testQuery(query);
    query = "SELECT Origin, SUM(ArrDelay) FROM mytable WHERE Carrier = 'AA' GROUP BY Origin ORDER BY Origin";
    testQuery(query);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
