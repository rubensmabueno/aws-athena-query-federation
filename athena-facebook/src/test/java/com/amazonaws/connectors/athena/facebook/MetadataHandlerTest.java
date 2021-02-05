/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.facebook;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Version;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class MetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataHandlerTest.class);

    private com.amazonaws.connectors.athena.facebook.MetadataHandler handler = new com.amazonaws.connectors.athena.facebook.MetadataHandler(new LocalKeyFactory(),
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            "spill-bucket",
            "spill-prefix");

    private BlockAllocatorImpl allocator;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
    {
        ListSchemasRequest req = new ListSchemasRequest(fakeIdentity(), "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        assertTrue(res.getSchemas().contains("marketing"));
    }

    @Test
    public void doListTables()
    {
        ListTablesRequest req = new ListTablesRequest(fakeIdentity(), "queryId", "default", "marketing");
        ListTablesResponse res = handler.doListTables(allocator, req);
        assertTrue(res.getTables().stream().anyMatch(n -> n.getTableName().equals("insights")));
    }

    @Test
    public void doGetTable()
    {
        GetTableRequest req = new GetTableRequest(fakeIdentity(), "queryId", "default",
                new TableName("marketing", "insights"));
        GetTableResponse res = handler.doGetTable(allocator, req);
        assertTrue(res.getSchema().getFields().size() > 0);
    }

    @Test
    public void getPartitions()
            throws Exception
    {
        Schema tableSchema = SchemaBuilder.newBuilder()
                .addStringField("account_id")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("account_id");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("account_id", new AllOrNoneValueSet(Types.MinorType.VARCHAR.getType(), true, false));

        GetTableLayoutRequest req = null;
        GetTableLayoutResponse res = null;
        try {

            req = new GetTableLayoutRequest(fakeIdentity(), "queryId", "default",
                    new TableName("marketing", "insights"),
                    new Constraints(constraintsMap),
                    tableSchema,
                    partitionCols);

            res = handler.doGetTableLayout(allocator, req);

            logger.info("doGetTableLayout - {}", res);
            Block partitions = res.getPartitions();
            for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
                logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
            }
            assertTrue(partitions.getRowCount() > 0);
            logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
        }
        finally {
            try {
                req.close();
                res.close();
            }
            catch (Exception ex) {
                logger.error("doGetTableLayout: ", ex);
            }
        }

        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits()
    {
        String accountIdCol = "account_id";

        //This is the schema that ExampleMetadataHandler has layed out for a 'Partition' so we need to populate this
        //minimal set of info here.
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField(accountIdCol)
                .build();

        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(accountIdCol);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 2;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(accountIdCol), i, "account_" + i);
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(fakeIdentity(), "queryId", "catalog_name",
                new TableName("marketing", "insights"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

            logger.info("doGetSplits: req[{}]", req);
            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - splits[{}]", continuationToken, response.getSplits());

            for (Split nextSplit : response.getSplits()) {
                assertNotNull(nextSplit.getProperty("account_id"));
            }

            assertTrue(!response.getSplits().isEmpty());

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations == 0);

        logger.info("doGetSplits: exit");
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }
}
