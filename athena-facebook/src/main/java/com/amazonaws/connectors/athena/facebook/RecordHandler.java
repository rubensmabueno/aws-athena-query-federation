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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.restfb.*;
import com.restfb.json.JsonObject;
import com.restfb.json.JsonValue;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example RecordHandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from your source. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ExampleMetadataHandler.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class RecordHandler
        extends com.amazonaws.athena.connector.lambda.handlers.RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(RecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";

    private AmazonS3 amazonS3;

    public RecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
    }

    @VisibleForTesting
    protected RecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
    }

    String ACCESS_TOKEN = "EAACpOw1jK5gBABsWqTLzU6JRKJHa700Kg7P1zb6PxaoA8cbcVmRZB39cpLWgULvSjFHsueBRFPXyb6OkRPGtElzS3j3yTijnPsE0pQn8onbq2NNjGOLaRDnWKGVnyS5srkxo1bUVJLQ6p62OWJlXsiZAz2D9lNBKsHWBm58biayJ9ujRfZA";

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());

        Split split = recordsRequest.getSplit();
        String splitAccountId = "";

        splitAccountId = split.getProperty("account_id");

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        builder.withExtractor("account_id", (VarCharExtractor) (ArrayList context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = (((String) context.get(0));
        });

        builder.withExtractor("date_start", (VarCharExtractor) (ArrayList context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((String) context.get(0));
        });

        builder.withExtractor("date_stop", (VarCharExtractor) (ArrayList context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((String) context.get(2));
        });

        builder.withExtractor("impressions", (VarCharExtractor) (ArrayList context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((String) context.get(3));
        });

        builder.withExtractor("spend", (VarCharExtractor) (ArrayList context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((String) context.get(0));
        });

        /**
         * TODO: Write data for our transaction STRUCT:
         * For complex types like List and Struct, we can build a Map to conveniently set nested values
         *
         builder.withFieldWriterFactory("transaction",
                (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                    (Object context, int rowNum) -> {
                         Map<String, Object> eventMap = new HashMap<>();
                         eventMap.put("id", Integer.parseInt(((String[])context)[4]));
                         eventMap.put("completed", Boolean.parseBoolean(((String[])context)[5]));
                         BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, eventMap);
                         return true;    //we don't yet support predicate pushdown on complex types
         });
         */

        //Used some basic code-gen to optimize how we generate response data.
        GeneratedRowWriter rowWriter = builder.build();

        //We read the transaction data line by line from our S3 object.
        //String line;
        //while ((line = s3Reader.readLine()) != null) {
        //    logger.info("readWithConstraint: processing line " + line);
        //
        //    //The sample_data.csv file is structured as year,month,day,account_id,transaction.id,transaction.complete
        //    String[] lineParts = line.split(",");
        //
        //    //We use the provided BlockSpiller to write our row data into the response. This utility is provided by
        //    //the Amazon Athena Query Federation SDK and automatically handles breaking the data into reasonably sized
        //    //chunks, encrypting it, and spilling to S3 if we've enabled these features.
        //    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, lineParts) ? 1 : 0);
        //}
        FacebookClient facebookClient = new DefaultFacebookClient(ACCESS_TOKEN, Version.LATEST);

        Connection<JsonObject> insightsConnection =
                facebookClient.fetchConnection("act_" + splitAccountId + "/insights",
                        JsonObject.class,
                        Parameter.with("metric", "page_fan_adds_unique,page_fan_adds"));

        for (List<JsonObject> insights: insightsConnection) {
            for (JsonObject insight : insights) {
                List<String> fields = new ArrayList<>();

                for (String fieldName : insight.names()) {
                    fields.add(insight.getString(fieldName, ""));
                }

                spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, fields) ? 1 : 0);
//                out.println(insight.getName());
            }
        }
    }
}
