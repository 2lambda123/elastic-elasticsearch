/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference;

import java.io.IOException;

public class RerankingIT extends InferenceBaseRestTest {

    public void testPutCohereRerankEndpoint() throws IOException {
        String endpoint = putCohereRerankEndpoint();
        postCohereRerankEndpoint(
            endpoint,
            "what is elasticsearch for?",
            new String[] { "for search", "for security", "for logs", "for email", "for rubber bands", "for kiwis" }
        );
    }

    private String putCohereRerankEndpoint() throws IOException {
        String endpointID = randomAlphaOfLength(10).toLowerCase();
        putRequest("/_inference/rerank/" + endpointID, """
            {
              "service": "cohere",
              "service_settings": {
                "model_id": "rerank-english-v2.0",
                "api_key": "8TNPBvpBO7oN97009HQHzQbBhNrxmREbcJrZCwkK" // TODO remove key
              }
            }
            """);
        return endpointID;
    }

    public void postCohereRerankEndpoint(String endpoint, String query, String[] input) throws IOException {
        StringBuilder body = new StringBuilder();

        // Start the JSON object
        body.append("{");

        // Add the query to the JSON object
        body.append("\"query\":\"").append(query).append("\",");

        // Start the input array
        body.append("\"input\":[");

        // Add each element of the input array to the JSON array
        for (int i = 0; i < input.length; i++) {
            body.append("\"").append(input[i]).append("\"");
            if (i < input.length - 1) {
                body.append(",");
            }
        }

        // End the input array and the JSON object
        body.append("]}");
        postRequest("/_inference/rerank/" + endpoint, body.toString());
    }

}
