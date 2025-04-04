/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import com.google.protobuf.CodedOutputStream;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class MetricsRestAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "metrics_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_metrics"));
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.hasContent()) {
            var transportRequest = new MetricsTransportAction.MetricsRequest(request.param("index"), request.content());
            return channel -> client.execute(MetricsTransportAction.TYPE, transportRequest, new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(MetricsTransportAction.MetricsResponse r) throws Exception {
                    return successResponse();
                }
            });
        }

        // according to spec empty requests are successful
        return channel -> { channel.sendResponse(successResponse()); };
    }

    private RestResponse successResponse() throws IOException {
        var response = ExportMetricsServiceResponse.newBuilder().build();
        var responseBytes = ByteBuffer.allocate(response.getSerializedSize());
        response.writeTo(CodedOutputStream.newInstance(responseBytes));

        return new RestResponse(RestStatus.OK, "application/x-protobuf", BytesReference.fromByteBuffer(responseBytes));
    }
}
