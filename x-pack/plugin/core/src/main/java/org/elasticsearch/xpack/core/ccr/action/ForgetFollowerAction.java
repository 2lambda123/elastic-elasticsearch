/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class ForgetFollowerAction extends Action<ForgetFollowerAction.Request, BroadcastResponse, ForgetFollowerAction.RequestBuilder> {

    public static final String NAME = "indices:admin/xpack/ccr/forget_follower";
    public static final ForgetFollowerAction INSTANCE = new ForgetFollowerAction();

    private ForgetFollowerAction() {
        super(NAME);
    }

    @Override
    public BroadcastResponse newResponse() {
        return new BroadcastResponse();
    }

    /**
     * Represents a forget follower request. Note that this an expert API intended to be used only when unfollowing a follower index fails
     * to emove the follower retention leases. Please be sure that you understand the purpose this API before using.
     */
    public static class Request extends BroadcastRequest<Request> {

        private static final ParseField FOLLOWER_CLUSTER = new ParseField("follower_cluster");
        private static final ParseField FOLLOWER_INDEX = new ParseField("follower_index");
        private static final ParseField FOLLOWER_INDEX_UUID = new ParseField("follower_index_uuid");
        private static final ParseField LEADER_REMOTE_CLUSTER = new ParseField("leader_remote_cluster");

        private static final ObjectParser<String[], Void> PARSER = new ObjectParser<>(NAME, () -> new String[4]);

        static {
            PARSER.declareString((parameters, value) -> parameters[0] = value, FOLLOWER_CLUSTER);
            PARSER.declareString((parameters, value) -> parameters[1] = value, FOLLOWER_INDEX);
            PARSER.declareString((parameters, value) -> parameters[2] = value, FOLLOWER_INDEX_UUID);
            PARSER.declareString((parameters, value) -> parameters[3] = value, LEADER_REMOTE_CLUSTER);
        }

        public static ForgetFollowerAction.Request fromXContent(
                final XContentParser parser,
                final String leaderIndex) throws IOException {
            final String[] parameters = PARSER.parse(parser, null);
            return new Request(parameters[0], parameters[1], parameters[2], parameters[3], leaderIndex);
        }

        private String followerCluster;

        /**
         * The name of the cluster containing the follower index.
         *
         * @return the name of the cluster containing the follower index
         */
        public String followerCluster() {
            return followerCluster;
        }

        private String followerIndex;

        /**
         * The name of the follower index.
         *
         * @return the name of the follower index
         */
        public String followerIndex() {
            return followerIndex;
        }

        private String followerIndexUUID;

        /**
         * The UUID of the follower index.
         *
         * @return the UUID of the follower index
         */
        public String followerIndexUUID() {
            return followerIndexUUID;
        }

        private String leaderRemoteCluster;

        /**
         * The alias of the remote cluster containing the leader index.
         *
         * @return the alias of the remote cluster
         */
        public String leaderRemoteCluster() {
            return leaderRemoteCluster;
        }

        private String leaderIndex;

        /**
         * The name of the leader index.
         *
         * @return the name of the leader index
         */
        public String leaderIndex() {
            return leaderIndex;
        }

        public Request() {

        }

        /**
         * Construct a forget follower request.
         *
         * @param followerCluster     the name of the cluster containing the follower index to forget
         * @param followerIndex       the name of follower index
         * @param followerIndexUUID   the UUID of the follower index
         * @param leaderRemoteCluster the alias of the remote cluster containing the leader index from the perspective of the follower index
         * @param leaderIndex         the name of the leader index
         */
        public Request(
                final String followerCluster,
                final String followerIndex,
                final String followerIndexUUID,
                final String leaderRemoteCluster,
                final String leaderIndex) {
            super(new String[]{leaderIndex});
            this.followerCluster = Objects.requireNonNull(followerCluster);
            this.leaderIndex = Objects.requireNonNull(leaderIndex);
            this.leaderRemoteCluster = Objects.requireNonNull(leaderRemoteCluster);
            this.followerIndex = Objects.requireNonNull(followerIndex);
            this.followerIndexUUID = Objects.requireNonNull(followerIndexUUID);
        }

        public Request(final StreamInput in) throws IOException {
            super.readFrom(in);
            followerCluster = in.readString();
            leaderIndex = in.readString();
            leaderRemoteCluster = in.readString();
            followerIndex = in.readString();
            followerIndexUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerCluster);
            out.writeString(leaderIndex);
            out.writeString(leaderRemoteCluster);
            out.writeString(followerIndex);
            out.writeString(followerIndexUUID);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

    }

    @Override
    public RequestBuilder newRequestBuilder(final ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, BroadcastResponse, RequestBuilder> {

        public RequestBuilder(final ElasticsearchClient client, final Action<Request, BroadcastResponse, RequestBuilder> action) {
            super(client, action, new Request());
        }

    }

}
