/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.Version;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;

public class ClusterHealthResponse extends ActionResponse implements StatusToXContentObject {
    private String clusterName;
    private int numberOfPendingTasks = 0;
    private int numberOfInFlightFetch = 0;
    private int delayedUnassignedShards = 0;
    private TimeValue taskMaxWaitingTime = TimeValue.timeValueMillis(0);
    private boolean timedOut = false;
    private ClusterStateHealth clusterStateHealth;
    private ClusterHealthStatus clusterHealthStatus;

    ClusterHealthResponse() {
    }

    /** needed for plugins BWC */
    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState) {
        this(clusterName, concreteIndices, clusterState, -1, -1, -1, TimeValue.timeValueHours(0));
    }

    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState, int numberOfPendingTasks,
                                 int numberOfInFlightFetch, int delayedUnassignedShards, TimeValue taskMaxWaitingTime) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
        this.clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    public String getClusterName() {
        return clusterName;
    }

    //package private for testing
    ClusterStateHealth getClusterStateHealth() {
        return clusterStateHealth;
    }

    public int getActiveShards() {
        return clusterStateHealth.getActiveShards();
    }

    public int getRelocatingShards() {
        return clusterStateHealth.getRelocatingShards();
    }

    public int getActivePrimaryShards() {
        return clusterStateHealth.getActivePrimaryShards();
    }

    public int getInitializingShards() {
        return clusterStateHealth.getInitializingShards();
    }

    public int getUnassignedShards() {
        return clusterStateHealth.getUnassignedShards();
    }

    public int getNumberOfNodes() {
        return clusterStateHealth.getNumberOfNodes();
    }

    public int getNumberOfDataNodes() {
        return clusterStateHealth.getNumberOfDataNodes();
    }

    public int getNumberOfPendingTasks() {
        return this.numberOfPendingTasks;
    }

    public int getNumberOfInFlightFetch() {
        return this.numberOfInFlightFetch;
    }

    /**
     * The number of unassigned shards that are currently being delayed (for example,
     * due to node leaving the cluster and waiting for a timeout for the node to come
     * back in order to allocate the shards back to it).
     */
    public int getDelayedUnassignedShards() {
        return this.delayedUnassignedShards;
    }

    /**
     * <tt>true</tt> if the waitForXXX has timeout out and did not match.
     */
    public boolean isTimedOut() {
        return this.timedOut;
    }

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public ClusterHealthStatus getStatus() {
        return clusterHealthStatus;
    }

    /**
     * Allows to explicitly override the derived cluster health status.
     *
     * @param status The override status. Must not be null.
     */
    public void setStatus(ClusterHealthStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("'status' must not be null");
        }
        this.clusterHealthStatus = status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return clusterStateHealth.getIndices();
    }

    /**
     *
     * @return The maximum wait time of all tasks in the queue
     */
    public TimeValue getTaskMaxWaitingTime() {
        return taskMaxWaitingTime;
    }

    /**
     * The percentage of active shards, should be 100% in a green system
     */
    public double getActiveShardsPercent() {
        return clusterStateHealth.getActiveShardsPercent();
    }

    public static ClusterHealthResponse readResponseFrom(StreamInput in) throws IOException {
        ClusterHealthResponse response = new ClusterHealthResponse();
        response.readFrom(in);
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = in.readString();
        
        if (in.getVersion().before(Version.V_5_0_0_alpha1)) {
            // For 2.x format
            // First read the old format
            int activePrimaryShards = in.readVInt();
            int activeShards = in.readVInt();
            int relocatingShards = in.readVInt();
            int initializingShards = in.readVInt();
            int unassignedShards = in.readVInt();
            int numberOfNodes = in.readVInt();
            int numberOfDataNodes = in.readVInt();
            numberOfPendingTasks = in.readInt();
            
            // Here's the critical part - read the status byte from 2.x which comes next
            ClusterHealthStatus status = ClusterHealthStatus.fromValue(in.readByte());
            // Set the cluster health status
            clusterHealthStatus = status;
            
            // Read indices (skip for compatibility)
            int size = in.readVInt();
            Map<String, ClusterIndexHealth> indices = new HashMap<>();
            for (int i = 0; i < size; i++) {
                ClusterIndexHealth indexHealth = new ClusterIndexHealth(in);
                indices.put(indexHealth.getIndex(), indexHealth);
            }
            
            timedOut = in.readBoolean();
            
            // Skip validation failures
            size = in.readVInt();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    in.readString();
                }
            }
            
            numberOfInFlightFetch = in.readInt();
            //Assume version is always >= 1.7.0
            delayedUnassignedShards = in.readInt();
            
            // Read active shards percentage
            double activeShardsPercent = in.readDouble();
            
            // Read task max waiting time
            taskMaxWaitingTime = new TimeValue(in);
            
            // Create ClusterStateHealth from the values we read
            clusterStateHealth = new ClusterStateHealth(
                numberOfNodes, numberOfDataNodes, activeShards, relocatingShards, 
                activePrimaryShards, initializingShards, unassignedShards, 
                activeShardsPercent, status, indices
            );
        } else {
            // Current version format
            clusterHealthStatus = ClusterHealthStatus.fromValue(in.readByte());
            clusterStateHealth = new ClusterStateHealth(in);
            numberOfPendingTasks = in.readInt();
            timedOut = in.readBoolean();
            numberOfInFlightFetch = in.readInt();
            delayedUnassignedShards = in.readInt();
            taskMaxWaitingTime = new TimeValue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterName);
        
        if (out.getVersion().before(Version.V_5_0_0_alpha1)) {
            // For 2.x format, we need to write all the fields in the order expected by 2.x nodes
            out.writeVInt(clusterStateHealth.getActivePrimaryShards());
            out.writeVInt(clusterStateHealth.getActiveShards());
            out.writeVInt(clusterStateHealth.getRelocatingShards());
            out.writeVInt(clusterStateHealth.getInitializingShards());
            out.writeVInt(clusterStateHealth.getUnassignedShards());
            out.writeVInt(clusterStateHealth.getNumberOfNodes());
            out.writeVInt(clusterStateHealth.getNumberOfDataNodes());
            out.writeInt(numberOfPendingTasks);
            out.writeByte(clusterHealthStatus.value());
            
            // Write indices
            Map<String, ClusterIndexHealth> indices = clusterStateHealth.getIndices();
            out.writeVInt(indices.size());
            for (ClusterIndexHealth indexHealth : indices.values()) {
                indexHealth.writeTo(out);
            }
            
            out.writeBoolean(timedOut);
            
            // Write empty validation failures
            out.writeVInt(0);
            
            out.writeInt(numberOfInFlightFetch);
            
            // Always write delayedUnassignedShards
            out.writeInt(delayedUnassignedShards);
            
            // Write active shards percent
            out.writeDouble(clusterStateHealth.getActiveShardsPercent());
            
            // Write task max waiting time
            taskMaxWaitingTime.writeTo(out);
        } else {
            // Current version format
            out.writeByte(clusterHealthStatus.value());
            clusterStateHealth.writeTo(out);
            out.writeInt(numberOfPendingTasks);
            out.writeBoolean(timedOut);
            out.writeInt(numberOfInFlightFetch);
            out.writeInt(delayedUnassignedShards);
            taskMaxWaitingTime.writeTo(out);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public RestStatus status() {
        return isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
    }

    private static final String CLUSTER_NAME = "cluster_name";
    private static final String STATUS = "status";
    private static final String TIMED_OUT = "timed_out";
    private static final String NUMBER_OF_NODES = "number_of_nodes";
    private static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    private static final String NUMBER_OF_PENDING_TASKS = "number_of_pending_tasks";
    private static final String NUMBER_OF_IN_FLIGHT_FETCH = "number_of_in_flight_fetch";
    private static final String DELAYED_UNASSIGNED_SHARDS = "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE = "task_max_waiting_in_queue";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = "task_max_waiting_in_queue_millis";
    private static final String ACTIVE_SHARDS_PERCENT_AS_NUMBER = "active_shards_percent_as_number";
    private static final String ACTIVE_SHARDS_PERCENT = "active_shards_percent";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String INDICES = "indices";
    private String level = "cluster";

    public void setLevel(String level) {
        this.level = level;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTER_NAME, getClusterName());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(TIMED_OUT, isTimedOut());
        builder.field(NUMBER_OF_NODES, getNumberOfNodes());
        builder.field(NUMBER_OF_DATA_NODES, getNumberOfDataNodes());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(DELAYED_UNASSIGNED_SHARDS, getDelayedUnassignedShards());
        builder.field(NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());
        builder.field(NUMBER_OF_IN_FLIGHT_FETCH, getNumberOfInFlightFetch());
        builder.timeValueField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS, TASK_MAX_WAIT_TIME_IN_QUEUE, getTaskMaxWaitingTime());
        builder.percentageField(ACTIVE_SHARDS_PERCENT_AS_NUMBER, ACTIVE_SHARDS_PERCENT, getActiveShardsPercent());

        String level = params.param("level", "cluster");
        if(!level.equals(this.level)){
            level=this.level;
        }
        boolean outputIndices = "indices".equals(level) || "shards".equals(level);

        if (outputIndices) {
            builder.startObject(INDICES);
            for (ClusterIndexHealth indexHealth : clusterStateHealth.getIndices().values()) {
                builder.startObject(indexHealth.getIndex());
                indexHealth.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
