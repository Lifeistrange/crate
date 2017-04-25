/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors how many operations issued from the current node are in progress across the cluster.
 * Note: one job can span multiple nodes.
 */
@Singleton
public class NodeJobsTracker {

    private static final Logger logger = Loggers.getLogger(NodeJobsTracker.class);

    private final Map<String, Long> operationsCountPerNode = new ConcurrentHashMap<>();
    private final Map<UUID, List<String>> jobToNodesMap = new ConcurrentHashMap<>();

    public void registerJob(String nodeId, UUID jobId) {
        if (nodeId != null) {
            jobToNodesMap.compute(jobId, (id, nodeIds) -> {
                if (nodeIds == null) {
                    nodeIds = Collections.synchronizedList(new ArrayList<>());
                }
                nodeIds.add(nodeId);
                return nodeIds;
            });
            operationsCountPerNode.compute(nodeId, (node, count) -> {
                    Long newNodeCount = count == null ? 1L : ++count;
                    return newNodeCount;
                }
            );
        } else {
            logger.warn("Received null node id for job {}", jobId);
        }
    }

    public void unregisterJob(UUID jobId) {
        List<String> nodeIds = jobToNodesMap.remove(jobId);
        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                operationsCountPerNode.compute(nodeId, (id, count) -> {
                    Long newNodeCount = count == null ? 0L : --count;
                    return newNodeCount;
                });
            }
        } else {
            logger.warn("Job {} was not recorded as running on any node", jobId);
        }
    }

    public void unregisterJobFromNode(String nodeId, UUID jobId) {
        if (nodeId == null) {
            logger.warn("Received null nodeId when attempting to unregister job {}", jobId);
            return;
        }

        List<String> nodeIds = jobToNodesMap.get(jobId);
        if (nodeIds != null) {
            Iterator<String> nodesIterator = nodeIds.iterator();
            while (nodesIterator.hasNext()) {
                if (nodesIterator.next().equals(nodeId)) {
                    nodesIterator.remove();
                    break;
                }
            }
            operationsCountPerNode.compute(nodeId, (id, count) -> {
                Long newNodeCount = count == null ? 0L : --count;
                return newNodeCount;
            });
        } else {
            logger.warn("Job {} was not recorded as running on any node", jobId);
        }
    }

    public long getInProgressJobsForNode(String nodeId) {
        Long count = operationsCountPerNode.get(nodeId);
        return count == null ? 0L : count;
    }
}
