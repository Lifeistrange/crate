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

import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class NodeJobsTrackerTest extends CrateUnitTest {

    private NodeJobsTracker nodeJobsTracker;

    @Before
    public void setupJobsTracker() {
        nodeJobsTracker = new NodeJobsTracker();
    }

    @Test
    public void testRegisterJobForNodeUpdatesStats() {
        UUID jobId = UUID.randomUUID();

        nodeJobsTracker.registerJob("node1", jobId);
        assertThat(nodeJobsTracker.getInProgressJobsForNode("node1"), is(1L));
    }

    @Test
    public void testRegisterWithNullNodeDoesNotFail() {
        try {
            nodeJobsTracker.registerJob(null, UUID.randomUUID());
        } catch (Throwable e) {
            fail("Did not expect exception when attempting to register a job against null node");
        }
    }

    @Test
    public void testUnregisterUnrecordedJobDoesNotFail() {
        assertThat(nodeJobsTracker.getInProgressJobsForNode("node1"), is(0L));
    }

    @Test
    public void testUnregisterJobUpdatesStatsForAllInvolvedNode() {
        UUID jobId = UUID.randomUUID();
        nodeJobsTracker.registerJob("node1", jobId);
        nodeJobsTracker.registerJob("node2", jobId);

        nodeJobsTracker.unregisterJob(jobId);

        assertThat(nodeJobsTracker.getInProgressJobsForNode("node1"), is(0L));
        assertThat(nodeJobsTracker.getInProgressJobsForNode("node2"), is(0L));
    }

    @Test
    public void testUnregisterJobForNodeUpdatesJustTheNodeStats() {
        UUID jobId = UUID.randomUUID();
        nodeJobsTracker.registerJob("node1", jobId);
        nodeJobsTracker.registerJob("node2", jobId);

        nodeJobsTracker.unregisterJobFromNode("node1", jobId);

        assertThat(nodeJobsTracker.getInProgressJobsForNode("node1"), is(0L));
        assertThat(nodeJobsTracker.getInProgressJobsForNode("node2"), is(1L));
    }

    @Test
    public void testUnregisterJobForNullNodeDoesNotFail() {
        UUID jobId = UUID.randomUUID();
        nodeJobsTracker.registerJob("node1", jobId);

        try {
            nodeJobsTracker.unregisterJobFromNode(null, jobId);
        } catch (Exception e) {
            fail("Did not expect unregistering a job for a null node to fail ");
        }

        assertThat(nodeJobsTracker.getInProgressJobsForNode("node1"), is(1L));
    }
}
