/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.export;

import org.cratedb.action.export.parser.ExportParser;
import org.cratedb.export.Exporter;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;


/**
 *
 */
public class TransportExportAction extends AbstractTransportExportAction {

    @Inject
    public TransportExportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                 TransportService transportService, IndicesService indicesService,
                                 ScriptService scriptService, CacheRecycler cacheRecycler,
                                 ExportParser exportParser, Exporter exporter, NodeEnvironment nodeEnv) {
        super(settings, threadPool, clusterService, transportService, indicesService, scriptService,
            cacheRecycler, exportParser, exporter, nodeEnv);
    }

    @Override
    protected String transportAction() {
        return ExportAction.NAME;
    }
}
