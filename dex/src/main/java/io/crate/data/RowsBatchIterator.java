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

package io.crate.data;

import com.google.common.annotations.VisibleForTesting;
import io.crate.concurrent.CompletableFutures;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation that is backed by {@link Iterable<Row>}.
 *
 * Use this class only if the backing iterable emits different row objects. Otherwise consider the usage of {@link
 * IterableControlledBatchIterator} instead.
 */
public class RowsBatchIterator implements BatchIterator {

    private final Iterable<Row> rows;
    private Iterator<Row> it;
    private Row currentRow = RowBridging.OFF_ROW;
    private final Columns rowData;

    public static BatchIterator newInstance(Iterable<Row> rows, int numCols) {
        return new CloseAssertingBatchIterator(new RowsBatchIterator(rows, numCols));
    }

    @VisibleForTesting
    RowsBatchIterator(Iterable<Row> rows, int numCols) {
        rowData = RowBridging.toInputs(() -> currentRow, numCols);
        this.rows = rows;
        this.it = rows.iterator();
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        it = rows.iterator();
        currentRow = RowBridging.OFF_ROW;
    }

    @Override
    public boolean moveNext() {
        if (it.hasNext()) {
            currentRow = it.next();
            return true;
        }
        currentRow = RowBridging.OFF_ROW;
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }
}