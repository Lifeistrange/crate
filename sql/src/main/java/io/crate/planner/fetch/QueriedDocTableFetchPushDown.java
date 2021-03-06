/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.*;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.*;

import static io.crate.planner.fetch.FetchFeasibility.isFetchFeasible;

class QueriedDocTableFetchPushDown {

    private final QuerySpec querySpec;
    private final DocTableRelation tableRelation;
    private LinkedHashMap<Reference, FetchReference> fetchRefs;

    // must not be static as the index could be changed due to usage on e.g. unions
    private final InputColumn docIdCol = new InputColumn(0, DataTypes.LONG);

    QueriedDocTableFetchPushDown(QueriedDocTable relation) {
        this.querySpec = relation.querySpec();
        this.tableRelation = relation.tableRelation();
    }

    InputColumn docIdCol() {
        return docIdCol;
    }

    Collection<Reference> fetchRefs() {
        if (fetchRefs == null || fetchRefs.isEmpty()) {
            return ImmutableList.of();
        }
        return fetchRefs.keySet();
    }

    private FetchReference allocateReference(Reference ref) {
        RowGranularity granularity = ref.granularity();
        if (granularity == RowGranularity.DOC) {
            ref = DocReferences.toSourceLookup(ref);
            if (fetchRefs == null) {
                fetchRefs = new LinkedHashMap<>();
            }
            return toFetchReference(ref, fetchRefs);
        } else {
             assert tableRelation.tableInfo().isPartitioned() && granularity == RowGranularity.PARTITION
                 : "table must be partitioned and granularity must be " + RowGranularity.PARTITION;
            return new FetchReference(docIdCol, ref);
        }
    }

    private FetchReference toFetchReference(Reference ref, LinkedHashMap<Reference, FetchReference> refs) {
        FetchReference fRef = refs.get(ref);
        if (fRef == null) {
            fRef = new FetchReference(docIdCol, ref);
            refs.put(ref, fRef);
        }
        return fRef;
    }

    @Nullable
    QueriedDocTable pushDown() {
        if (querySpec.groupBy().isPresent() || querySpec.having().isPresent() || querySpec.hasAggregates()) {
            return null;
        }

        Optional<OrderBy> orderBy = querySpec.orderBy();
        ImmutableSet<Symbol> querySymbols = orderBy.isPresent()
            ? ImmutableSet.copyOf(orderBy.get().orderBySymbols()) : ImmutableSet.of();

        boolean fetchRequired = isFetchFeasible(querySpec.outputs(), querySymbols);
        if (!fetchRequired) return null;

        // build the subquery
        QuerySpec sub = new QuerySpec();
        Reference docIdReference = DocSysColumns.forTable(tableRelation.tableInfo().ident(), DocSysColumns.FETCHID);

        List<Symbol> outputs = new ArrayList<>();
        if (orderBy.isPresent()) {
            sub.orderBy(orderBy.get());
            outputs.add(docIdReference);
            outputs.addAll(querySymbols);
        } else {
            outputs.add(docIdReference);
        }
        for (Symbol symbol : querySpec.outputs()) {
            if (Symbols.containsColumn(symbol, DocSysColumns.SCORE) && !outputs.contains(symbol)) {
                outputs.add(symbol);
            }
        }
        sub.outputs(outputs);
        QueriedDocTable subRelation = new QueriedDocTable(tableRelation, sub);
        HashMap<Symbol, InputColumn> symbolMap = new HashMap<>(sub.outputs().size());

        int index = 0;
        for (Symbol symbol : sub.outputs()) {
            symbolMap.put(symbol, new InputColumn(index++, symbol.valueType()));
        }

        // push down the where clause
        sub.where(querySpec.where());
        querySpec.where(null);

        ToFetchReferenceVisitor toFetchReferenceVisitor = new ToFetchReferenceVisitor();
        toFetchReferenceVisitor.processInplace(querySpec.outputs(), symbolMap);

        if (orderBy.isPresent()) {
            // replace order by symbols with input columns, we need to copy the order by since it was pushed down to the
            // subquery before
            List<Symbol> newOrderBySymbols = MappingSymbolVisitor.copying().process(orderBy.get().orderBySymbols(), symbolMap);
            querySpec.orderBy(new OrderBy(newOrderBySymbols, orderBy.get().reverseFlags(), orderBy.get().nullsFirst()));
        }

        sub.limit(querySpec.limit());
        sub.offset(querySpec.offset());
        return subRelation;
    }

    private class ToFetchReferenceVisitor extends MappingSymbolVisitor {

        private ToFetchReferenceVisitor() {
            super(ReplaceMode.COPY);
        }

        @Override
        public Symbol visitReference(Reference symbol, Map<? extends Symbol, ? extends Symbol> context) {
            Symbol mapped = context.get(symbol);
            if (mapped != null) {
                return mapped;
            }
            return allocateReference(symbol);
        }
    }
}
