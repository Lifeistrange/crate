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

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.ReferenceReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;

import java.util.*;
import java.util.function.Function;

public final class FetchRewriter {

    public static List<Symbol> generateFetchOutputs(FetchDescription fetchDescription) {
        InputColumn fetchId = new InputColumn(0);
        return Lists2.copyAndReplace(
            fetchDescription.postFetchOutputs,
            st -> {
                int indexOf = fetchDescription.preFetchOutputs.indexOf(st);
                if (indexOf == -1) {
                    return ReferenceReplacer.replaceRefs(st, r -> new FetchReference(fetchId, r));
                }
                return new InputColumn(indexOf, fetchDescription.preFetchOutputs.get(indexOf).valueType());
            });
    }

    public final static class FetchDescription {

        private final List<Symbol> preFetchOutputs;
        private final List<Symbol> postFetchOutputs;
        private final Collection<Reference> fetchRefs;

        private FetchDescription(List<Symbol> preFetchOutputs,
                                 List<Symbol> postFetchOutputs,
                                 Collection<Reference> fetchRefs) {
            this.preFetchOutputs = preFetchOutputs;
            this.postFetchOutputs = postFetchOutputs;
            this.fetchRefs = fetchRefs;
        }

        public Collection<Reference> fetchRefs() {
            return fetchRefs;
        }
    }

    public static boolean isFetchFeasible(QuerySpec querySpec) {
        Set<Symbol> querySymbols = extractQuerySymbols(querySpec);
        return FetchFeasibility.isFetchFeasible(querySpec.outputs(), querySymbols);
    }

    private static Set<Symbol> extractQuerySymbols(QuerySpec querySpec) {
        Optional<OrderBy> orderBy = querySpec.orderBy();
        return orderBy.isPresent()
            ? ImmutableSet.copyOf(orderBy.get().orderBySymbols())
            : ImmutableSet.of();
    }

    public static FetchDescription rewrite(QueriedDocTable query) {
        QuerySpec querySpec = query.querySpec();
        Set<Symbol> querySymbols = extractQuerySymbols(querySpec);

        assert FetchFeasibility.isFetchFeasible(querySpec.outputs(), querySymbols)
            : "Fetch rewrite shouldn't be done if it's not feasible";
        Set<Reference> fetchRefs = new LinkedHashSet<>();
        final Reference[] scoreColumn = new Reference[] { null };
        Function<Reference, Symbol> maybeConvertToSourceLookupAndSaveRefs = ref -> {
            if (ref.granularity() == RowGranularity.DOC) {
                if (ref.ident().columnIdent().equals(DocSysColumns.SCORE)) {
                    scoreColumn[0] = ref;
                    return ref;
                }
                Reference reference = DocReferences.toSourceLookup(ref);
                fetchRefs.add(reference);
                return reference;
            }
            return ref;
        };
        List<Symbol> postFetchOutputs = Lists2.copyAndReplace(
            querySpec.outputs(),
            s -> {
                if (querySymbols.contains(s)) {
                    return s;
                }
                return ReferenceReplacer.replaceRefs(s, maybeConvertToSourceLookupAndSaveRefs);
            });

        Reference fetchId = DocSysColumns.forTable(query.tableRelation().tableInfo().ident(), DocSysColumns.FETCHID);
        ArrayList<Symbol> preFetchOutputs = new ArrayList<>(1 + querySymbols.size());
        preFetchOutputs.add(fetchId);
        preFetchOutputs.addAll(querySymbols);
        if (scoreColumn[0] != null) {
            preFetchOutputs.add(scoreColumn[0]);
        }
        querySpec.outputs(preFetchOutputs);
        return new FetchDescription(preFetchOutputs, postFetchOutputs, fetchRefs);
    }
}
