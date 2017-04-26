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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.*;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.*;
import io.crate.planner.fetch.FetchRewriter;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.PendingFetch;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private final static class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            QuerySpec qs = table.querySpec();
            if (!isSimpleSelect(qs, context)) {
                return null;
            }
            if (!context.fetchDecider().tryFetchRewrite()) {
                return normalSelect(table, context);
            }
            if (!FetchRewriter.isFetchFeasible(qs)) {
                return normalSelect(table, context);
            }
            FetchRewriter.FetchDescription fetchDescription = FetchRewriter.rewrite(table);
            Planner.Context plannerContext = context.plannerContext();

            Plan subPlan = normalSelect(table, context);
            if (context.fetchDecider().finalizeFetch()) {
                subPlan = Merge.ensureOnHandler(subPlan, plannerContext);
                return planFetch(fetchDescription, plannerContext, subPlan);
            }
            return new PendingFetch(subPlan, fetchDescription);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (!isSimpleSelect(querySpec, context)) {
                return null;
            }
            if (querySpec.where().hasQuery()) {
                NoPredicateVisitor.ensureNoMatchPredicate(querySpec.where().query());
            }
            return normalSelect(table, context);
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec qs = relation.querySpec();
            if (qs.hasAggregates() || qs.groupBy().isPresent()) {
                return null;
            }

            /*
             * select * from (                                                      // parent relation
             *      select x, y, z from (                                           // relation
             *           select x, y, z from t order by x desc limit 100            // relation.subRelation
             *      ) tt
             *      where
             *      tt.x = 10
             *      order by tt.x asc limit 50
             * ) ttt
             * order by ttt.x desc limit 10
             */
            Planner.Context plannerContext = context.plannerContext();
            FetchDecider parentFetchDecider = context.fetchDecider();
            context.setFetchDecider(FetchDecider.NO_FINALIZE);
            QueriedRelation subRelation = relation.subRelation();
            Plan subPlan = plannerContext.planSubRelation(subRelation, context);

            FetchRewriter.FetchDescription fetchDescription = subPlan.resultDescription().fetchDescription();
            subPlan = Merge.ensureOnHandler(subPlan, plannerContext);
            WhereClause where = qs.where();
            Limits limits = plannerContext.getLimits(qs);
            boolean limitAndOrderByApplied = false;

            if (fetchDescription != null) {
                if (where.noMatch()) {
                    subPlan.addProjection(ProjectionBuilder.filterProjection(subRelation.fields(), where), null, null, null);
                    where = WhereClause.MATCH_ALL;
                }
                if (where.hasQuery()) {
                    /* select * from (select y, x from t order by x limit 10) tt where x + x = 2
                     *                       ~~~~                                      ~~~~~~~~~
                     *                       |
                     *                      postFetchOutputs
                     */
                    Symbol queryWithInputCols = fetchDescription.tryCreateInputColumns(where.query());
                    if (queryWithInputCols != null) {
                        FilterProjection filterProjection = new FilterProjection(
                            queryWithInputCols,
                            InputColumn.fromSymbols(fetchDescription.preFetchOutputs()));
                        subPlan.addProjection(filterProjection, null, null, null);
                        where = WhereClause.MATCH_ALL;
                    }
                }

                OrderBy orderBy = qs.orderBy().orElse(null);
                if (orderBy != null) {
                    boolean[] eagerOrderByPossible = new boolean[] { true };
                    FieldsVisitor.visitFields(orderBy.orderBySymbols(), f -> {
                        Symbol symbol = fetchDescription.postFetchOutputs().get(f.index());
                        if (fetchDescription.isFetched(symbol)) {
                            eagerOrderByPossible[0] = false;
                        }
                    });
                    if (eagerOrderByPossible[0]) {
                        limitAndOrderByApplied = true;
                        orderBy.replace(s -> FieldReplacer.replaceFields(s, f -> fetchDescription.postFetchOutputs().get(f.index())));
                        Projection topNOrEval = ProjectionBuilder.topNOrEval(
                            fetchDescription.preFetchOutputs(),
                            orderBy,
                            limits.offset(),
                            limits.finalLimit(),
                            fetchDescription.preFetchOutputs()
                        );
                        subPlan.addProjection(topNOrEval, null, null, null);
                    }
                }

                if (limitAndOrderByApplied
                    && where == WhereClause.MATCH_ALL
                    && parentFetchDecider.tryFetchRewrite()
                    && !parentFetchDecider.finalizeFetch()) {

                    return new PendingFetch(subPlan, fetchDescription);
                }
                subPlan = planFetch(fetchDescription, plannerContext, subPlan);
            }

            maybeAddFilterProjection(where, subRelation.fields(), subPlan);
            if (!limitAndOrderByApplied) {
                Projection topN = ProjectionBuilder.topNOrEval(
                    subRelation.fields(),
                    qs.orderBy().orElse(null),
                    limits.offset(),
                    limits.finalLimit(),
                    qs.outputs()
                );
                subPlan.addProjection(topN, null, null, null);
            }
            return subPlan;
        }

    }

    private static Plan planFetch(FetchRewriter.FetchDescription fetchDescription, Planner.Context plannerContext, Plan subPlan) {
        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        FetchPhase fetchPhase = new FetchPhase(
            plannerContext.nextExecutionPhaseId(),
            readerAllocations.nodeReaders().keySet(),
            readerAllocations.bases(),
            readerAllocations.tableIndices(),
            fetchDescription.fetchRefs()
        );
        InputColumn fetchId = new InputColumn(0);
        FetchSource fetchSource = new FetchSource(
            fetchDescription.partitionedByColumns(),
            Collections.singletonList(fetchId),
            fetchDescription.fetchRefs()
        );
        FetchProjection fetchProjection = new FetchProjection(
            fetchPhase.phaseId(),
            plannerContext.fetchSize(),
            ImmutableMap.of(fetchDescription.tableIdent(), fetchSource),
            FetchRewriter.generateFetchOutputs(fetchDescription),
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        subPlan.addProjection(fetchProjection, null, null, null);
        return new QueryThenFetch(subPlan, fetchPhase);
    }

    private static void maybeAddFilterProjection(WhereClause whereClause, List<? extends Symbol> subRelationOutputs, Plan plan) {
        if (whereClause.hasQuery() || whereClause.noMatch()) {
            FilterProjection filterProjection = ProjectionBuilder.filterProjection(subRelationOutputs, whereClause);
            plan.addProjection(filterProjection, null, null, null);
        }
    }

    private static boolean isSimpleSelect(QuerySpec querySpec, ConsumerContext context) {
        if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
            return false;
        }
        if (querySpec.where().hasVersions()) {
            context.validationException(new VersionInvalidException());
            return false;
        }
        return true;
    }

    /**
     * Create a (distributed if possible) Collect plan.
     *
     * Data will be pre-sorted if there is a ORDER BY clause.
     * Limit (incl offset) will be pre-applied.
     *
     * Both ORDER-BY & Limit needs to be "finalized" in a Merge-to-handler.
     */
    private static Collect normalSelect(QueriedTableRelation table, ConsumerContext context) {
        QuerySpec querySpec = table.querySpec();
        Planner.Context plannerContext = context.plannerContext();
        Optional<OrderBy> optOrderBy = querySpec.orderBy();
        /*
         * ORDER BY columns are added to OUTPUTS - they're required to do an ordered merge.
         *
         * select name order by date
         *
         * qsOutputs:           [name]
         * toCollect:           [name, date]           // includes order by symbols, that aren't already selected
         */
        List<Symbol> qsOutputs = querySpec.outputs();
        List<Symbol> toCollect = getToCollectSymbols(qsOutputs, optOrderBy);
        table.tableRelation().validateOrderBy(optOrderBy);

        Limits limits = plannerContext.getLimits(querySpec);
        RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
            plannerContext,
            table,
            toCollect,
            topNOrEmptyProjections(toCollect, limits)
        );
        tryApplySizeHint(context.requiredPageSize(), limits, collectPhase);
        collectPhase.orderBy(optOrderBy.orElse(null));
        return new Collect(
            collectPhase,
            limits.finalLimit(),
            limits.offset(),
            qsOutputs.size(),
            limits.limitAndOffset(),
            PositionalOrderBy.of(optOrderBy.orElse(null), toCollect)
        );
    }

    private static void tryApplySizeHint(@Nullable Integer requiredPageSize, Limits limits, RoutedCollectPhase collectPhase) {
        if (requiredPageSize == null) {
            if (limits.hasLimit()) {
                collectPhase.nodePageSizeHint(limits.limitAndOffset());
            }
        } else {
            collectPhase.pageSizeHint(requiredPageSize);
        }
    }

    private static List<Projection> topNOrEmptyProjections(List<Symbol> toCollect, Limits limits) {
        if (limits.hasLimit()) {
            return Collections.singletonList(new TopNProjection(
                limits.limitAndOffset(),
                0,
                InputColumn.fromSymbols(toCollect)
            ));
        }
        return Collections.emptyList();
    }

    /**
     * @return qsOutputs + symbols from orderBy which are not already within qsOutputs (if orderBy is present)
     */
    private static List<Symbol> getToCollectSymbols(List<Symbol> qsOutputs, Optional<OrderBy> optOrderBy) {
        if (optOrderBy.isPresent()) {
            return Lists2.concatUnique(qsOutputs, optOrderBy.get().orderBySymbols());
        }
        return qsOutputs;
    }

    private final static class NoPredicateVisitor extends SymbolVisitor<Void, Void> {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        private NoPredicateVisitor() {
        }

        static void ensureNoMatchPredicate(Symbol symbolTree) {
            NO_PREDICATE_VISITOR.process(symbolTree, null);
        }

        @Override
        public Void visitFunction(Function symbol, Void context) {
            if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
                throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
            }
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return null;
        }
    }
}
