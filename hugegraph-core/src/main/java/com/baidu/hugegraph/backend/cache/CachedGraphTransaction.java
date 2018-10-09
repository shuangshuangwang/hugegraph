/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.CachedBackendStore.QueryId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.ConditionQueryFlatten;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.google.common.collect.ImmutableList;

public class CachedGraphTransaction extends GraphTransaction {

    private final static int MAX_CACHE_EDGES_PER_QUERY = 100;

    private final Cache verticesCache;
    private final Cache edgesCache;

    public CachedGraphTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        HugeConfig conf = graph.configuration();

        int capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", capacity, expire);

        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", capacity, expire);
    }

    private Cache cache(String prefix, int capacity, long expire) {
        String name = prefix + "-" + super.graph().name();
        Cache cache = CacheManager.instance().cache(name, capacity);
        cache.expire(expire);
        return cache;
    }

    @Override
    public Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVerticesByIds((IdQuery) query);
        }
        // just for debug
        else if (query instanceof ConditionQuery){
            ExtendableIterator<HugeVertex> rs = new ExtendableIterator<>();
            for (ConditionQuery cq: ConditionQueryFlatten.flatten(
                                    (ConditionQuery) query)) {
                Query q = this.optimizeQuery(cq);
                if (!q.ids().isEmpty()) {
                    rs.extend(this.queryVerticesByIds((IdQuery) q));
                } else if (!q.empty()) {
                    rs.extend(super.queryVerticesFromBackend(q));
                }
            }
            return rs;
        // just for debug
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query) {
        IdQuery newQuery = new IdQuery(HugeType.VERTEX, query);
        List<HugeVertex> vertices = new ArrayList<>(query.ids().size());
        for (Id vertexId : query.ids()) {
            Object vertex = this.verticesCache.get(vertexId);
            if (vertex != null) {
                vertices.add((HugeVertex) vertex);
            } else {
                newQuery.query(vertexId);
            }
        }
        if (vertices.isEmpty()) {
            newQuery = query;
        }
        if (!newQuery.empty()) {
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(newQuery);
            while (rs.hasNext()) {
                HugeVertex vertex = rs.next();
                vertices.add(vertex);
                this.verticesCache.update(vertex.id(), vertex);
            }
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        if (query.empty()) {
            // Query all edges, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        Id id = new QueryId(query);
        @SuppressWarnings("unchecked")
        List<HugeEdge> edges = (List<HugeEdge>) this.edgesCache.get(id);
        if (edges == null) {
            // Iterator can't be cached, caching list instead
            edges = ImmutableList.copyOf(super.queryEdgesFromBackend(query));
            if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
                this.edgesCache.update(id, edges);
            }
        }
        return edges.iterator();
    }

    @Override
    public HugeVertex addVertex(HugeVertex vertex) {
        // Update vertex cache
        this.verticesCache.invalidate(vertex.id());

        return super.addVertex(vertex);
    }

    @Override
    public void removeVertex(HugeVertex vertex) {
        // Update vertex cache
        this.verticesCache.invalidate(vertex.id());

        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeVertex(vertex);
    }

    @Override
    public <V> void addVertexProperty(HugeVertexProperty<V> prop) {
        super.addVertexProperty(prop);

        // Update vertex cache
        HugeVertex vertex = prop.element();
        this.verticesCache.updateIfPresent(vertex.id(), vertex);

    }

    @Override
    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        super.removeVertexProperty(prop);

        // Update vertex cache
        HugeVertex vertex = prop.element();
        this.verticesCache.updateIfPresent(vertex.id(), vertex);
    }

    @Override
    public void commit() throws BackendException {
        if (!this.edgesInTx().isEmpty()) {
            // TODO: Use a more precise strategy to update the edge cache
            this.edgesCache.clear();
        }
        super.commit();
    }

    @Override
    public void rollback() throws BackendException {
        // Update vertex cache
        for (Id id : this.verticesInTx()) {
            this.verticesCache.invalidate(id);
        }

        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.rollback();
    }

    @Override
    public void removeIndex(IndexLabel indexLabel) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeIndex(indexLabel);
    }
}
