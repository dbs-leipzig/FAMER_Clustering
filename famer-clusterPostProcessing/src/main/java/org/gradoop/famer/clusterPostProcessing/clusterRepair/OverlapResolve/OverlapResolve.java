/*
 * Copyright © 2016 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func.*;
import org.gradoop.famer.common.functions.GetF0Tuple2;
import org.gradoop.famer.common.functions.Vertex2vertex_clusterId;
import org.gradoop.famer.common.functions.Vertex_T2vertexId_vertex_T;
import org.gradoop.famer.common.maxDeltaLinkSelection.MaxLinkSrcSelection;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.Cluster2cluster_clusterId;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class OverlapResolve implements UnaryGraphToGraphOperator {
    private Double delta;
    public OverlapResolve (Double inputDelta){delta = inputDelta;}
    @Override
    public LogicalGraph execute(LogicalGraph clusteredGraph) {
        DataSet<Vertex> vertices = clusteredGraph.getVertices();
        // filter out normal, weak links
        clusteredGraph = clusteredGraph.callForGraph(new MaxLinkSrcSelection(delta));
        DataSet<Edge> edges = clusteredGraph.getEdges().flatMap(new FilterOutStrong());


        // convert to cluster collection
        ClusterCollection cc = new ClusterCollection(vertices, edges);
        DataSet<Cluster> clusters = cc.getClusterCollection();

//
/*Phase 1:*/
        // find to be overlapped vertices and associated clusters
        DataSet<Tuple2<Cluster, String>> cluster_clusterId = clusters.map(new Cluster2cluster_clusterId());
        DataSet<Vertex> toBeResolvedVertices = vertices.flatMap(new FilterOverlappedVertices());
        DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new Vertex2vertex_clusterId(true));
        DataSet<Tuple3<String, Vertex, Cluster>> toBeResolvedVertexId_vertex_cluster = toBeResolvedVertex_clusterId.
                join(cluster_clusterId).where(1).equalTo(1).with(new VertexVsClusterJoin()).map(new Vertex_T2vertexId_vertex_T());

        // process overlapped vertices
        DataSet<Tuple3<Vertex, String, String>> vertex_oldClsId_newClsId = toBeResolvedVertexId_vertex_cluster.groupBy(0)
                .reduceGroup(new NoMergeResolver(ResolveIteration.ITERATION1));

        // update clusters
        DataSet<Tuple3<Cluster, String, String>> updated = vertex_oldClsId_newClsId.groupBy(1).reduceGroup(new IntegrateVertices());
        DataSet<Cluster> singletons = vertex_oldClsId_newClsId.flatMap(new FormSingletons());
        clusters = cluster_clusterId.leftOuterJoin(updated).where(1).equalTo(1).with(new UpdateJoin(ResolveIteration.ITERATION1)).union(singletons);
        clusters = clusters.map(new Cluster2cluster_clusterId()).groupBy(1).reduceGroup(new UpdateClusterLinks());

        // update vertices
        vertices = clusters.flatMap(new Cluster2vertex_vertexId()).distinct(1).map(new GetF0Tuple2<>());


///*Phase 2:*/

        cluster_clusterId = clusters.map(new Cluster2cluster_clusterId());
        toBeResolvedVertices = vertices.flatMap(new FilterOverlappedVertices());
        toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new Vertex2vertex_clusterId(true));
        toBeResolvedVertexId_vertex_cluster = toBeResolvedVertex_clusterId.
                join(cluster_clusterId).where(1).equalTo(1).with(new VertexVsClusterJoin()).map(new Vertex_T2vertexId_vertex_T());

        // process overlapped vertices
        vertex_oldClsId_newClsId = toBeResolvedVertexId_vertex_cluster.groupBy(0)
                .reduceGroup(new NoMergeResolver(ResolveIteration.ITERATION2));

        // update clusters
        updated = vertex_oldClsId_newClsId.groupBy(1).reduceGroup(new IntegrateVertices());
        singletons = vertex_oldClsId_newClsId.flatMap(new FormSingletons());
        clusters = cluster_clusterId.leftOuterJoin(updated).where(1).equalTo(1).with(new UpdateJoin(ResolveIteration.ITERATION2)).union(singletons);
        clusters = clusters.map(new Cluster2cluster_clusterId()).groupBy(1).reduceGroup(new UpdateClusterLinks());

        // update vertices
        vertices = clusters.flatMap(new Cluster2vertex_vertexId()).distinct(1).map(new GetF0Tuple2<>());

        //output
        clusteredGraph =  clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
        return clusteredGraph.callForGraph(new RemoveInterClustersLinks());
    }

    @Override
    public String getName() {
        return OverlapResolve.class.getName();
    }



}
