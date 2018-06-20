package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func.*;
import org.gradoop.famer.common.functions.getF0Tuple2;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.famer.common.functions.vertex_T2vertexId_vertex_T;
import org.gradoop.famer.common.maxDeltaLinkSelection.maxLinkSrcSelection;
import org.gradoop.famer.common.model.impl.Cluster;
import org.gradoop.famer.common.model.impl.ClusterCollection;
import org.gradoop.famer.common.model.impl.functions.cluster2cluster_clusterId;
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
        clusteredGraph = clusteredGraph.callForGraph(new maxLinkSrcSelection(delta));
        DataSet<Edge> edges = clusteredGraph.getEdges().flatMap(new filterOutStrong());


        // convert to cluster collection
        ClusterCollection cc = new ClusterCollection(vertices, edges);
        DataSet<Cluster> clusters = cc.getClusterCollection();

//
/*Phase 1:*/
        // find to be overlapped vertices and associated clusters
        DataSet<Tuple2<Cluster, String>> cluster_clusterId = clusters.map(new cluster2cluster_clusterId());
        DataSet<Vertex> toBeResolvedVertices = vertices.flatMap(new filterOverlappedVertices());
        DataSet<Tuple2<Vertex, String>> toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
        DataSet<Tuple3<String, Vertex, Cluster>> toBeResolvedVertexId_vertex_cluster = toBeResolvedVertex_clusterId.
                join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin()).map(new vertex_T2vertexId_vertex_T());

        // process overlapped vertices
        DataSet<Tuple3<Vertex, String, String>> vertex_oldClsId_newClsId = toBeResolvedVertexId_vertex_cluster.groupBy(0)
                .reduceGroup(new NoMergeResolver(ResolveIteration.ITERATION1));

        // update clusters
        DataSet<Tuple3<Cluster, String, String>> updated = vertex_oldClsId_newClsId.groupBy(1).reduceGroup(new integrateVertices());
        DataSet<Cluster> singletons = vertex_oldClsId_newClsId.flatMap(new formSingletons());
        clusters = cluster_clusterId.leftOuterJoin(updated).where(1).equalTo(1).with(new updateJoin(ResolveIteration.ITERATION1)).union(singletons);
        clusters = clusters.map(new cluster2cluster_clusterId()).groupBy(1).reduceGroup(new updateClusterLinks());

        // update vertices
        vertices = clusters.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());


///*Phase 2:*/

        cluster_clusterId = clusters.map(new cluster2cluster_clusterId());
        toBeResolvedVertices = vertices.flatMap(new filterOverlappedVertices());
        toBeResolvedVertex_clusterId = toBeResolvedVertices.flatMap(new vertex2vertex_clusterId(true));
        toBeResolvedVertexId_vertex_cluster = toBeResolvedVertex_clusterId.
                join(cluster_clusterId).where(1).equalTo(1).with(new vertexVsClusterJoin()).map(new vertex_T2vertexId_vertex_T());

        // process overlapped vertices
        vertex_oldClsId_newClsId = toBeResolvedVertexId_vertex_cluster.groupBy(0)
                .reduceGroup(new NoMergeResolver(ResolveIteration.ITERATION2));

        // update clusters
        updated = vertex_oldClsId_newClsId.groupBy(1).reduceGroup(new integrateVertices());
        singletons = vertex_oldClsId_newClsId.flatMap(new formSingletons());
        clusters = cluster_clusterId.leftOuterJoin(updated).where(1).equalTo(1).with(new updateJoin(ResolveIteration.ITERATION2)).union(singletons);
        clusters = clusters.map(new cluster2cluster_clusterId()).groupBy(1).reduceGroup(new updateClusterLinks());

        // update vertices
        vertices = clusters.flatMap(new cluster2vertex_vertexId()).distinct(1).map(new getF0Tuple2<>());

        //output
        clusteredGraph =  clusteredGraph.getConfig().getLogicalGraphFactory().fromDataSets(vertices, edges);
        return clusteredGraph.callForGraph(new RemoveInterClustersLinks());
    }

    @Override
    public String getName() {
        return OverlapResolve.class.getName();
    }



}
