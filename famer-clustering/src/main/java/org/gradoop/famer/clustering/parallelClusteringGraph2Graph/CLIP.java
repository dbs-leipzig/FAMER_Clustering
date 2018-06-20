package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.CLIPConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.Phase2Method;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func.addFakeEdge;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func.filterSrcConsistentClusterVertices;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func.link2link_endId;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.resolveBulkIteration;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.seqResolveGrpRduc;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.util.Minus;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.GenerateOutput;
import org.gradoop.famer.common.FilterOutLinks.functions.FilterOutSpecificLinks;
import org.gradoop.famer.common.functions.getF0Tuple2;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.famer.common.functions.vertex2vertex_gradoopId;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.findMax2;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.link2link_endId_endType;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.makeEdgeWithSelectedStatus2;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.famer.common.util.minus;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class CLIP implements UnaryGraphToGraphOperator{
    private CLIPConfig clipConfig;
    private static ClusteringOutputType clusteringOutputType;

    public CLIP(CLIPConfig clipConfig, ClusteringOutputType clusteringOutputType){
        this.clipConfig = clipConfig;
        this.clusteringOutputType = clusteringOutputType;
    }



    @Override
    public LogicalGraph execute(LogicalGraph input) {

        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new link2link_srcVertex_trgtVertex(input).execute();

        DataSet<Tuple3<Edge, String, String>> link_endId_endType = link_srcVertex_targetVertex.flatMap(new link2link_endId_endType());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_endId_endType.groupBy(1,2).reduceGroup(new findMax2(clipConfig.getDelta()));
        DataSet<Edge> allEdgesWithSelectedStatus = edges_edgeId_isSelected.groupBy(1).reduceGroup(new makeEdgeWithSelectedStatus2());

        DataSet<Edge> strongEdges = allEdgesWithSelectedStatus.flatMap(new FilterOutSpecificLinks(1));

        // remove vertices and edges of complete clusters
        LogicalGraph temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getGraphHead(), input.getVertices(), strongEdges);
        temp = temp.callForGraph(new ConnectedComponents("ph1-"));
        DataSet<Tuple2<Vertex, String>> completeVerticesWithId = temp.getVertices().flatMap(new vertex2vertex_clusterId(false)).groupBy(1)
                .reduceGroup(new filterSrcConsistentClusterVertices(clipConfig.getSourceNo())).map(new vertex2vertex_gradoopId());

        // remove complete vertices
        DataSet<Tuple2<Vertex, String>> allVerticesWithId = input.getVertices().map(new vertex2vertex_gradoopId());
        DataSet<Vertex> remainingVertices = new minus().execute(allVerticesWithId, completeVerticesWithId);

        // remove edges of complete clusters or related to complete clusters
        DataSet<Tuple2<Edge,String>> completeVerticesIds_FakeEdge = completeVerticesWithId.map(new addFakeEdge());

        DataSet<Tuple2<Edge, String>> allNonWeakEdgesWithSrcId = (allEdgesWithSelectedStatus.flatMap(new FilterOutSpecificLinks(0))).map(new link2link_endId(0));
        DataSet<Edge> NonWeakEdges_temp = new Minus().execute(allNonWeakEdgesWithSrcId, completeVerticesIds_FakeEdge);
        DataSet<Tuple2<Edge, String>> NonWeakEdges_tempWithTrgtId = NonWeakEdges_temp.map(new link2link_endId(1));
        DataSet<Edge> remainingEdges = new Minus().execute(NonWeakEdges_tempWithTrgtId, completeVerticesIds_FakeEdge);

        temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getGraphHead(), remainingVertices, remainingEdges);

        /////////////////////////////////////
        if (clipConfig.isRemoveSrcConsistentVertices()) {

            temp = temp.callForGraph(new ConnectedComponents("ph2-"));
            DataSet<Tuple2<Vertex, String>> srcConsistentVerticesWithId = temp.getVertices().flatMap(new vertex2vertex_clusterId(false)).groupBy(1)
                    .reduceGroup(new filterSrcConsistentClusterVertices()).map(new vertex2vertex_gradoopId());
            allVerticesWithId = temp.getVertices().map(new vertex2vertex_gradoopId());
            remainingVertices = new minus().execute(allVerticesWithId, srcConsistentVerticesWithId);
            DataSet<Tuple2<Edge, String>> srcConsistentVerticesIds_FakeEdge = srcConsistentVerticesWithId.map(new addFakeEdge());
            allNonWeakEdgesWithSrcId = remainingEdges.map(new link2link_endId(0));
            NonWeakEdges_temp = new Minus().execute(allNonWeakEdgesWithSrcId, srcConsistentVerticesIds_FakeEdge);
            NonWeakEdges_tempWithTrgtId = NonWeakEdges_temp.map(new link2link_endId(1));
            remainingEdges = new Minus().execute(NonWeakEdges_tempWithTrgtId, srcConsistentVerticesIds_FakeEdge);

            completeVerticesWithId = completeVerticesWithId.union(srcConsistentVerticesWithId);
            temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getGraphHead(), remainingVertices, remainingEdges);
        }
        /////////////////////////////////////

        if (clipConfig.getPhase2Method().equals(Phase2Method.BULK_ITERATION))
            temp = temp.callForGraph(new resolveBulkIteration(2, clipConfig.getSimValueCoef(),clipConfig.getDegreeCoef(), clipConfig.getStrengthCoef()));
        else if (clipConfig.getPhase2Method().equals(Phase2Method.SEQUENTIAL_GROUPREDUCE))
            temp = temp.callForGraph(new seqResolveGrpRduc(clipConfig.getSimValueCoef(), clipConfig.getStrengthCoef()));

        input = input.getConfig().getLogicalGraphFactory().fromDataSets(temp.getVertices().union(completeVerticesWithId.map(new getF0Tuple2())), input.getEdges());
        input = input.callForGraph(new GenerateOutput(clusteringOutputType));
        return input;
    }

    @Override
    public String getName() {
        return CLIP.class.getName();
    }
}
