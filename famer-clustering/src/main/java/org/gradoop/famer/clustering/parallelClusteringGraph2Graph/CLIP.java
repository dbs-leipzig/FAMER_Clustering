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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.CLIPConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func.AddFakeEdge;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func.FilterSrcConsistentClusterVertices;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func.Link2link_endId;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.SeqResolveGrpRduc;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.GenerateOutput;
import org.gradoop.famer.common.FilterOutLinks.functions.FilterOutSpecificLinks;
import org.gradoop.famer.common.functions.GetF0Tuple2;
import org.gradoop.famer.common.functions.Vertex2vertex_clusterId;
import org.gradoop.famer.common.functions.Vertex2vertex_gradoopId;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.FindMax2;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.Link2link_endId_endType;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.MakeEdgeWithSelectedStatus2;
import org.gradoop.famer.common.util.Link2link_srcVertex_trgtVertex;
import org.gradoop.famer.common.util.Minus;
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

        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new Link2link_srcVertex_trgtVertex(input).execute();

        DataSet<Tuple3<Edge, String, String>> link_endId_endType = link_srcVertex_targetVertex.flatMap(new Link2link_endId_endType());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_endId_endType.groupBy(1,2).reduceGroup(new FindMax2(clipConfig.getDelta()));
        DataSet<Edge> allEdgesWithSelectedStatus = edges_edgeId_isSelected.groupBy(1).reduceGroup(new MakeEdgeWithSelectedStatus2());

        DataSet<Edge> strongEdges = allEdgesWithSelectedStatus.flatMap(new FilterOutSpecificLinks(1));

        // remove vertices and edges of complete clusters
        LogicalGraph temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getGraphHead(), input.getVertices(), strongEdges);
        temp = temp.callForGraph(new ConnectedComponents("ph1-"));
        DataSet<Tuple2<Vertex, String>> completeVerticesWithId = temp.getVertices().flatMap(new Vertex2vertex_clusterId(false)).groupBy(1)
                .reduceGroup(new FilterSrcConsistentClusterVertices(clipConfig.getSourceNo())).map(new Vertex2vertex_gradoopId());

        // remove complete vertices
        DataSet<Tuple2<Vertex, String>> allVerticesWithId = input.getVertices().map(new Vertex2vertex_gradoopId());
        DataSet<Vertex> remainingVertices = new Minus().execute(allVerticesWithId, completeVerticesWithId);

        // remove edges of complete clusters or related to complete clusters
        DataSet<Tuple2<Edge,String>> completeVerticesIds_FakeEdge = completeVerticesWithId.map(new AddFakeEdge());

        DataSet<Tuple2<Edge, String>> allNonWeakEdgesWithSrcId = (allEdgesWithSelectedStatus.flatMap(new FilterOutSpecificLinks(0))).map(new Link2link_endId(0));
        DataSet<Edge> NonWeakEdges_temp = new org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.util.Minus().execute(allNonWeakEdgesWithSrcId, completeVerticesIds_FakeEdge);
        DataSet<Tuple2<Edge, String>> NonWeakEdges_tempWithTrgtId = NonWeakEdges_temp.map(new Link2link_endId(1));
        DataSet<Edge> remainingEdges = new org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.util.Minus().execute(NonWeakEdges_tempWithTrgtId, completeVerticesIds_FakeEdge);

        temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getGraphHead(), remainingVertices, remainingEdges);

        /////////////////////////////////////
        if (clipConfig.isRemoveSrcConsistentVertices()) {

            temp = temp.callForGraph(new ConnectedComponents("ph2-"));
            DataSet<Tuple2<Vertex, String>> srcConsistentVerticesWithId = temp.getVertices().flatMap(new Vertex2vertex_clusterId(false)).groupBy(1)
                    .reduceGroup(new FilterSrcConsistentClusterVertices()).map(new Vertex2vertex_gradoopId());
            allVerticesWithId = temp.getVertices().map(new Vertex2vertex_gradoopId());
            remainingVertices = new Minus().execute(allVerticesWithId, srcConsistentVerticesWithId);
            DataSet<Tuple2<Edge, String>> srcConsistentVerticesIds_FakeEdge = srcConsistentVerticesWithId.map(new AddFakeEdge());
            allNonWeakEdgesWithSrcId = remainingEdges.map(new Link2link_endId(0));
            NonWeakEdges_temp = new org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.util.Minus().execute(allNonWeakEdgesWithSrcId, srcConsistentVerticesIds_FakeEdge);
            NonWeakEdges_tempWithTrgtId = NonWeakEdges_temp.map(new Link2link_endId(1));
            remainingEdges = new org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.util.Minus().execute(NonWeakEdges_tempWithTrgtId, srcConsistentVerticesIds_FakeEdge);

            completeVerticesWithId = completeVerticesWithId.union(srcConsistentVerticesWithId);
            temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getGraphHead(), remainingVertices, remainingEdges);
        }
        /////////////////////////////////////

        temp = temp.callForGraph(new SeqResolveGrpRduc(clipConfig.getSimValueCoef(), clipConfig.getStrengthCoef()));

        input = input.getConfig().getLogicalGraphFactory().fromDataSets(temp.getVertices().union(completeVerticesWithId.map(new GetF0Tuple2())), input.getEdges());
        input = input.callForGraph(new GenerateOutput(clusteringOutputType));
        return input;
    }

    @Override
    public String getName() {
        return CLIP.class.getName();
    }
}
