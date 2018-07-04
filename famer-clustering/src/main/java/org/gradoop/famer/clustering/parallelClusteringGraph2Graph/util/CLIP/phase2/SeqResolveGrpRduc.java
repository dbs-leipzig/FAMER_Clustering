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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.ConnectedComponents;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc.*;
import org.gradoop.famer.common.functions.Convert2Tuple1;
import org.gradoop.famer.common.functions.Link2link_id;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class SeqResolveGrpRduc implements UnaryGraphToGraphOperator{
    private Double simValueCoef;
    private Double strengthCoef;
    public SeqResolveGrpRduc(Double inputSimValueCoef, Double inputStrengthCoef){
        simValueCoef = inputSimValueCoef;
        strengthCoef = inputStrengthCoef;
    }
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        input = input.callForGraph(new ConnectedComponents());
        DataSet<Tuple3<String, String, String>> vertexId_src_conComId = input.getVertices().map(new Vertex2VertexInfo());
        DataSet<Tuple4<String, String, String, Double>> edgeId_srcId_trgtId_prioValue = input.getEdges().map(new Edge2EdgeInfo(simValueCoef, strengthCoef));
        DataSet<Tuple5<String, String, String, String, Double>> join1Result = vertexId_src_conComId.join(edgeId_srcId_trgtId_prioValue).where(0).equalTo(1).
            with(new Join1());
        DataSet<Tuple7<String, String, String, String, String, String, Double>> srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_prioValue
                = join1Result.join(vertexId_src_conComId).where(3).equalTo(0).with(new Join2());
        DataSet<Tuple1<String>> edgeIds = srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_prioValue.groupBy(2).sortGroup(6, Order.DESCENDING).reduceGroup(new Reducer()).map(new Convert2Tuple1());
        DataSet<Tuple2<Edge, String>> edge_edgeId = input.getEdges().map(new Link2link_id());
        DataSet<Edge> edges = edge_edgeId.join(edgeIds).where(1).equalTo(0).with(new Join());
        input = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
        input = input.callForGraph(new ConnectedComponents("ph3-"));
        return input;
    }

    @Override
    public String getName() {
        return SeqResolveGrpRduc.class.getName();
    }
}
