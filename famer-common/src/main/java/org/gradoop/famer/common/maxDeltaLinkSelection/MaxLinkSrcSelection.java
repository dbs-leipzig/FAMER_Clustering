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

package org.gradoop.famer.common.maxDeltaLinkSelection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.FindMax2;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.Link2link_endId_endType;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.MakeEdgeWithSelectedStatus2;
import org.gradoop.famer.common.util.Link2link_srcVertex_trgtVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class MaxLinkSrcSelection implements UnaryGraphToGraphOperator {
    private Double delta;

    @Override
    public String getName() {
        return "MaxLinkSrcSelection";
    }
    public MaxLinkSrcSelection(Double Delta) {delta = Delta;}

    @Override
    public LogicalGraph execute (LogicalGraph input) {

        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new Link2link_srcVertex_trgtVertex(input).execute();

        DataSet<Tuple3<Edge, String, String>> link_endId_endType = link_srcVertex_targetVertex.flatMap(new Link2link_endId_endType());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_endId_endType.groupBy(1,2).reduceGroup(new FindMax2(delta));
        DataSet<Edge> edges = edges_edgeId_isSelected.groupBy(1).reduceGroup(new MakeEdgeWithSelectedStatus2());
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }
}
