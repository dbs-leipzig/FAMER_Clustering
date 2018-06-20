package org.gradoop.famer.common.maxDeltaLinkSelection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.findMax;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.findMax2;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.link2link_endId_endType;
import org.gradoop.famer.common.maxDeltaLinkSelection.functions.makeEdgeWithSelectedStatus2;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class maxLinkSrcSelection implements UnaryGraphToGraphOperator {
    private Double delta;

    @Override
    public String getName() {
        return "maxLinkSrcSelection";
    }
    public maxLinkSrcSelection (Double Delta) {delta = Delta;}

    @Override
    public LogicalGraph execute (LogicalGraph input) {

        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new link2link_srcVertex_trgtVertex(input).execute();

        DataSet<Tuple3<Edge, String, String>> link_endId_endType = link_srcVertex_targetVertex.flatMap(new link2link_endId_endType());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_endId_endType.groupBy(1,2).reduceGroup(new findMax2(delta));
        DataSet<Edge> edges = edges_edgeId_isSelected.groupBy(1).reduceGroup(new makeEdgeWithSelectedStatus2());
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }
}
