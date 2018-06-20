package org.gradoop.famer.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.util.functions.removeInterClustersLinks;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class RemoveInterClustersLinks implements UnaryGraphToGraphOperator{
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_trgtVertex = new link2link_srcVertex_trgtVertex(input).execute();
        DataSet<Edge> edges =  link_srcVertex_trgtVertex.flatMap(new removeInterClustersLinks());
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }

    @Override
    public String getName() {
        return "RemoveInterClustersLinks";
    }
}
