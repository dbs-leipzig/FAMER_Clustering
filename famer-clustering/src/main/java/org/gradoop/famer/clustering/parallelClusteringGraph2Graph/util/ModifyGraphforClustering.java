package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Used to assign Vertex Priority to vertices.
 * It must be used before applying org.gradoop.famer.clustering.clustering algorithms on graphs
 * Notice: VertexPriority must not be 0
 */
public class ModifyGraphforClustering implements UnaryGraphToGraphOperator {
    public String getName() {
        return ModifyGraphforClustering.class.getName();
    }
    public LogicalGraph execute(final LogicalGraph input) {
        DataSet<Vertex> vertices = DataSetUtils.zipWithUniqueId(input.getVertices()).map(new MapFunction<Tuple2<Long, Vertex>, Vertex>() {
            public Vertex map(Tuple2<Long, Vertex> in) throws Exception {
                in.f1.setProperty("VertexPriority", in.f0+1);
                return in.f1;
            }
        });
        return input.getConfig().getLogicalGraphFactory().fromDataSets(vertices, input.getEdges());
    }

}
