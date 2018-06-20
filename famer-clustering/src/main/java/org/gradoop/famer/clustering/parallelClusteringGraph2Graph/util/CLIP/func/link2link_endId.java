package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class link2link_endId implements MapFunction <Edge, Tuple2<Edge, String>>{
    private Integer endType;
    public link2link_endId(Integer inputEndType){endType = inputEndType;}
    @Override
    public Tuple2<Edge, String> map(Edge edge) throws Exception {
        if (endType == 0)
            return Tuple2.of(edge, edge.getSourceId().toString());
        else
            return Tuple2.of(edge, edge.getTargetId().toString());
    }
}
