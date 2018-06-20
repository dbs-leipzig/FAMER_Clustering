package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class join implements JoinFunction <Tuple2<Edge, String>, Tuple1<String>, Edge> {
    @Override
    public Edge join(Tuple2<Edge, String> in1, Tuple1<String> in2) throws Exception {
        return in1.f0;
    }
}
