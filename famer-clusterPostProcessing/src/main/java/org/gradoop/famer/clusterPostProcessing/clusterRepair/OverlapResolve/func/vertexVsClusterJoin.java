package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 * .
 */
public class vertexVsClusterJoin implements JoinFunction <Tuple2<Vertex, String>, Tuple2<Cluster, String>, Tuple2 <Vertex, Cluster>> {
    @Override
    public Tuple2<Vertex, Cluster> join(Tuple2<Vertex, String> in1, Tuple2<Cluster, String> in2) throws Exception {
        return Tuple2.of(in1.f0, in2.f0);
    }
}
