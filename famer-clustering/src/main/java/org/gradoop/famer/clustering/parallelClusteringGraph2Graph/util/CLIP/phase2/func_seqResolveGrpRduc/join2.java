package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 */
public class join2 implements JoinFunction <Tuple5<String, String, String, String, Double>, Tuple3<String, String, String>, Tuple7<String, String, String, String, String, String, Double>> {
    @Override
    public Tuple7<String, String, String, String, String, String, Double> join(Tuple5<String, String, String, String, Double> in1, Tuple3<String, String, String> in2) throws Exception {
        return Tuple7.of(in1.f0, in2.f1, in2.f2, in1.f1, in1.f2, in1.f3,
                in1.f4);
    }
}
