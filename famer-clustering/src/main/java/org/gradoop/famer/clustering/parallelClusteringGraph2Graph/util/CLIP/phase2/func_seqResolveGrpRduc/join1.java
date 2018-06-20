package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 */
public class join1 implements JoinFunction <Tuple3<String, String, String>, Tuple4<String, String, String, Double>, Tuple5<String, String, String, String, Double>>{
    @Override
    public Tuple5<String, String, String, String, Double> join(Tuple3<String, String, String> in1, Tuple4<String, String, String, Double> in2) throws Exception {
        return Tuple5.of(in1.f1, in2.f0, in2.f1, in2.f2, in2.f3);
    }
}
