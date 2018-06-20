package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class getPairs implements MapFunction <Tuple5<Cluster, Cluster, String, Integer, Double>, Tuple2<Cluster, Cluster>>{
    @Override
    public Tuple2<Cluster, Cluster> map(Tuple5<Cluster, Cluster, String, Integer, Double> value) throws Exception {
        return Tuple2.of(value.f0, value.f1);
    }
}
