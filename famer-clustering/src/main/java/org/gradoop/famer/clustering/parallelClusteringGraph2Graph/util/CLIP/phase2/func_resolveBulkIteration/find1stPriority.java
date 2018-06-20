package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class find1stPriority implements GroupReduceFunction <Tuple5<Cluster, Cluster, String, Integer, Double>, Tuple5<Cluster, Cluster, String, Integer, Double>>{
    @Override
    public void reduce(Iterable<Tuple5<Cluster, Cluster, String, Integer, Double>> values, Collector<Tuple5<Cluster, Cluster, String, Integer, Double>> out) throws Exception {
        Tuple5<Cluster, Cluster, String, Integer, Double> minDegreeMaxSimDegree = Tuple5.of(null, null, "", Integer.MAX_VALUE, Double.MIN_VALUE);
        for (Tuple5<Cluster, Cluster, String, Integer, Double> value : values){
            if (value.f3 < minDegreeMaxSimDegree.f3)
                minDegreeMaxSimDegree = value;
            else if (value.f3.equals(minDegreeMaxSimDegree.f3) && value.f4 > minDegreeMaxSimDegree.f4)
                minDegreeMaxSimDegree = value;
        }
        out.collect(minDegreeMaxSimDegree);
    }
}
