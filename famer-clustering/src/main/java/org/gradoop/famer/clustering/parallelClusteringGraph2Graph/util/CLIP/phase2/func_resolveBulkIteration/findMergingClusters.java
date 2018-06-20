package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class findMergingClusters implements GroupReduceFunction <Tuple2<Cluster, String>, Tuple2<Cluster, Cluster>>{
    @Override
    public void reduce(Iterable<Tuple2<Cluster, String>> in, Collector<Tuple2<Cluster, Cluster>> out) throws Exception {
        Cluster[] clusters = new Cluster[2];
        int cnt = 0;
        for (Tuple2<Cluster,String> i:in){
            if (!i.f1.equals("")){
                clusters[cnt] = i.f0;
                cnt++;
            }
        }
        if (cnt > 1){
            out.collect(Tuple2.of(clusters[0], clusters[1]));
        }

    }
}
