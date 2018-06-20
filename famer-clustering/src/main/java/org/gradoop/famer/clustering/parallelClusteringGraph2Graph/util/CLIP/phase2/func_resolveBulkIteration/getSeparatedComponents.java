package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class getSeparatedComponents implements FlatMapFunction <Cluster, Cluster>{
    @Override
    public void flatMap(Cluster value, Collector<Cluster> out) throws Exception {
        if (value.getInterLinks().size() == 0)
            out.collect(value);
    }
}
