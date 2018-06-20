package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class cluster2interLink extends RichFlatMapFunction<Cluster, Edge> {
    @Override
    public void flatMap(Cluster input, Collector<Edge> out) throws Exception {
        for (Edge e: input.getInterLinks())
            out.collect(e);
    }
}
