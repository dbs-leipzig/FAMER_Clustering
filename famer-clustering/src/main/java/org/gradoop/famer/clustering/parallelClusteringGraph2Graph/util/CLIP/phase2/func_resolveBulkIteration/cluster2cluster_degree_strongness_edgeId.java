package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */

public class cluster2cluster_degree_strongness_edgeId implements FlatMapFunction<Cluster, Tuple5<Cluster, Integer, Double, Integer, String>> {
    @Override
    public void flatMap(Cluster in, Collector<Tuple5<Cluster, Integer, Double, Integer, String>> out) throws Exception {
        Collection<Edge> interLinks = in.getInterLinks();
        for (Edge e: interLinks)
            out.collect(Tuple5.of(in, interLinks.size(), Double.parseDouble(e.getPropertyValue("value").toString()),
                   Integer.parseInt(e.getPropertyValue("isSelected").toString()) ,e.getId().toString()));
    }
}
