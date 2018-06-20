package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class cluster2cluster_degree_edgeId implements FlatMapFunction <Cluster, Tuple4<Cluster, Integer, Double, String>>{
    @Override
    public void flatMap(Cluster in, Collector<Tuple4<Cluster, Integer, Double, String>> out) throws Exception {
        Collection<Edge> interLinks = in.getInterLinks();
        for (Edge e: interLinks)
            out.collect(Tuple4.of(in, interLinks.size(), Double.parseDouble(e.getPropertyValue("value").toString()),e.getId().toString()));
    }
}
