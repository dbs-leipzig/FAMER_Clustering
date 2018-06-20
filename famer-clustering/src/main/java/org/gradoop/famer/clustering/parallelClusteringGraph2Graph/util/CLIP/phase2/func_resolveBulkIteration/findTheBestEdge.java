package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class findTheBestEdge implements MapFunction <Cluster, Tuple2<Cluster, String>> {
    @Override
    public Tuple2<Cluster, String> map(Cluster in) throws Exception {
        Collection<Edge> interLinks = in.getInterLinks();
        Double maxPriority = -1d;
        String maxEdgeId = "";
        for (Edge e: interLinks) {
            Double priorityValue = (0.5*Double.parseDouble(e.getPropertyValue("value").toString()))+
                    (0.5*(Integer.parseInt(e.getPropertyValue("isSelected").toString())));
            if (priorityValue > maxPriority){
                maxEdgeId = e.getId().toString();
                maxPriority = priorityValue;
            }

        }
        return Tuple2.of(in, maxEdgeId);
    }
}
