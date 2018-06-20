package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class getClusterVertices implements FlatMapFunction <Cluster, Vertex>{
    @Override
    public void flatMap(Cluster in, Collector<Vertex> out) throws Exception {
        Collection<Vertex> vertices = in.getVertices();
        boolean isCompletePerfect = in.getIsCompletePerfect();
        boolean isPerfect = in.getIsPerfect();
        boolean isPerfectIsolated = in.getIsPerfectIsolated();
        for (Vertex v: vertices) {
            v.setProperty("isPerfect", isPerfect);
            v.setProperty("isCompletePerfect", isCompletePerfect);
            v.setProperty("isPerfectIsolated", isPerfectIsolated);
            out.collect(v);
        }
    }
}
