package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class cluster2vertex_vertexId implements FlatMapFunction <Cluster, Tuple2<Vertex, String>> {
    @Override
    public void flatMap(Cluster in, Collector<Tuple2<Vertex, String>> out) throws Exception {
        for (Vertex v:in.getVertices())
            out.collect(Tuple2.of(v, v.getId().toString()));
    }
}
