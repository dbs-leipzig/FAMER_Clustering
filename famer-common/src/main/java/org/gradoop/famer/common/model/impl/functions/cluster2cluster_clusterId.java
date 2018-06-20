package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 *
 */
public class cluster2cluster_clusterId implements MapFunction <Cluster, Tuple2<Cluster, String>>{
    @Override
    public Tuple2<Cluster, String> map(Cluster in) throws Exception {

        return Tuple2.of(in, in.getClusterId());
    }
}
