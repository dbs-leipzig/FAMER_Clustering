package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class setClusterId implements MapFunction <Vertex, Vertex>{
    @Override
    public Vertex map(Vertex in) throws Exception {
        in.setProperty("ClusterId", "r"+in.getPropertyValue("VertexPriority").toString());
        return in;
    }
}
