package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class setComponentId implements MapFunction <Vertex, Vertex>{
    @Override
    public Vertex map(Vertex value) throws Exception {
        value.setProperty("ComponentId", value.getPropertyValue("ClusterId").toString());
        return value;
    }
}
