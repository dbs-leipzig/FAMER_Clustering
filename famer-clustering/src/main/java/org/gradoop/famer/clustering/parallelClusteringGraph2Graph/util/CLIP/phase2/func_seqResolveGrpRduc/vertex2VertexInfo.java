package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class vertex2VertexInfo implements MapFunction <Vertex, Tuple3<String, String, String>>{
    @Override
    public Tuple3<String, String, String> map(Vertex vertex) throws Exception {
        return Tuple3.of(vertex.getId().toString(), vertex.getPropertyValue("srcId").toString(),
                vertex.getPropertyValue("ClusterId").toString());
    }
}
