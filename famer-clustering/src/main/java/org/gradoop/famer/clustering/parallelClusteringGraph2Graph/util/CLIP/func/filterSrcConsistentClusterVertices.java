package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class filterSrcConsistentClusterVertices implements GroupReduceFunction <Tuple2<Vertex, String>, Vertex>{
    private Integer sourceNo;
    public filterSrcConsistentClusterVertices (Integer inputSourceNo){sourceNo = inputSourceNo;}
    public filterSrcConsistentClusterVertices (){sourceNo = -1;}

    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> iterable, Collector<Vertex> collector) throws Exception {
        Collection<String> sources = new ArrayList<>();
        Collection <Vertex> vertices = new ArrayList<>();
        Boolean isSourceConsistent = true;
        for (Tuple2<Vertex, String> vertex:iterable){
            String src = vertex.f0.getPropertyValue("srcId").toString();
            if (!sources.contains(src))
                sources.add(src);
            else
                isSourceConsistent = false;
            vertices.add(vertex.f0);
        }
        if (isSourceConsistent){
            if ((sourceNo != -1 && vertices.size() == sourceNo) || sourceNo == -1) {
                for (Vertex vertex : vertices)
                    collector.collect(vertex);
            }

        }
    }
}
