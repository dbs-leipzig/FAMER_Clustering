package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class vertices2Collections implements GroupReduceFunction <Tuple2<Vertex, String>, Tuple2<Collection<Vertex>, String>>{
    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> in, Collector<Tuple2<Collection<Vertex>, String>> out) throws Exception {
        Collection<Vertex> vertices = new ArrayList<>();
        Collection<String> gradoopIds = new ArrayList<>();
        String clusterId = "";
        for (Tuple2<Vertex, String> elem:in){
            clusterId = elem.f1;
            if (!gradoopIds.contains(elem.f0.getId().toString())){
                gradoopIds.add(elem.f0.getId().toString());
                vertices.add(elem.f0);
            }
        }
        out.collect(Tuple2.of(vertices, clusterId));
    }
}
