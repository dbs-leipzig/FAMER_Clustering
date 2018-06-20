package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class links2Collections implements GroupReduceFunction<Tuple3<Edge, String, Boolean>, Tuple2<Collection<Tuple2<Edge, Boolean>>, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, String, Boolean>> in, Collector<Tuple2<Collection<Tuple2<Edge, Boolean>>, String>> out) throws Exception {
        Collection<Tuple2<Edge, Boolean>> edge_type = new ArrayList<>();
        String clusterId = "";
        for (Tuple3<Edge, String, Boolean> elem: in){
            if (!edge_type.contains(Tuple2.of(elem.f0, elem.f2)))
                edge_type.add(Tuple2.of(elem.f0, elem.f2));
            clusterId = elem.f1;
        }
        out.collect(Tuple2.of(edge_type,clusterId));
    }
}