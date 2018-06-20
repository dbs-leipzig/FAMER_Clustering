package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class gradoopId2vertexJoin2 implements JoinFunction<Tuple3<Edge, Vertex, String>, Tuple2<Vertex, String>, Tuple3<Edge, Vertex, Vertex>> {
    @Override
    public Tuple3<Edge, Vertex, Vertex> join(Tuple3<Edge, Vertex, String> first, Tuple2<Vertex, String> second) throws Exception {
        return Tuple3.of(first.f0, first.f1, second.f0);
    }
}

