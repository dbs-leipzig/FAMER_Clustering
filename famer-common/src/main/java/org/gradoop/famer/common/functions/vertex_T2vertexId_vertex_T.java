package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f1;f1->f2")
public class vertex_T2vertexId_vertex_T<T> implements MapFunction<Tuple2<Vertex, T>, Tuple3<String, Vertex, T>> {
    @Override
    public Tuple3<String, Vertex, T> map(Tuple2<Vertex, T> in) throws Exception {
        return Tuple3.of(in.f0.getId().toString(), in.f0, in.f1);
    }
}
