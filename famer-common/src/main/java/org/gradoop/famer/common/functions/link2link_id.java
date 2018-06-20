package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class link2link_id implements MapFunction <Edge, Tuple2<Edge, String>>{
    @Override
    public Tuple2<Edge, String> map(Edge value) throws Exception {
        return Tuple2.of(value, value.getId().toString());
    }
}
