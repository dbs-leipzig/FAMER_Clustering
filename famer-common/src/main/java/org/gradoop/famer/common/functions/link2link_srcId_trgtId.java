package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 *
 */
public class link2link_srcId_trgtId implements MapFunction<Edge, Tuple3<Edge, String, String>> {
    @Override
    public Tuple3<Edge, String, String> map(Edge in) throws Exception {
        return Tuple3.of(in, in.getSourceId().toString(), in.getTargetId().toString());
    }
}
