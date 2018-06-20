package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class reduceTuple2toSet implements ReduceFunction<Tuple2<Long, Long>> {
    private int index;
    public reduceTuple2toSet (int Index){
        index = Index;
    }
    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> in1, Tuple2<Long, Long> in2) throws Exception {
        if (index ==0)
            return Tuple2.of(in1.f0+in2.f0,0l);
        else
            return Tuple2.of(0l,in1.f1+in2.f1);
    }
}
