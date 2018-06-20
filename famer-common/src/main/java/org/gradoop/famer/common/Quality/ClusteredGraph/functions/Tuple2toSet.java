package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class Tuple2toSet implements MapFunction<Tuple2<Long, Long>, Tuple2<String, Long>> {
    private String indicator;
    private Integer index;
    public Tuple2toSet (String Indicator, Integer Index){
        indicator = Indicator;
        index = Index;
    }
    public Tuple2<String, Long> map(Tuple2<Long, Long> in) throws Exception {
        if (index ==0)
            return Tuple2.of(indicator,in.f0);
        else
            return Tuple2.of(indicator,in.f1);
    }
}

