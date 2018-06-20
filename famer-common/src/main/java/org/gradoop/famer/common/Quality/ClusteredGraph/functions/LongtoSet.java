package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
public class LongtoSet implements MapFunction<Long, Tuple2<String, Long>> {
    private String indicator;
    public LongtoSet (String Indicator){
        indicator = Indicator;
    }
    public Tuple2<String, Long> map(Long in) throws Exception {
        return Tuple2.of(indicator,in);
    }
}