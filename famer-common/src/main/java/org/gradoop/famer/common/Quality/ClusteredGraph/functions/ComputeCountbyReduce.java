package org.gradoop.famer.common.Quality.ClusteredGraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 */
public class ComputeCountbyReduce  implements GroupReduceFunction <Tuple2<String, String>, Long> {
    public void reduce(Iterable<Tuple2<String, String>> values, Collector<Long> out) throws Exception {
        Long cnt = -1l;
        for (Tuple2<String, String> value : values) {
            cnt++;
        }
        out.collect(cnt);
    }
}
