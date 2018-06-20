package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 */
public class enumerate<T> implements GroupReduceFunction <Tuple2<T, String>, Tuple3<T, Integer,Integer>>{
    private String firstLabel;
    private String secondLabel;
    public enumerate (String FirstLabel, String SecondLabel){
        firstLabel = FirstLabel;
        secondLabel = SecondLabel;
    }
    @Override
    public void reduce(Iterable<Tuple2<T, String>> values, Collector<Tuple3<T, Integer, Integer>> out) throws Exception {
        Integer first = 0;
        Integer second = 0;
        T entity = null;
        for (Tuple2<T, String> value:values){
            entity = value.f0;
            if (value.f1.equals(firstLabel))
                first++;
            else
                second++;
        }
        out.collect(Tuple3.of(entity, first, second));
    }
}
