package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */

public class addLabel<T> implements MapFunction <T, Tuple2<T, String>>{
    private String label;
    public addLabel (String Label){label=Label;}
    @Override
    public Tuple2<T, String> map(T value) throws Exception {
        return Tuple2.of(value, label);
    }
}
