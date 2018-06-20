package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")

public class getF0Tuple2<T0, T1> implements MapFunction <Tuple2<T0,T1>, T0>{
    @Override
    public T0 map(Tuple2<T0, T1> in) throws Exception {
        return in.f0;
    }
}
