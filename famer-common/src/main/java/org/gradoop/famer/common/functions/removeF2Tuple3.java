package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1")

public class removeF2Tuple3<A, B, C> implements MapFunction<Tuple3<A, B, C>, Tuple2<A,B>> {
    @Override
    public Tuple2<A, B> map(Tuple3<A, B, C> value) throws Exception {
        return Tuple2.of(value.f0, value.f1);
    }
}
