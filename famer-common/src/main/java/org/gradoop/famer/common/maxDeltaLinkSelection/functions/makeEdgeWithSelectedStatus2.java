package org.gradoop.famer.common.maxDeltaLinkSelection.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class makeEdgeWithSelectedStatus2 implements GroupReduceFunction <Tuple3<Edge, String, Integer>, Edge>{

    @Override
    public void reduce(Iterable<Tuple3<Edge, String, Integer>> values, Collector<Edge> out) throws Exception {
        int isSelectedCnt = 0;
        Edge e = new Edge();
        for (Tuple3<Edge, String, Integer> value: values){
            e = value.f0;
            isSelectedCnt += (value.f2);
        }
        if (isSelectedCnt == 2)
            e.setProperty("isSelected", 2);
        else if (isSelectedCnt == 1)
            e.setProperty("isSelected", 1);
        else
            e.setProperty("isSelected", 0);
        out.collect(e);
    }
}
