package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class filterOutStrong implements FlatMapFunction<Edge, Edge> {
    @Override
    public void flatMap(Edge edge, Collector<Edge> collector) throws Exception {
        if (Integer.parseInt(edge.getPropertyValue("isSelected").toString()) == 2)
            collector.collect(edge);
    }
}