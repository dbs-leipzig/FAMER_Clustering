package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class formSingletons implements FlatMapFunction<Tuple3<Vertex, String, String>, Cluster>{
    @Override
    public void flatMap(Tuple3<Vertex, String, String> input, Collector<Cluster> output) throws Exception {
        if (input.f2.contains("s")){
            Collection<Vertex> vertices = new ArrayList<>();
            input.f0.setProperty("ClusterId", input.f2);
            vertices.add(input.f0);
            output.collect(new Cluster(vertices, input.f2));
        }
    }
}
