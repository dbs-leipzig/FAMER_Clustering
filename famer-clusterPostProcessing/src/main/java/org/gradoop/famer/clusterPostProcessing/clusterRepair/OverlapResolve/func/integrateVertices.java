package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

public class integrateVertices implements GroupReduceFunction<Tuple3<Vertex, String, String>, Tuple3<Cluster, String, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Vertex, String, String>> iterable, Collector<Tuple3<Cluster, String, String>> collector) throws Exception {
        Collection<Vertex> vertices = new ArrayList<>();
        String oldClsId = "";
        String newClsId = "";
        Boolean f1 = false;
        boolean f2 = false;

        for (Tuple3<Vertex, String, String> i : iterable) {

            oldClsId = i.f1;
            if (i.f0 != null) { // resolve
                i.f0.setProperty("ClusterId", i.f2);
                vertices.add(i.f0);
                f1 = true;

            }
            else {//merge case

                newClsId = i.f2;
                f2 = true;
            }
        }
        if (f1&& f2)
            System.out.print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
        collector.collect(Tuple3.of(new Cluster(vertices), oldClsId, newClsId));
    }
}