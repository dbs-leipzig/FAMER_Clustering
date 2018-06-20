package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.util;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class Minus {
    public DataSet<Edge> execute (DataSet<Tuple2<Edge, String>> first, DataSet<Tuple2<Edge, String>> second){
        DataSet<Tuple3<Edge, String, String>> firstSet = first.map(new identify("e"));
        DataSet<Tuple3<Edge, String, String>> secondSet = second.map(new identify("v"));
        return firstSet.union(secondSet).groupBy(1).reduceGroup(new reducer());
    }

    private class identify implements MapFunction<Tuple2<Edge, String>, Tuple3<Edge, String, String>> {
        private String id;
        public identify(String ID) {
            id = ID;
        }

        @Override
        public Tuple3<Edge, String, String> map(Tuple2<Edge, String> value) throws Exception {
            return Tuple3.of(value.f0, value.f1, id);
        }
    }

    private class reducer implements GroupReduceFunction<Tuple3<Edge, String, String>, Edge> {
        @Override
        public void reduce(Iterable<Tuple3<Edge, String, String>> values, Collector<Edge> out) throws Exception {
            Collection<Edge> edgs = new ArrayList<>();
            boolean isOut = true;
            for (Tuple3<Edge, String, String> v: values) {
                if (v.f2.equals("v")){
                    isOut = false;
                    break;
                }
                else
                    edgs.add(v.f0);
            }
            if (isOut) {
                for (Edge e:edgs)
                    out.collect(e);
            }
        }
    }
}
