package org.gradoop.famer.common.util;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.functions.link2link_srcId_trgtId;
import org.gradoop.famer.common.functions.vertex2vertex_gradoopId;
import org.gradoop.famer.common.util.functions.gradoopId2vertexJoin1;
import org.gradoop.famer.common.util.functions.gradoopId2vertexJoin2;
import org.gradoop.flink.model.api.epgm.LogicalGraph;


/**
 *
 */

public class minus<A> {
    public DataSet<A> execute (DataSet<Tuple2<A, String>> first, DataSet<Tuple2<A, String>> second){
        DataSet<Tuple3<A, String, String>> firstSet = first.map(new identify("first"));
        DataSet<Tuple3<A, String, String>> secondSet = second.map(new identify("sec"));
        return firstSet.union(secondSet).groupBy(1).reduceGroup(new reducer());
    }

    private class identify<A> implements MapFunction<Tuple2<A, String>, Tuple3<A, String, String>> {
        private String id;
        public identify(String ID) {
            id = ID;
        }

        @Override
        public Tuple3<A, String, String> map(Tuple2<A, String> value) throws Exception {
            return Tuple3.of(value.f0, value.f1, id);
        }
    }

    private class reducer implements GroupReduceFunction<Tuple3<A, String, String>, A> {
        @Override
        public void reduce(Iterable<Tuple3<A, String, String>> values, Collector<A> out) throws Exception {
            int cnt = 0;
            A a = null;
            String type = "";
            for (Tuple3<A, String, String> v: values) {
                a = v.f0;
                type = v.f2;
                cnt++;
            }
            if (cnt == 1 && type.equals("first"))
                out.collect(a);
        }
    }
}