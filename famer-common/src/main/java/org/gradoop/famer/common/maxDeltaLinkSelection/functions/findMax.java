package org.gradoop.famer.common.maxDeltaLinkSelection.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

import java.util.*;
// delta is not used yet
/**
 */
public class findMax implements GroupReduceFunction <Tuple3<Edge, String, String>, Tuple3<Edge, String, Integer>> {
    private Double delta;

    public findMax (Double Delta){
        delta = Delta;
    }
    @Override
    public void reduce(Iterable<Tuple3<Edge, String, String>> values, Collector<Tuple3<Edge, String, Integer>> out) throws Exception {
        Collection<Tuple4<Edge, String, String, Double>> edge_list = new ArrayList<>();
        for (Tuple3<Edge, String, String> e : values){
            edge_list.add(Tuple4.of(e.f0, e.f1, e.f2, Double.parseDouble(e.f0.getPropertyValue("value").toString())));
        }
        Tuple4<Edge, String, String, Double>[] edge_array = edge_list.toArray(new Tuple4[edge_list.size()]);
        edge_list.clear();
        Arrays.sort(edge_array, new valueComparator());
        ArrayUtils.reverse(edge_array);
        int i;
        Integer isSelectedIndex = 0;
//        for (i = 0; i< edge_array.length; i++) {
//        }
//        for (;i< edge_array.length; i++) {
//
//        }
        if (edge_array[0].f1.compareTo(edge_array[0].f2) < 0)
            isSelectedIndex = 1;
        else
            isSelectedIndex = 2;
        out.collect(Tuple3.of(edge_array[0].f0, edge_array[0].f0.getId().toString(), isSelectedIndex));
        for ( i = 1; i< edge_array.length; i++)
            out.collect(Tuple3.of(edge_array[i].f0, edge_array[i].f0.getId().toString(), 0));
    }

    private class valueComparator implements Comparator <Tuple4<Edge, String, String, Double>> {
        @Override
        public int compare(Tuple4<Edge, String, String, Double> in1, Tuple4<Edge, String, String, Double> in2) {
            if (in1.f3< in2.f3)
                return -1;
            if (in1.f3>in2.f3)
                return 1;
            return 0;
        }
    }
}
