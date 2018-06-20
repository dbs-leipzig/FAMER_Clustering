package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Iterator;

/**
 */

public class edge2ClusterPair2 implements GroupReduceFunction<Tuple5<Cluster, Integer, Double, Integer, String>, Tuple4<Cluster, Cluster, String, Double>> {
    private Double simValueCoef;
    private Double degreeCoef;
    private Double strengthCoef;

    public edge2ClusterPair2(Double inputSimValueCoef, Double inputDegreeCoef, Double inputStrengthCoef){
        simValueCoef = inputSimValueCoef;
        degreeCoef = inputDegreeCoef;
        strengthCoef = inputStrengthCoef;
    }


    @Override
    public void reduce(Iterable<Tuple5<Cluster, Integer, Double, Integer, String>> in, Collector<Tuple4<Cluster, Cluster, String, Double>> out) throws Exception {

        Cluster c1,c2;
        Iterator<Tuple5<Cluster, Integer, Double, Integer, String>> iterator = in.iterator();
        Tuple5<Cluster, Integer, Double, Integer, String> next = iterator.next();
        Integer degree = next.f1;
        Double simValue = next.f2;
        Integer strongness = next.f3;

        c1 = next.f0;


        Tuple5<Cluster, Integer, Double, Integer, String> nextnext = iterator.next();
        degree = Math.min(nextnext.f1, degree);
        c2 = nextnext.f0;

//        Double coef = 1d/3d;
//        strongness--;
//        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& "+simValue+"&&&&"+degree+"&&&&"+strongness);
        Double priorityValue = (simValueCoef*simValue)+(degreeCoef*(1/degree))+(strengthCoef*(strongness));
//        Double priorityValue = simValue;


        out.collect(Tuple4.of(c1, c2, c1.getComponentId(), priorityValue));

    }
}
