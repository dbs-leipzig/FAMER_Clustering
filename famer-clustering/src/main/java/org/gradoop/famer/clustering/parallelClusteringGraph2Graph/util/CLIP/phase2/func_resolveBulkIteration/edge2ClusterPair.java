package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Iterator;

/**
 */
public class edge2ClusterPair implements GroupReduceFunction <Tuple4<Cluster, Integer, Double, String>, Tuple5<Cluster, Cluster, String,  Integer, Double>>{
    @Override
    public void reduce(Iterable<Tuple4<Cluster, Integer, Double, String>> in, Collector<Tuple5<Cluster, Cluster, String, Integer, Double>> out) throws Exception {

        Cluster c1,c2;
        Iterator<Tuple4<Cluster, Integer, Double, String>> iterator = in.iterator();
        Tuple4<Cluster, Integer, Double, String> next = iterator.next();
        Integer degree = next.f1;
        Double simDegree = next.f2;
        c1 = next.f0;


        Tuple4<Cluster, Integer, Double, String> nextnext = iterator.next();
        degree = Math.min(nextnext.f1, degree);
//        if (degree == 1) {
//            simDegree = 2.0;
//        }
        c2 = nextnext.f0;

//        for (Edge e:c1.getInterLinks())
//            System.out.println(c1.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId());
//        for (Edge e:c2.getInterLinks())
//            System.out.println(c2.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId());
        // if two cluster are compatible
//        if (c1.isCompatible(c2)) {

            // sort clusters
//            if (c1.getClusterId().compareTo(c2.getClusterId()) < 0)
                out.collect(Tuple5.of(c1, c2, c1.getComponentId(), degree, simDegree));
//            else
//                out.collect(Tuple4.of(c2, c1, degree, simDegree));
////        }

    }
}
