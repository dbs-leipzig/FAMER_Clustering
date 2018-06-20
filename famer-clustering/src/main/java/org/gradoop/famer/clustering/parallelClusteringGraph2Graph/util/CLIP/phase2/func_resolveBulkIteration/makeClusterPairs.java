package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.famer.common.model.impl.Cluster;

/**
 */
public class makeClusterPairs implements GroupReduceFunction <Tuple2<Cluster, String>, Tuple2<Cluster, Cluster>>{
    @Override
    public void reduce(Iterable<Tuple2<Cluster, String>> in, Collector<Tuple2<Cluster, Cluster>> out) throws Exception {

        Cluster[] clusters = new Cluster[2];
        int cnt = 0;
        for (Tuple2<Cluster, String> i :in){
            clusters[cnt] = i.f0;
            cnt++;
        }

        out.collect(Tuple2.of(clusters[0], clusters[1]));
//        Iterator<Tuple2<Cluster, String>> iterator = in.iterator();
//        Cluster c1,c2;
//        c1 = c2 = null;
////        try {
//            c1 = iterator.next().f0;
//            c2 = iterator.next().f0;
//            out.collect(Tuple2.of(c1, c2));
////        }
//        catch (Exception e){
//            System.out.println("***************************** "+ c1.getInterLinks().size() );
//        }

        /*for (Tuple2<Cluster, String> i : in){
            if (i.f1.equals(""))
                isEdgeIdEmpty = true;
            clusters.add(i.f0);
        }
        if (isEdgeIdEmpty){
            for (Cluster c: clusters)
                out.collect(Tuple2.of(c, null));
        }
        else {
            Iterator<Cluster> iterator = clusters.iterator();
            Cluster c1 = iterator.next();
            Cluster c2 = iterator.next();
            out.collect(Tuple2.of(c1,c2));
        }*/
    }
}
