package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class updateClusterInterLinks implements FlatMapFunction <Tuple2<Cluster, Cluster>, Tuple2<Cluster, String>>{
    @Override
    public void flatMap(Tuple2<Cluster, Cluster> in, Collector<Tuple2<Cluster, String>> out) throws Exception {
//        if (in.f1 == null){
//            out.collect(Tuple2.of(in.f0, in.f0.getClusterId()));
//            return;
//        }

        Collection<Edge> sharedInterLinks = new ArrayList<>();
        for (Edge e:in.f0.getInterLinks())
                sharedInterLinks.add(e);
        Edge[] edges = sharedInterLinks.toArray(new Edge[sharedInterLinks.size()]);

        for (Edge e:edges){
            if (!in.f1.getInterLinks().contains(e))
                sharedInterLinks.remove(e);
        }



        in.f0.removeFromInterLinks(sharedInterLinks);
        in.f1.removeFromInterLinks(sharedInterLinks);


        if (in.f0.isCompatible(in.f1)) {
            Edge integratedLink = integrateInterLinks(sharedInterLinks);
            in.f0.add2InterLinks(integratedLink);
            in.f1.add2InterLinks(integratedLink);
        }



//        for (Edge e:in.f0.getInterLinks())
//            System.out.println(in.f0.getClusterId()+","+in.f1.getClusterId()+","+in.f0.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId()+", v: "+in.f0.getVertices().size()+", inter: "+in.f0.getInterLinks().size());
//        for (Edge e:in.f1.getInterLinks())
//            System.out.println(in.f0.getClusterId()+","+in.f1.getClusterId()+","+in.f1.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId()+", v: "+in.f1.getVertices().size()+", inter: "+in.f1.getInterLinks().size());



        out.collect(Tuple2.of(in.f0, in.f0.getClusterId()));
        out.collect(Tuple2.of(in.f1, in.f1.getClusterId()));

    }
    private Edge integrateInterLinks (Collection<Edge> edges){
        Double simDegree = 0d;
        Edge output = null;
        for (Edge e: edges) {
            simDegree += Double.parseDouble(e.getPropertyValue("value").toString());
            output = e;
        }
        simDegree /= edges.size();
        for (Edge e: edges) {
            if (output.getId().toString().compareTo(e.getId().toString()) < 0)
                output = e;
        }
        output.setProperty("newValue", simDegree);
        return output;
    }
}






























