package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 */
public class uniformClusterInterlinks implements GroupReduceFunction <Tuple2<Cluster, String>, Cluster>{
    @Override
    public void reduce(Iterable<Tuple2<Cluster, String>> in, Collector<Cluster> out) throws Exception {
        Collection<Cluster> clusters = new ArrayList<>();
        for (Tuple2<Cluster, String> cluster_clusterId: in){
            clusters.add(cluster_clusterId.f0);
        }
        if (clusters.size() == 1) {
            out.collect(clusters.iterator().next());
        }
        else {
            Collection<String> interLinksIds = new ArrayList<>();
            Collection<String> remainingLinksIds = new ArrayList<>();

            for (Cluster c: clusters){
                for (Edge e: c.getInterLinks())
                    interLinksIds.add(e.getId().toString());
            }
            for (String id: interLinksIds){
                if(Collections.frequency(interLinksIds, id) == clusters.size()) {
                    if (!remainingLinksIds.contains(id)) {
                        remainingLinksIds.add(id);
                    }
                }
            }
            interLinksIds.clear();

            Collection<Edge> remainingLinks = new ArrayList<>();

            for (Cluster c: clusters) {
                for (Edge e : c.getInterLinks()){
//                    if (remainingLinksIds.contains(e.getId().toString()))
//                        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                    if (remainingLinksIds.contains(e.getId().toString()) && e.getPropertyValue("newValue") != null) {
                        Properties properties = e.getProperties();
                        Double newValue = Double.parseDouble(properties.get("newValue").toString());
                        properties.remove("newValue");
                        properties.set("value", newValue);
                        e.setProperties(properties);
                        if (!remainingLinks.contains(e))
                            remainingLinks.add(e);
                    }
                }
            }
            Cluster c = clusters.iterator().next();
            c.setInterLinks(remainingLinks);
//            System.out.println("clusters.size(): "+clusters.size()+", id: "+c.getClusterId()+"&&&&&&&&&&&&&&"+", v:"+c.getVertices().size()+", intra:"+c.getIntraLinks().size()+", inter:"+c.getInterLinks().size());
//            for (Edge e:c.getInterLinks())
//                System.out.println(c.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId()+", v: "+c.getVertices().size());
            out.collect(c);

        }

    }
}
