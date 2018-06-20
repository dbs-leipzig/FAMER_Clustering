package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class updateClusterLinks implements GroupReduceFunction<Tuple2<Cluster, String>, Cluster> {
    @Override
    public void reduce(Iterable<Tuple2<Cluster, String>> input, Collector<Cluster> output) throws Exception {
        Collection<Vertex> vertices = new ArrayList<>();
        Collection<Edge> links = new ArrayList<>();
        Collection<Edge> intraLinks = new ArrayList<>();
        Collection<Edge> interLinks = new ArrayList<>();
        String clusterId = "";

        Collection<Tuple3<Edge, Vertex, Vertex>> vertexPairs = new ArrayList<>();

        for (Tuple2<Cluster, String> in:input){
            for (Vertex vertex: in.f0.getVertices()){
                if (!vertices.contains(vertex))
                    vertices.add(vertex);
            }
            for (Edge link: in.f0.getInterLinks()){
                if (!links.contains(link))
                    links.add(link);
            }
            for (Edge link: in.f0.getIntraLinks()){
                if (!links.contains(link))
                    links.add(link);
            }
            clusterId = in.f1;
        }

        for (Edge link: links){
            Vertex f0 = null;
            Vertex f1 = null;
            for (Vertex v: vertices){
                if (v.getId().equals(link.getSourceId()))
                    f0 = v;
                else if (v.getId().equals(link.getTargetId()))
                    f1 = v;
            }
            vertexPairs.add(Tuple3.of(link, f0, f1));
        }
        for (Tuple3<Edge, Vertex, Vertex> pair: vertexPairs) {
            if(pair.f1 == null && pair.f2==null);
            else if(pair.f1 == null || pair.f2==null)
                interLinks.add(pair.f0);
            else {
//                    String[] f1ClusterIds = pair.f1.getPropertyValue("ClusterId").toString().split(",");
//                    String[] f2ClusterIds = pair.f2.getPropertyValue("ClusterId").toString().split(",");
//                    if (hasIntersection(f1ClusterIds, f2ClusterIds))
//                        interLinks.add(pair.f0);
//                    else
//                        intraLinks.add(pair.f0);
                List<String> srcClusterId = Arrays.asList(pair.f1.getPropertyValue("ClusterId").toString().split(","));
                List<String> trgtClusterId = Arrays.asList(pair.f2.getPropertyValue("ClusterId").toString().split(","));

                if(srcClusterId.contains(clusterId) && trgtClusterId.contains(clusterId)) {
                    intraLinks.add(pair.f0);
                    if (srcClusterId.size()>1 || trgtClusterId.size()>1)
                        interLinks.add(pair.f0);
                }
                else if((srcClusterId.contains(clusterId) && !trgtClusterId.contains(clusterId)) || (!srcClusterId.contains(clusterId) && trgtClusterId.contains(clusterId)))
                    interLinks.add(pair.f0);

            }
        }
        output.collect(new Cluster(vertices, intraLinks, interLinks, clusterId, ""));

    }
}