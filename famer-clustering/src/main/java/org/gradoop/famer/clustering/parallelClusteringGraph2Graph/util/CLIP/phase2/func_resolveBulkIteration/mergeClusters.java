package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_resolveBulkIteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class mergeClusters implements MapFunction <Tuple2<Cluster, Cluster>, Cluster>{
    @Override
    public Cluster map(Tuple2<Cluster, Cluster> in) throws Exception {
//        System.out.println(in.f0.getClusterId()+","+in.f1.getClusterId());


        String newClusterId =  in.f0.getClusterId();
//        String componentId = in.f0.getComponentId();

        Collection<Vertex> vertices = new ArrayList<>();
        for (Vertex v: in.f0.getVertices())
            vertices.add(v);
        for (Vertex v: in.f1.getVertices())
            vertices.add(v);
        for (Vertex v: vertices)
            v.setProperty("ClusterId", newClusterId);


        Collection<Edge> intraLinks = new ArrayList<>();
        for (Edge e: in.f0.getIntraLinks())
            intraLinks.add(e);
        for (Edge e: in.f1.getIntraLinks())
            intraLinks.add(e);




        Collection<Edge> interLinks = new ArrayList<>();
        for (Edge e: in.f0.getInterLinks())
            interLinks.add(e);
        for (Edge e: in.f1.getInterLinks()) {
//            if ((vertexIds1.contains(e.getSourceId().toString()) && vertexIds2.contains(e.getTargetId().toString()))
//                    || (vertexIds2.contains(e.getSourceId().toString()) && vertexIds1.contains(e.getTargetId().toString()))) {
            if (interLinks.contains(e)){
                intraLinks.add(e);
                interLinks.remove(e);
            }
            else
                interLinks.add(e);
        }

        Cluster newCluster = new Cluster(vertices, intraLinks, interLinks, in.f0.getClusterId(), in.f0.getComponentId());


//        for (Edge e:in.f0.getInterLinks())
//            System.out.println(in.f0.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId()+", v: "+in.f0.getVertices().size());
//        for (Edge e:in.f1.getInterLinks())
//            System.out.println(in.f1.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId()+", v: "+in.f1.getVertices().size());
//        for (Edge e:newCluster.getInterLinks())
//            System.out.println(newCluster.getClusterId()+"*******************"+e.getPropertyValue("value")+","+e.getSourceId()+","+e.getTargetId()+", v: "+newCluster.getVertices().size());
        return newCluster;
    }
}
























