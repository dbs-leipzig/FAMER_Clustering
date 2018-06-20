package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class makeClusterJoin implements JoinFunction <Tuple2<Collection<Vertex>, String>, Tuple2<Collection<Tuple2<Edge, Boolean>>, String>,
        Cluster> {
    @Override
    public Cluster join(Tuple2<Collection<Vertex>, String> first, Tuple2<Collection<Tuple2<Edge, Boolean>>, String> second) throws Exception {


        Collection<Edge> intraLinks = new ArrayList<>();
        Collection<Edge> interLinks = new ArrayList<>();
        Collection<Vertex> vertices = new ArrayList<>();

        if (second != null) {
            for (Tuple2<Edge, Boolean> edge : second.f0) {
                if (edge.f1)
                    interLinks.add(edge.f0);
                else
                    intraLinks.add(edge.f0);
            }
        }
        String componentId = "";
        if (first.f0.iterator().next().getPropertyValue("ComponentId")!=null)
            componentId = first.f0.iterator().next().getPropertyValue("ComponentId").toString();
        for (Vertex v:first.f0){
            if (!vertices.contains(v))
                vertices.add(v);
        }

        Cluster cluster = new Cluster(vertices, intraLinks, interLinks, first.f1, componentId);


        return cluster;
    }
}
