package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class getClusterEdges implements FlatMapFunction <Cluster, Edge>{
    @Override
    public void flatMap(Cluster in, Collector<Edge> out) throws Exception {
        Collection<Edge> interLinks = in.getInterLinks();
        for (Edge e:interLinks)
            out.collect(e);
        Collection<Edge> intraLinks = in.getIntraLinks();
        for (Edge e : intraLinks)
            out.collect(e);
    }
}
