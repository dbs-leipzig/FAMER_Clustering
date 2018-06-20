package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.ResolveIteration;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class NoMergeResolver implements GroupReduceFunction<Tuple3<String, Vertex, Cluster>, Tuple3<Vertex, String, String>> {
    private ResolveIteration iteration;
    public NoMergeResolver(ResolveIteration resolveIteration){iteration = resolveIteration;}
    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, Cluster>> input, Collector<Tuple3<Vertex, String, String>> output) throws Exception {
        Collection<Cluster> engagingClusters = new ArrayList<>();
        Integer relatedVerticesSize = 0;
        Vertex vertex = null;
        for (Tuple3<String, Vertex, Cluster> in:input){
            Collection<Vertex> relatedVertices = in.f2.getRelatedVertices(in.f1.getId());
            relatedVerticesSize += relatedVertices.size();
            vertex = in.f1;
            if (!isAllOverlap(relatedVertices))
                engagingClusters.add(in.f2);
        }
        String newClusterId = "";
        // state 1: not connected to any vertex
        if (relatedVerticesSize == 0){
            // singletone
            newClusterId = "s"+vertex.getPropertyValue("VertexPriority").toString();
        }
        // state 2: just connected to overlapped vertices
        else if (engagingClusters.size() == 0){
            // iteration 1 unchanged
            // iteration 2 : singleton
            if (iteration.equals(ResolveIteration.ITERATION2))
                newClusterId = "s"+vertex.getPropertyValue("VertexPriority").toString();
        }
        // state 3: just connected to 1 cluster
        else if (engagingClusters.size() == 1){
            // resolve
            newClusterId = engagingClusters.iterator().next().getClusterId();
        }
        // state 4: connected to more than 1 clusters
        else {
            // resolve by computing associationDegree
            Double maxDegree = 0d;
            for (Cluster cluster:engagingClusters) {
                Double assDegree = cluster.getAssociationDegree40(vertex.getId().toString());
                if (assDegree > maxDegree) {
                    maxDegree = assDegree;
                    newClusterId = cluster.getClusterId();
                }
            }
        }
        if (!newClusterId.equals("")) {
            String[] clusterIds = vertex.getPropertyValue("ClusterId").toString().split(",");
            for (String id : clusterIds)
                output.collect(Tuple3.of(vertex, id, newClusterId));
        }
    }

    private boolean isAllOverlap(Collection<Vertex> relatedVertices) {
        for (Vertex v: relatedVertices){
            if (!v.getPropertyValue("ClusterId").toString().contains(","))
                return false;
        }
        return true;
    }
}
