package org.gradoop.famer.common.model.impl;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.functions.getF0Tuple2;
import org.gradoop.famer.common.functions.link2link_id;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.famer.common.functions.vertex2vertex_gradoopId;
import org.gradoop.famer.common.model.impl.functions.*;
import org.gradoop.famer.common.util.link2link_srcVertex_trgtVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 *
 */
public class ClusterCollection {

    private DataSet<Cluster> clusterCollection;



    public ClusterCollection(){}
    public ClusterCollection(DataSet<Cluster> Clusters){
        clusterCollection = Clusters;
    }



    public ClusterCollection(LogicalGraph clusteredLogicalGraph) {
        clusterCollection = fromLogicalGraph(clusteredLogicalGraph);
    }
    public ClusterCollection(DataSet<Vertex> vertices, DataSet<Edge> edges) {
        clusterCollection = fromLogicalGraph(vertices, edges);
    }
    public void setClusterCollection (DataSet<Cluster> input){ clusterCollection = input;}
    public void setClusterCollection (LogicalGraph input){ clusterCollection = fromLogicalGraph(input);}

    public  DataSet<Cluster> getClusterCollection()  {
        return clusterCollection;}

    public DataSet<Cluster> fromLogicalGraph (DataSet<Vertex> inputVertices, DataSet<Edge> inputEdges)  {

        // cluster links
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = new link2link_srcVertex_trgtVertex(inputVertices, inputEdges).execute();
        DataSet<Tuple3<Edge, String, Boolean>> edge_srcClusterId_trgtClusterId = edge_srcVertex_trgtVertex.flatMap(new classifyLinks());

        DataSet<Tuple2<Collection<Tuple2<Edge, Boolean>>, String>> links = edge_srcClusterId_trgtClusterId.groupBy(1).reduceGroup(new links2Collections());

        // cluster vertices
        DataSet<Tuple2<Collection<Vertex>, String>> vertices = inputVertices
                .flatMap(new vertex2vertex_clusterId(true)).groupBy(1).reduceGroup(new vertices2Collections());

        // creating clusters
        DataSet<Cluster> output = vertices.leftOuterJoin(links).where(1).equalTo(1).with(new makeClusterJoin());
        return output;
    }

    public DataSet<Cluster> fromLogicalGraph (LogicalGraph clusteredLogicalGraph)  {
        return fromLogicalGraph (clusteredLogicalGraph.getVertices(), clusteredLogicalGraph.getEdges());
    }
    public LogicalGraph toLogicalGraph (GradoopFlinkConfig config){
        DataSet<Vertex> vertices = clusterCollection.flatMap(new getClusterVertices());
        DataSet<Tuple2<Vertex, String>> vertices_Ids = vertices.map(new vertex2vertex_gradoopId());
        vertices = vertices_Ids.distinct(1).map(new getF0Tuple2());
        DataSet<Edge> edges = clusterCollection.flatMap(new getClusterEdges());
        DataSet<Tuple2<Edge, String>> edges_Ids = edges.map(new link2link_id());
        edges = edges_Ids.distinct(1).map(new getF0Tuple2());
        return config.getLogicalGraphFactory().fromDataSets(vertices, edges);
    }

    public void DenotateClusterCollection (Integer SourceNo)  {
        clusterCollection = clusterCollection.map(new DenotateCluster(SourceNo));
    }



    public DataSet<Long> compareClusterCollections (ClusterCollection c) throws Exception {

        DataSet<Tuple2<Cluster, String>> cluster_clusterId_1 = c.clusterCollection.map(new MapFunction<Cluster, Tuple2<Cluster, String>>() {
            @Override
            public Tuple2<Cluster, String> map(Cluster value) throws Exception {
                return Tuple2.of(value, value.getClusterId());
            }
        });
        DataSet<Tuple2<Cluster, String>> cluster_clusterId_2 = clusterCollection.map(new MapFunction<Cluster, Tuple2<Cluster, String>>() {
            @Override
            public Tuple2<Cluster, String> map(Cluster value) throws Exception {
                return Tuple2.of(value, value.getClusterId());
            }
        });


        DataSet<Long> output = cluster_clusterId_1.union(cluster_clusterId_2).groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<Cluster, String>, Long>() {
            @Override
            public void reduce(Iterable<Tuple2<Cluster, String>> values, Collector<Long> out) throws Exception {
                Cluster c1 = null, c2 = null;
                int cnt = 0;
                for (Tuple2<Cluster, String> value : values){
                    if (cnt==0)
                        c1= value.f0;
                    else
                        c2 = value.f0;
                    cnt++;
                }
                if (cnt == 1) {
                    out.collect(1l);
                    return;
                }
                if (c1.isDifferent(c2))
                    out.collect(1l);
                else
                    out.collect(0l);
            }
        }).reduce(new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1+value2;
            }
        });
        return output;
    }





}
