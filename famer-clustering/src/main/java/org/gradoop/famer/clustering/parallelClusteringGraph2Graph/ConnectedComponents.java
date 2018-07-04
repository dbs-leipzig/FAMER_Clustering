/*
 * Copyright © 2016 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.GenerateOutput;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ModifyGraphforClustering;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

import java.io.Serializable;

/**
 * The implementation of Connected Component algorithm.
 */


public class ConnectedComponents
        implements UnaryGraphToGraphOperator, Serializable{

    
    public String getName() {
        // TODO Auto-generated method stub
        return ConnectedComponents.class.getName();
    }
    private String clusterIdPrefix;
    private static ClusteringOutputType clusteringOutputType;

    public ConnectedComponents(){clusterIdPrefix=""; clusteringOutputType = ClusteringOutputType.GRAPH;}
    public ConnectedComponents(String prefix){
        clusterIdPrefix = prefix;
        clusteringOutputType = ClusteringOutputType.GRAPH;
    }
    public ConnectedComponents(String prefix, ClusteringOutputType clusteringOutputType){
        clusterIdPrefix = prefix;
        this.clusteringOutputType = clusteringOutputType;
    }


    
    public LogicalGraph execute(LogicalGraph graph) {
        graph = graph.callForGraph(new ModifyGraphforClustering());


        try {


            Graph gellyGraph = Graph.fromDataSet(
                   graph.getVertices().map(new ToGellyVertexWithIdValue()),
                    graph.getEdges().flatMap(new ToGellyEdgeforSGInput()),
                    graph.getConfig().getExecutionEnvironment()
            );

            DataSet<org.apache.flink.graph.Vertex<GradoopId, Long>> ResVertices = new GSAConnectedComponents<GradoopId, Long, Long>(Integer.MAX_VALUE)
                    .run(gellyGraph);

            DataSet<Vertex> lgVertices= ResVertices.join(graph.getVertices().map(new toVertexAndGradoopId())).where(0).equalTo(0)
                    .with(new joinFunc(clusterIdPrefix));
            LogicalGraph resultLG = graph.getConfig().getLogicalGraphFactory().fromDataSets(graph.getGraphHead(), lgVertices, graph.getEdges());
            resultLG = resultLG.callForGraph(new GenerateOutput(clusteringOutputType));

            return resultLG;



        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        return null;
    }



    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Long>> {
        
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
//            in.setProperty("VertexPriority", Long.parseLong(in.getPropertyValue("id").toString()));
            Long vp = Long.parseLong(in.getPropertyValue("VertexPriority").toString());
//            in.setProperty("ClusterId", 0);
//            in.setProperty("roundNo", 0);
            GradoopId id = in.getId();
            return new org.apache.flink.graph.Vertex<GradoopId, Long>(id, vp);
        }
    }
    public static final class toVertexAndGradoopId
            implements MapFunction<Vertex, Tuple2<GradoopId, Vertex>> {
        
        public Tuple2<GradoopId, Vertex> map(Vertex in) throws Exception {
            return Tuple2.of(in.getId(),in);
        }
    }

    public class ToGellyEdgeforSGInput<E extends EPGMEdge>
            implements FlatMapFunction<E, Edge<GradoopId, Double>> {
        
        public void flatMap(E e, Collector<Edge<GradoopId, Double>> out) throws Exception {
            out.collect(new Edge(e.getSourceId(), e.getTargetId(), 0.0));
        }
    }
    public class joinFunc implements JoinFunction <org.apache.flink.graph.Vertex<GradoopId, Long>, Tuple2<GradoopId, Vertex>, Vertex>{
        private String clusterIdPrefix;
        public joinFunc(String prefix){clusterIdPrefix = prefix;}
        public Vertex join(org.apache.flink.graph.Vertex<GradoopId, Long> in1, Tuple2<GradoopId, Vertex> in2) {
            in2.f1.setProperty("ClusterId",clusterIdPrefix+in1.f1);
            return in2.f1;
        }
    }

}
