package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ERClustering;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;


/**
 * Used to filter out edges
 * It only keeps edges with n-top similarity degree
 */
public class MaxN <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
        implements UnaryGraphToGraphOperator {

    public Integer N ;
    final private String Type;
    public MaxN (String mySwitch, Integer n){
        N = n;
        Type = mySwitch;
    }
    public LogicalGraph execute(final LogicalGraph input) {
        final Integer executeN= N;
        final String executeType = Type;

        DataSet<Tuple3<GradoopId,Edge,Double>> vertexId_Edge_SimValue = input.getEdges().map(new MapFunction<Edge, Tuple3<GradoopId, Edge, Double>>() {
            public Tuple3<GradoopId,Edge, Double> map(Edge edge) {
                if (executeType.equals("forward"))
                    return Tuple3.of(edge.getSourceId(),edge, Double.parseDouble(edge.getPropertyValue("value").toString()));
                else // if Type.equals("backward")
                    return Tuple3.of(edge.getTargetId(),edge, Double.parseDouble(edge.getPropertyValue("value").toString()));
            }
        });

        DataSet<Edge> limitedEdges = vertexId_Edge_SimValue.groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(new GroupReduceFunction<Tuple3<GradoopId, Edge, Double>, Edge>() {
            public void reduce(Iterable<Tuple3<GradoopId, Edge, Double>> in, Collector<Edge> out) {
//                List<Tuple3<GradoopId, E, Double>> topN = new ArrayList<Tuple3<GradoopId, E, Double>>();
//                Integer topNMaxsize = executeN;
//                double min = 1;
//                double potentialMin = 1;
//                for (Tuple3<GradoopId, E, Double> edgeId_simValue : in) {
//                    if (topN.size() < topNMaxsize) {
//                        topN.add(edgeId_simValue);
//                        if (edgeId_simValue.f2 < min)
//                            min = edgeId_simValue.f2;
//                    }
//                    else {
//                        if (edgeId_simValue.f2 > min){
//                            topN.add(edgeId_simValue);
//                            for (Tuple3<GradoopId, E, Double> topNElement : topN) {
//                                potentialMin = min;
//                                if (topNElement.f2 == min)
//                                    topN.remove(topNElement);
//
//                                else if (topNElement.f2 < potentialMin) {
//                                    potentialMin = topNElement.f2;
//                                }
//                            }
//                            min = potentialMin;
//                        }
//
//                    }
//                }
//                for (Tuple3<GradoopId, E, Double> topNElement : topN)
//                    out.collect(topNElement.f1);
                int i=0;
                for (Tuple3<GradoopId, Edge, Double> edgeId_edge_simValue : in) {
                    i++;
                    if (i<=executeN)
                        out.collect(edgeId_edge_simValue.f1);
                }

            }
        });
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), limitedEdges);
    }
    public String getName() {
        return MaxN.class.getName();
    }

}

