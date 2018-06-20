
package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.GenerateOutput;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ModifyGraphforClustering;
import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ERClustering.LongMaxAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * The implementation of Correlation Clustering (Pivot) algorithm.
 */

//to do: add epsilon as a parameter

public class CorrelationClustering
        implements UnaryGraphToGraphOperator {
    private static boolean IsEdgesFull;
    private static ClusteringOutputType clusteringOutputType;

    public CorrelationClustering(boolean isEdgesBiderection, ClusteringOutputType ClusteringOutputType){
        IsEdgesFull = isEdgesBiderection;
        clusteringOutputType = ClusteringOutputType;

    }

    
    public String getName() {
        return CorrelationClustering.class.getName();
    }

    
    public LogicalGraph execute(LogicalGraph input) {// set up execution environment
        input = input.callForGraph(new ModifyGraphforClustering());

        Graph graph = Graph.fromDataSet(
                input.getVertices().map(new ToGellyVertexWithIdValue()),
                input.getEdges().flatMap(new ToGellyEdgeforSGInput()),
                input.getConfig().getExecutionEnvironment()
        );
        DataSet<org.gradoop.common.model.impl.pojo.Edge> edges  = input.getEdges();
        GradoopFlinkConfig config = input.getConfig();
        input = null;


        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        LongMaxAggregator longMaxAggregator = new LongMaxAggregator();
        parameters.registerAggregator("MaxDegree", longMaxAggregator);
        parameters.setSolutionSetUnmanagedMemory(true);
        Graph result = graph.runScatterGatherIteration(new myScatter(), new myGather(), Integer.MAX_VALUE, parameters);


        LogicalGraph resultLG = config.getLogicalGraphFactory().fromDataSets(result.getVertices().map(new Center.ToGradoopVertex()), edges);
        resultLG = resultLG.callForGraph(new GenerateOutput(clusteringOutputType));

        return resultLG;
    }




    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
        
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
            GradoopId id = in.getId();
//            in.setProperty("VertexPriority", Long.parseLong(in.getPropertyValue("id").toString()));
//            Long vp = Long.parseLong(in.getPropertyValue("VertexPriority").toString());
            in.setProperty("ClusterId", 0L);
            in.setProperty("IsCenter", false);
            in.setProperty("roundNo", 0);
            return new org.apache.flink.graph.Vertex(id,in);
        }
    }
    public static final class ToGradoopVertex
            implements MapFunction<org.apache.flink.graph.Vertex, Vertex> {
        
        public Vertex map(org.apache.flink.graph.Vertex in) throws Exception {
            return (Vertex) in.getValue();
        }
    }

    public class ToGellyEdgeforSGInput<E extends EPGMEdge>
            implements FlatMapFunction<E, Edge<GradoopId, Double>> {
        
        public void flatMap(E e, Collector<Edge<GradoopId, Double>> out) throws Exception {
            out.collect(new Edge(e.getSourceId(), e.getTargetId(), 0.0));
            if (!IsEdgesFull)
                out.collect(new Edge(e.getTargetId(), e.getSourceId(), 0.0));
        }
    }


/**************************************************************************************************************************/
/**************************************************************************************************************************/
/**************************************************************************************************************************/
    /**************************************************************************************************************************/


    public static final class myScatter extends ScatterFunction<GradoopId, Vertex, Long, Double> {

        public void sendMessages(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex) {
            int SubSuperStep = getSuperstepNumber() % 3;
            switch (SubSuperStep){
                case 1 : // find max degree
                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0) {
                        for (Edge<GradoopId, Double> edge : getEdges()) {
                            sendMessageTo(edge.getTarget(), 1L);
                        }
                        sendMessageTo(vertex.getId(),0L);
                    }
                    break;

                case 2: //select centers
                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0){
                        double randomNum = Math.random();
                        LongValue currentMaxDegree = getPreviousIterationAggregate("MaxDegree");
//                        System.out.println(getSuperstepNumber()+" "+currentMaxDegree);
                        double epsilon = 0.9;
                        double CenterSelectionProbability =  epsilon / (double) (currentMaxDegree.getValue());
                        if (randomNum <= CenterSelectionProbability) {
                            Long vp = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                            for (Edge <GradoopId, Double> edge : getEdges()) {
                                sendMessageTo(edge.getTarget(), vp);
                            }
                            sendMessageTo(vertex.getId(), vp);
                        }
                        else{// send fake msg//
                            sendMessageTo(vertex.getId(), 0L);
                        }
                    }
                    break;
                case 0: // grow cluster around centers
//                    System.out.println(getSuperstepNumber());


                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0){
                        if(vertex.f1.getPropertyValue("IsCenter").getBoolean()) {
                            Long vp = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                            for (Edge<GradoopId, Double> edge : getEdges()) {
                                sendMessageTo(edge.getTarget(), vp);
                            }
                            sendMessageTo(vertex.getId(), vp);
                        }
                        else {
                            sendMessageTo(vertex.getId(), 0L);
                        }
                    }
            }
        }
    }

    /**************************************************************************************************************************/

    public static final  class myGather extends GatherFunction<GradoopId, Vertex, Long> {

        LongMaxAggregator aggregator = new LongMaxAggregator();
        public void preSuperstep() {
            // retrieve the Aggregator
            aggregator = getIterationAggregator("MaxDegree");
        }
        public void updateVertex(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex, MessageIterator<Long> inMessages) {
            int SubSuperStep = getSuperstepNumber() % 3;
            switch (SubSuperStep){
                case 1 : // find max degree
                    Long degree = 0L;
                    if(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0) {
                        for (Long msg : inMessages) {
                            degree +=msg;
                        }
                        aggregator.aggregate(degree.intValue());
                    }
                    setNewVertexValue(vertex.f1);
                    break;
                case 2: // select centers
                    int msgCnt=0;
                    boolean sameVertex = false;
                    Long vertexPriority = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                    for (Long msg : inMessages) {
                        if (msg != 0) {
                            if (msg.equals(vertexPriority)) { //msg != 0: do not consider fake msges
                                sameVertex = true;
                            }
                            else
                                msgCnt ++;
                        }
                    }

                    if(msgCnt ==0 && sameVertex) {
                        vertex.f1.setProperty("IsCenter", true);
                    }
                    setNewVertexValue(vertex.f1);
                    break;

                case 0: // grow cluster around centers
                    vertex.f1.setProperty("roundNo",getSuperstepNumber()/3);
                    if (Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()) == 0) {// if the vertex is unassigned

                        Long MinClusterId = Long.MAX_VALUE;
                        for (Long msg : inMessages) {
                            if (msg < MinClusterId && msg!=0)
                                MinClusterId = msg;// concurrency rule no. 2
                        }

                        if (MinClusterId != Long.MAX_VALUE)
                            vertex.f1.setProperty("ClusterId", MinClusterId);
                        setNewVertexValue(vertex.f1);
                    }
            }
        }
    }

}




