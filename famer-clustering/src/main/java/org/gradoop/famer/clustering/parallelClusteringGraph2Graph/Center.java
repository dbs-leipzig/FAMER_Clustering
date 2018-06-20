package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;

/**
 * The implementation of Center algorithm.
 */


import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
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
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ERClustering.CenterMessage;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


public class Center
        implements UnaryGraphToGraphOperator {
    private int prio;
    private static boolean IsEdgesFull;
    private static ClusteringOutputType clusteringOutputType;


    public String getName() {
        return Center.class.getName();
    }
    public Center(int prioIn, boolean isEdgeFullIn, ClusteringOutputType clusteringOutputType){
        prio = prioIn;
        IsEdgesFull = isEdgeFullIn;
        this.clusteringOutputType = clusteringOutputType;
    }

    public LogicalGraph execute(LogicalGraph input) {// set up execution environment
        input = input.callForGraph(new ModifyGraphforClustering());
        Graph graph = Graph.fromDataSet(
                input.getVertices().map(new ToGellyVertexWithIdValue()),
                input.getEdges().flatMap(new ToGellyEdgeforSGInput()),
                input.getConfig().getExecutionEnvironment()
        );
        GradoopFlinkConfig config = input.getConfig();
        input = null;


        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        LongSumAggregator lsa = new LongSumAggregator();
        parameters.registerAggregator("IsFinished", lsa);
        parameters.setSolutionSetUnmanagedMemory(true);


        Graph result = graph.runScatterGatherIteration(new myScatter(), new myGather(prio), Integer.MAX_VALUE, parameters);

        LogicalGraph resultLG = config.getLogicalGraphFactory().fromDataSets(result.getVertices().map(new ToGradoopVertex()), input.getEdges());
        resultLG = resultLG.callForGraph(new GenerateOutput(clusteringOutputType));

        return resultLG;
    }

    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
            GradoopId id = in.getId();
//            in.f1.setProperty("VertexPriority", in.f0);
            in.setProperty("ClusterId", in.getPropertyValue("VertexPriority").toString());
            in.setProperty("IsCenter", false);
            in.setProperty("IsNonCenter", false);
            in.setProperty("Deciding", "");
            in.setProperty("state", 0);
            return new org.apache.flink.graph.Vertex<GradoopId, Vertex>(id, in);
        }
    }
    public static final class ToGradoopVertex
            implements MapFunction<org.apache.flink.graph.Vertex, Vertex> {
        public Vertex map(org.apache.flink.graph.Vertex in) throws Exception {
            return (org.gradoop.common.model.impl.pojo.Vertex) in.getValue();
        }
    }

    public class ToGellyEdgeforSGInput<E extends EPGMEdge>
            implements FlatMapFunction<E, Edge<GradoopId, Double>> {
        public void flatMap(E e, Collector<Edge<GradoopId, Double>> out) throws Exception {
            out.collect(new Edge(e.getSourceId(), e.getTargetId(), Double.parseDouble(e.getPropertyValue("value").toString())));
            if (!IsEdgesFull)
                out.collect(new Edge(e.getTargetId(), e.getSourceId(), Double.parseDouble(e.getPropertyValue("value").toString())));
        }
    }

/**************************************************************************************************************************/
/**************************************************************************************************************************/
/**************************************************************************************************************************/
    /**************************************************************************************************************************/


    public static final class myScatter extends ScatterFunction<GradoopId, Vertex, CenterMessage, Double> {

        public void sendMessages(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex) {
//            System.out.println(getSuperstepNumber());
            int supNo = getSuperstepNumber() % 2 ;
            if(getSuperstepNumber()==1 ||(supNo == 1 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString())!= 0)){//identify potential centers

                Long vertexPrio = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                CenterMessage msg = new CenterMessage(vertexPrio,0.0);
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    msg.setSimDegree(edge.getValue());
                    sendMessageTo(edge.getTarget(), msg);

                }
            }
            else if (supNo == 0 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString())!= 0){//
                CenterMessage msg = new CenterMessage(Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()), 0.0);
                String addInfo = "";
                if (vertex.f1.getPropertyValue("IsCenter").getBoolean())
                    addInfo += "IsCenter";
                else if (vertex.f1.getPropertyValue("IsNonCenter").getBoolean()) {
                    addInfo += "IsNonCenter";
                }
                else {
                    addInfo += vertex.f1.getPropertyValue("Deciding");
                }
                msg.setAdditionalInfo(addInfo);
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    msg.setSimDegree(edge.getValue());
                    sendMessageTo(edge.getTarget(), msg);
                }

            }
        }
    }

    /**************************************************************************************************************************/

    public static final  class myGather extends GatherFunction<GradoopId, Vertex, CenterMessage> {
        private int prio;
        //prio 1: selecet the center with the highest vertxpriority as center
        //prio 0: selecet the center with the lowest vertxpriority as center
        LongSumAggregator aggregator = new LongSumAggregator();
        public void preSuperstep() {
            // retrieve the Aggregator
            aggregator = getIterationAggregator("IsFinished");
        }
        public myGather(int in) {
            prio = in;
        }
        public void updateVertex(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex, MessageIterator<CenterMessage> inMessages) {
            int supNo = getSuperstepNumber() % 2 ;
            if (supNo == 1) {

                String decideing = "";
                if (!vertex.f1.getPropertyValue("IsCenter").getBoolean() && !vertex.f1.getPropertyValue("IsNonCenter").getBoolean() ) {
                    int state = Integer.parseInt(vertex.f1.getPropertyValue("state").toString());
                    List<CenterMessage> msgList = new ArrayList<CenterMessage>();
                    for (CenterMessage msg : inMessages) {
                        msgList.add(msg);
                    }
//                    System.out.println(msgList.size());

                    CenterMessage[] msgArray = msgList.toArray(new CenterMessage[msgList.size()]);

//                    for (int i=0; i < msgArray.length;i++){
//                        CenterMessage maxCntrMsg = new CenterMessage(0L,-10.0);
//                        int maxIndex = 0;
//                        for (int j=i; j < msgArray.length;j++){
//                            Double res = msgArray[j].getSimDegree()-maxCntrMsg.getSimDegree();
//                            if (res > 0){
//                                maxCntrMsg = msgArray [j];
//                                maxIndex = j;
//                            }
//                        }
//                        CenterMessage temp = msgArray[i];
//                        msgArray[i] = maxCntrMsg;
//                        msgArray [maxIndex] = temp;
//                    }
                    Arrays.sort(msgArray, new Comparator<CenterMessage>() {
                        public int compare(CenterMessage in1, CenterMessage in2) {
                            try {


                                if (in1.getSimDegree() > in2.getSimDegree())
                                    return -1;
                                if (in1.getSimDegree() < in2.getSimDegree())
                                    return 1;
                                if (in1.getClusterId() < in2.getClusterId() && prio == 0)
                                    return -1;
                                if (in1.getClusterId() > in2.getClusterId() && prio == 1)
                                    return -1;
                                if (in1.getClusterId() > in2.getClusterId() && prio == 0)
                                    return 1;
                                if (in1.getClusterId() < in2.getClusterId() && prio == 1)
                                    return 1;
                                return 0;
                            }
                            catch (Exception e){
                                System.out.println(e.getMessage());
                            }
                            return 0;
                        }
                    });
                    if (state < msgArray.length )
                        decideing += msgArray[state].getClusterId()+","+vertex.f1.getPropertyValue("VertexPriority");
                }
                aggregator.aggregate(1);
                vertex.f1.setProperty("Deciding", decideing);
                setNewVertexValue(vertex.f1);

            }
            else if (supNo == 0) {
                if(!vertex.f1.getPropertyValue("IsCenter").getBoolean() && !vertex.f1.getPropertyValue("IsNonCenter").getBoolean()) {
                    List<CenterMessage> msgList = new ArrayList<CenterMessage>();
                    for (CenterMessage msg : inMessages) {
                        msgList.add(msg);
                    }

                    CenterMessage[] msgArray = msgList.toArray(new CenterMessage[msgList.size()]);
//                    for (int i=0; i < msgArray.length;i++){
//                        CenterMessage maxCntrMsg = new CenterMessage(0L,-10.0);
//                        int maxIndex = 0;
//                        for (int j=i; j < msgArray.length;j++){
//                            Double res = msgArray[j].getSimDegree()-maxCntrMsg.getSimDegree();
//                            if (res > 0){
//                                maxCntrMsg = msgArray [j];
//                                maxIndex = j;
//                            }
//                        }
//                        CenterMessage temp = msgArray[i];
//                        msgArray[i] = maxCntrMsg;
//                        msgArray [maxIndex] = temp;
//                    }

                    Arrays.sort(msgArray, new Comparator<CenterMessage>() {
                        public int compare(CenterMessage in1, CenterMessage in2) {
                            try {
                                if (in1.getSimDegree() > in2.getSimDegree())
                                    return -1;
                                if (in1.getSimDegree() < in2.getSimDegree())
                                    return 1;
                                if (in1.getClusterId() < in2.getClusterId() && prio == 0)
                                    return -1;
                                if (in1.getClusterId() > in2.getClusterId() && prio == 1)
                                    return -1;
                                if (in1.getClusterId() > in2.getClusterId() && prio == 0)
                                    return 1;
                                if (in1.getClusterId() < in2.getClusterId() && prio == 1)
                                    return 1;
                                return 0;
                            }
                            catch (Exception e){
                                System.out.println(e.getMessage());
                            }
                            return 0;
                        }
                    });
                    int state = Integer.parseInt(vertex.f1.getPropertyValue("state").toString());
                    for (int i = state; i < msgArray.length; i++) {
//                        if(vertex.f1.getPropertyValue("author").toString().equals("value2")) {
//                            System.out.println(getSuperstepNumber()+" "+i+" "+msgArray[i].getAdditionalInfo());
//                        }

                        if (msgArray[i].getAdditionalInfo().contains("IsCenter")) {
//                            System.out.println(getSuperstepNumber()+" "+vertex.f1.getPropertyValue("VertexPriority"));
                            vertex.f1.setProperty("ClusterId", Long.parseLong(msgArray[i].getClusterId().toString()));
                            vertex.f1.setProperty("IsNonCenter", true);
                            break;
                        }
                        if (msgArray[i].getAdditionalInfo().contains("IsNonCenter")) {
//                            System.out.println(getSuperstepNumber()+" "+vertex.f1.getPropertyValue("VertexPriority"));

                            vertex.f1.setProperty("state", state+1);
                        }
                        else {
                            aggregator.aggregate(1);
                            Long selectedClusterId = Long.parseLong(msgArray[i].getAdditionalInfo().split(",")[0]);
                            Long origin = Long.parseLong(msgArray[i].getAdditionalInfo().split(",")[1]);
                            Long vertexPriority = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                            if (selectedClusterId.equals(vertexPriority)) {
//                                System.out.println(getSuperstepNumber()+" "+vertex.f1.getPropertyValue("VertexPriority"));
                                if ((origin > vertexPriority && prio == 0)||(origin < vertexPriority && prio == 1)) {
                                    vertex.f1.setProperty("IsCenter", true);
                                    vertex.f1.setProperty("ClusterId", vertexPriority);
//                                    System.out.println("parallel "+vertexPriority);
                                }
                                break;
                            }
                            break;
//                            if (i == msgArray.length) {
//                                vertex.f1.setProperty("IsNonCenter", true);
//                            }
                        }
                    }
                }
                setNewVertexValue(vertex.f1);
            }
        }
    }
}





