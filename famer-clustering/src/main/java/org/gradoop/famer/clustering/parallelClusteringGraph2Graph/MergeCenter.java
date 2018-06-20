
package org.gradoop.famer.clustering.parallelClusteringGraph2Graph;


import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
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
/**
 * The implementation of Merge Center algorithm
 */

public class MergeCenter
        implements UnaryGraphToGraphOperator {
    private int prio;
    private static boolean IsEdgesFull;
    private double threshold;
    private static ClusteringOutputType clusteringOutputType;

    
    public String getName() {
        return MergeCenter.class.getName();
    }

    public MergeCenter(int prioIn, double thresholdIn, boolean IsEdgesFullIn, ClusteringOutputType ClusteringOutputType) {
        prio = prioIn;
        IsEdgesFull = IsEdgesFullIn;
        threshold = thresholdIn;
        clusteringOutputType = ClusteringOutputType;
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
        LongSumAggregator lsa = new LongSumAggregator();
        parameters.registerAggregator("IsFinished", lsa);
        LongSumAggregator lsa2 = new LongSumAggregator();
        parameters.registerAggregator("Phase2", lsa2);
        LongSumAggregator lsa3 = new LongSumAggregator();
        parameters.registerAggregator("Phase3", lsa3);
        parameters.setSolutionSetUnmanagedMemory(true);
        Graph result = graph.runScatterGatherIteration(
               new myScatter(), new myGather(prio, threshold), Integer.MAX_VALUE, parameters); //

        LogicalGraph resultLG = config.getLogicalGraphFactory().fromDataSets(result.getVertices().map(new Center.ToGradoopVertex()), edges);
        resultLG = resultLG.callForGraph(new GenerateOutput(clusteringOutputType));

        return resultLG;
    }


    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
        
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
            GradoopId id = in.getId();
//            in.setProperty("VertexPriority", Long.parseLong(in.getPropertyValue("id").toString()));
            Long vp = Long.parseLong(in.getPropertyValue("VertexPriority").toString());
            in.setProperty("ClusterId", vp);
            in.setProperty("IsCenter", false);
            in.setProperty("IsNonCenter", false);
            in.setProperty("Deciding", "");
            in.setProperty("state", 0);
            in.setProperty("CenterDegree", 0.0);
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


        LongSumAggregator phase2 = new LongSumAggregator();
        LongSumAggregator phase3 = new LongSumAggregator();


        public void preSuperstep() {
            // retrieve the Aggregator
            phase2 = getIterationAggregator("Phase2");
            phase3 = getIterationAggregator("Phase3");
        }

        public void sendMessages(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex) {

            int supNo = getSuperstepNumber() % 2;
            if (getSuperstepNumber() == 1 || (supNo == 1 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString()) != 0)) {//identify potential centers
                Long vertexPrio = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                CenterMessage msg = new CenterMessage(vertexPrio, 0.0);
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    msg.setSimDegree(edge.getValue());
                    sendMessageTo(edge.getTarget(), msg);
                }
            }
            else if (supNo == 0 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString()) != 0) {//
                CenterMessage msg = new CenterMessage(Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()), 0.0);
                String addInfo = "";
                if (vertex.f1.getPropertyValue("IsCenter").getBoolean())
                    addInfo += "IsCenter";
                else if (vertex.f1.getPropertyValue("IsNonCenter").getBoolean()) {
                    addInfo += "IsNonCenter";
                } else {
                    addInfo += vertex.f1.getPropertyValue("Deciding");
                }
                msg.setAdditionalInfo(addInfo);
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    msg.setSimDegree(edge.getValue());
                    sendMessageTo(edge.getTarget(), msg);
                }
            }
            else if(Long.parseLong(getPreviousIterationAggregate("Phase2").toString()) != 0) {

                CenterMessage msg = new CenterMessage(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()), 0.0);
                if (vertex.f1.getPropertyValue("IsCenter").getBoolean())
                    msg.setAdditionalInfo("IsCenter,"+vertex.f1.getPropertyValue("CenterDegree"));
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    msg.setSimDegree(edge.getValue());
                    sendMessageTo(edge.getTarget(), msg);
                }
                msg.setSimDegree(Double.MIN_VALUE);
                sendMessageTo(vertex.getId(), msg);
            }
            else if(Long.parseLong(getPreviousIterationAggregate("Phase3").toString()) != 0) {

                if (vertex.f1.getPropertyValue("IsCenter").getBoolean()) {
                    CenterMessage msg = new CenterMessage(Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString()), 0.0);
                    for (Edge<GradoopId, Double> edge : getEdges()) {
                        msg.setSimDegree(edge.getValue());
                        sendMessageTo(edge.getTarget(), msg);
                    }
                }
            }
        }
    }

    /**************************************************************************************************************************/

    public static final class myGather extends GatherFunction<GradoopId, Vertex, CenterMessage> {
        private int prio;
        private double threshold;
        //prio 1: selecet the center with the highest vertxpriority as center
        //prio 0: selecet the center with the lowest vertxpriority as center
        LongSumAggregator aggregator = new LongSumAggregator();
        LongSumAggregator phase2 = new LongSumAggregator();
        LongSumAggregator phase3 = new LongSumAggregator();

        public void preSuperstep() {
            // retrieve the Aggregator
            aggregator = getIterationAggregator("IsFinished");
            phase2 = getIterationAggregator("Phase2");
            phase3 = getIterationAggregator("Phase3");
        }

        public myGather(int inPrio, double thresholdIn) {
            prio = inPrio;
            threshold = thresholdIn;
        }

        public void updateVertex(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex, MessageIterator<CenterMessage> inMessages) {
            int supNo = getSuperstepNumber() % 2;
            if (getSuperstepNumber() == 1 || (supNo == 1 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString()) != 0)) {//identify potential centers
                String decideing = "";
                if (!vertex.f1.getPropertyValue("IsCenter").getBoolean() && !vertex.f1.getPropertyValue("IsNonCenter").getBoolean()) {
                    int state = Integer.parseInt(vertex.f1.getPropertyValue("state").toString());
                    List<CenterMessage> msgList = new ArrayList<CenterMessage>();
                    for (CenterMessage msg : inMessages) {
                        msgList.add(msg);
                    }
                    CenterMessage[] msgArray = msgList.toArray(new CenterMessage[msgList.size()]);
                    Arrays.sort(msgArray, new Comparator<CenterMessage>() {
                        public int compare(CenterMessage in1, CenterMessage in2) {
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
                    });
                    if (state < msgArray.length)
                        decideing += msgArray[state].getClusterId() + "," + vertex.f1.getPropertyValue("VertexPriority");
                }
                aggregator.aggregate(1);
                vertex.f1.setProperty("Deciding", decideing);
                setNewVertexValue(vertex.f1);
            }
            else if (supNo == 0 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString()) != 0) {//
                phase2.aggregate(1);
                if (!vertex.f1.getPropertyValue("IsCenter").getBoolean() && !vertex.f1.getPropertyValue("IsNonCenter").getBoolean()) {
                    List<CenterMessage> msgList = new ArrayList<CenterMessage>();
                    for (CenterMessage msg : inMessages) {
                        msgList.add(msg);
                    }
                    CenterMessage[] msgArray = msgList.toArray(new CenterMessage[msgList.size()]);
                    Arrays.sort(msgArray, new Comparator<CenterMessage>() {
                        public int compare(CenterMessage in1, CenterMessage in2) {
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
                    });
                    int state = Integer.parseInt(vertex.f1.getPropertyValue("state").toString());

                    for (int i = state; i < msgArray.length; i++) {
                        if (msgArray[i].getAdditionalInfo().contains("IsCenter")) {
                            vertex.f1.setProperty("ClusterId", Long.parseLong(msgArray[i].getClusterId().toString()));
                            vertex.f1.setProperty("IsNonCenter", true);
                            break;
                        }
                        if (msgArray[i].getAdditionalInfo().contains("IsNonCenter")) {
                            vertex.f1.setProperty("state", state + 1);
                        }
                        else {
                            aggregator.aggregate(1);
                            Long selectedClusterId = Long.parseLong(msgArray[i].getAdditionalInfo().split(",")[0]);
                            Long origin = Long.parseLong(msgArray[i].getAdditionalInfo().split(",")[1]);
                            Long vertexPriority = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                            if (selectedClusterId.equals(vertexPriority)) {
                                if ((origin > vertexPriority && prio == 0) || (origin < vertexPriority && prio == 1)) {
                                    vertex.f1.setProperty("IsCenter", true);
                                    vertex.f1.setProperty("ClusterId", vertexPriority);
                                    vertex.f1.setProperty("CenterDegree",msgArray[i].getSimDegree());
                                }
                                break;
                            }
                            break;
                        }
                    }
                }
                setNewVertexValue(vertex.f1);
            }
            else if (Long.parseLong(getPreviousIterationAggregate("Phase2").toString()) != 0) {
//                phase3.aggregate(1);
                CenterMessage selectedCM = new CenterMessage(Long.MAX_VALUE,0.0);
                boolean isDiff = false;
                if(vertex.f1.getPropertyValue("IsCenter").getBoolean()) {
                    for (CenterMessage msg : inMessages) {
                        if (msg.getSimDegree() >= threshold && msg.getSimDegree() <= Double.parseDouble(vertex.f1.getPropertyValue("CenterDegree").toString()) && msg.getClusterId() < selectedCM.getClusterId())
                            selectedCM = msg;
                    }
                }
                else {
                    Long temp = Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString());
                    for (CenterMessage msg : inMessages) {
                        if (msg.getSimDegree() >= threshold && msg.getAdditionalInfo().contains("IsCenter") && msg.getSimDegree() <= Double.parseDouble(msg.getAdditionalInfo().split(",")[1])){
                            if (!msg.getClusterId().equals(temp))
                                isDiff = true;
                            temp = msg.getClusterId();
                            if (msg.getClusterId() < selectedCM.getClusterId())
                                selectedCM = msg;
                        }
                    }
                }
                if (!selectedCM.getClusterId().equals(Long.MAX_VALUE)){
                    if (selectedCM.getClusterId() < Long.parseLong(vertex.f1.getPropertyValue("ClusterId").toString())) {
                        vertex.f1.setProperty("ClusterId", selectedCM.getClusterId());
                        phase2.aggregate(1);//
                    }
                }
                if (isDiff)
                    phase2.aggregate(1);
                setNewVertexValue(vertex.f1);
            }
            else if (Long.parseLong(getPreviousIterationAggregate("Phase3").toString()) != 0) {
                CenterMessage selectedCM = new CenterMessage(0l,Double.MAX_VALUE);
                if(vertex.f1.getPropertyValue("IsCenter").getBoolean()) {
                    for (CenterMessage msg : inMessages) {
                        if (msg.getSimDegree() >  selectedCM.getSimDegree())
                            selectedCM = msg;
                        else if (msg.getSimDegree().equals(selectedCM.getSimDegree())){
                            if ((msg.getClusterId()<selectedCM.getClusterId() && prio==0) || (msg.getClusterId() > selectedCM.getClusterId() && prio==1))
                                selectedCM = msg;
                        }
                    }
                }
                setNewVertexValue(vertex.f1);
            }

        }
    }
}




