

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

/**
 * The implementation of Star algorithm.
 */


public class Star
        implements UnaryGraphToGraphOperator {
    private int prio;
    //prio 1: selecet the center with the highest vertxpriority as center
    //prio 0: selecet the center with the lowest vertxpriority as center
    private int type;
    // 1: degree = number of in/out coming edges
    //2: degree = sum of degrees of in/out coming edges
    private static boolean IsEdgesFull;
    private static ClusteringOutputType clusteringOutputType;

    public String getName() {
        return Star.class.getName();
    }

    public Star(int prioIn, int typeIn, boolean IsEdgesFullIn, ClusteringOutputType ClusteringOutputType) {
        prio = prioIn;
        type = typeIn;
        IsEdgesFull = IsEdgesFullIn;
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
        parameters.setSolutionSetUnmanagedMemory(true);
        Graph result = graph.runScatterGatherIteration(new myScatter(type), new myGather(prio,type), Integer.MAX_VALUE, parameters);
        LogicalGraph resultLG = config.getLogicalGraphFactory().fromDataSets(result.getVertices().map(new ToGradoopVertex()),edges);
        resultLG = resultLG.callForGraph(new GenerateOutput(clusteringOutputType));
        return resultLG;
    }



    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
            GradoopId id = in.getId();
//            in.setProperty("VertexPriority", Long.parseLong(in.getPropertyValue("id").toString()));
            Long vp = Long.parseLong(in.getPropertyValue("VertexPriority").toString());
            in.setProperty("IsCenter", false);
            in.setProperty("ClusterId", "");
            in.setProperty("Degree", 0.0);
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
        private int type;
        LongSumAggregator aggregator = new LongSumAggregator();

        public myScatter(int in) {
            type = in;
        }
        public void preSuperstep() {
            // retrieve the Aggregator
            aggregator = getIterationAggregator("IsFinished");
        }
        public void sendMessages(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex) {
            if (getSuperstepNumber() == 1) {//identify vertex degrees

                Long vertexPrio = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                CenterMessage msg = new CenterMessage(vertexPrio, 0.0);
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    if (type == 1)
                        msg.setSimDegree(1.0);
                    else if (type == 2)
                        msg.setSimDegree(edge.getValue());
                    sendMessageTo(edge.getTarget(), msg);
                }
                msg.setSimDegree(0.0);
                sendMessageTo(vertex.getId(), msg);
            }
            else if (getSuperstepNumber()%2 == 0) {//identify centers
                Long vertexPrio = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                CenterMessage msg = new CenterMessage(vertexPrio, -1.0);
                if (vertex.f1.getPropertyValue("ClusterId").toString().equals("")) {
                    msg.setSimDegree(Double.parseDouble(vertex.f1.getPropertyValue("Degree").toString()));
                    aggregator.aggregate(1);
                }
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), msg);
                }
                sendMessageTo(vertex.getId(), msg);
            }
            else if (getSuperstepNumber() %2 == 1 && Long.parseLong(getPreviousIterationAggregate("IsFinished").toString()) != 0) {//identify clusters
                Long vertexPrio = Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString());
                CenterMessage msg = new CenterMessage(vertexPrio, 0.0);
                if (vertex.f1.getPropertyValue("IsCenter").getBoolean())
                    msg.setAdditionalInfo("IsCenter");
                for (Edge<GradoopId, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), msg);
                }
                sendMessageTo(vertex.getId(), msg);
            }
        }
    }

    /**************************************************************************************************************************/

    public static final class myGather extends GatherFunction<GradoopId, Vertex, CenterMessage> {
        private int prio;
        private int type;

        public myGather(int inPrio, int inType) {
            prio = inPrio;
            type = inType;
        }

        public void updateVertex(org.apache.flink.graph.Vertex<GradoopId, Vertex> vertex, MessageIterator<CenterMessage> inMessages) {
            if (getSuperstepNumber() == 1) {
                Double degree = 0.0;
                int msgCnt = 0;
                for (CenterMessage msg : inMessages) {
                    if(msg.getSimDegree()!=0) {
                        degree += msg.getSimDegree();
                        msgCnt ++;
                    }
                }
                if (type == 2 && msgCnt!=0)
                    degree /= msgCnt;
                vertex.f1.setProperty("Degree", degree);
//                System.out.println("** "+vertex.f1.getPropertyValue("VertexPriority")+" "+degree);
                setNewVertexValue(vertex.f1);
            }
            else if (getSuperstepNumber() %2 == 0) {
                CenterMessage max = new CenterMessage(0L, -1.0);
                for (CenterMessage msg : inMessages) {
                    if(!msg.getSimDegree().equals(-1.0)) {
                        if (msg.getSimDegree() > max.getSimDegree())
                            max = msg;
                        else if (msg.getSimDegree().equals(max.getSimDegree())) {
                            if ((msg.getClusterId() < max.getClusterId() && prio == 0) ||
                                    (msg.getClusterId() > max.getClusterId() && prio == 1))
                                max = msg;
                        }
                    }
                }

                if (max.getClusterId() != 0 && max.getClusterId().equals(Long.parseLong(vertex.f1.getPropertyValue("VertexPriority").toString()))) {
                    vertex.f1.setProperty("IsCenter", true);
                }
                setNewVertexValue(vertex.f1);
            }
            else if (getSuperstepNumber() %2 == 1) {
                String clusterIds = "";
                for (CenterMessage msg : inMessages) {
                    if(msg.getAdditionalInfo().contains("IsCenter"))
                        clusterIds += ","+msg.getClusterId();
                }
                if (!clusterIds.equals(""))
                    clusterIds = clusterIds.substring(1, clusterIds.length());
                vertex.f1.setProperty("ClusterId",clusterIds);
                setNewVertexValue(vertex.f1);
            }
        }
    }
}





