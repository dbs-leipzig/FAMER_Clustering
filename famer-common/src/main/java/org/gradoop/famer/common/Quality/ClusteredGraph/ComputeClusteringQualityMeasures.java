package org.gradoop.famer.common.Quality.ClusteredGraph;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.Quality.ClusteredGraph.functions.*;
import org.gradoop.famer.common.functions.getF1Tuple2;
import org.gradoop.famer.common.functions.vertex2vertex_clusterId;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.io.Serializable;


/** It is used when there is no Golden Truth file and only vertices with the same ids (GTIdLabel) are true positives
 * Computes Precision, Recall, and FMeasure of the input
 * The input can be a GraphCollection or a LogicalGraph.
 *
 */
public class ComputeClusteringQualityMeasures {
    private GraphCollection inputGrphCltion;
    private LogicalGraph inputGrph;
    private String GTIdLabel;
    private boolean hasOverlap;



    private static DataSet<Tuple2<String, Long>> tpset;
    private static DataSet<Tuple2<String, Long>> apset;
    private static DataSet<Tuple2<String, Long>> GTRecordsNoSet;
    private static DataSet<Tuple2<String, Long>> repAPSet;
    private static DataSet<Tuple2<String, Long>> repTPSet;

    private static ExecutionEnvironment env;
    private static Long tp;
    private static Long ap;
    private static  Long GTRecordsNo;
    private static Long repPositiveNo;
    private static Long repTruePositiveNo;





    private void constructor(String GTIdLabel, boolean hasOverlap){
        inputGrphCltion = null;
        inputGrph = null;
        ap = tp =   -1L;
        tpset = apset = GTRecordsNoSet =  null;
        this.GTIdLabel = GTIdLabel;
        this.hasOverlap = hasOverlap;
    }


    public ComputeClusteringQualityMeasures(GraphCollection clusteringResult, String GTIdLabel, boolean hasOverlap) {
        constructor(GTIdLabel, hasOverlap);
        inputGrphCltion = clusteringResult;
        env = clusteringResult.getConfig().getExecutionEnvironment();

    }
    public ComputeClusteringQualityMeasures(LogicalGraph clusteringResult, String GTIdLabel, boolean hasOverlap) {
        constructor(GTIdLabel, hasOverlap);
        inputGrph = clusteringResult;
        env = clusteringResult.getConfig().getExecutionEnvironment();

    }


    public void computeSets() throws Exception {

        DataSet<Vertex> resultVertices;
        if (inputGrphCltion != null) {
            resultVertices = inputGrphCltion.getVertices();

        } else {
            resultVertices = inputGrph.getVertices();
        }
        repAPSet = env.fromElements(Tuple2.of("repAP",0l));
        repTPSet = env.fromElements(Tuple2.of("repTP",0l));

        DataSet<Tuple2<GradoopId, String>> vertexId_RecId = resultVertices.map(new MapFunction<Vertex, Tuple2<GradoopId, String>>() {
            public Tuple2<GradoopId, String> map(Vertex in) {
                String recId= in.getPropertyValue("recId").toString();
                return Tuple2.of(in.getId(), recId);

            }
        });
        GTRecordsNoSet = vertexId_RecId.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<GradoopId, String>, Tuple2<String, Long>>() {
            public void reduce(Iterable<Tuple2<GradoopId, String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
                Long cnt = 0l;
                for (Tuple2<GradoopId, String> v:values) {
                    cnt++;
                }

                out.collect(Tuple2.of("gt",(cnt*(cnt-1)/2)));
            }
        });
        GTRecordsNoSet = GTRecordsNoSet.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of("gt",value1.f1+value2.f1);
            }
        });
        if(hasOverlap){
            DataSet<Tuple3<String, String, String>> vertexId_pubId_ClusterId = resultVertices.flatMap(new FlatMapFunction<Vertex, Tuple3<String, String, String>>() {
                @Override
                public void flatMap(Vertex vertex, Collector<Tuple3<String, String, String>> out) throws Exception {
                    if (vertex.getPropertyValue("ClusterId").toString().contains(",")){
                        String[] clusterIds = vertex.getPropertyValue("ClusterId").toString().split(",");
                        String recId= vertex.getPropertyValue(GTIdLabel).toString();
                        for (int i=0; i <clusterIds.length; i++) {
                            out.collect(Tuple3.of(vertex.getId().toString(), recId, clusterIds[i]));
                        }

                    }
                }
            });

            DataSet<Tuple2<String, String>> pairedVertices__pubId1pubId2 = vertexId_pubId_ClusterId.join(vertexId_pubId_ClusterId).where(2).equalTo(2).with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple6<String, String,String, String, String, String>>() {
                public Tuple6<String, String, String, String,String, String> join(Tuple3<String, String, String> in1, Tuple3<String,String, String> in2) {
                    return Tuple6.of(in1.f0, in1.f1, in1.f2,in2.f0, in2.f1, in2.f2);

                }
            }).flatMap(new FlatMapFunction<Tuple6<String, String, String, String,String, String>, Tuple2<String, String>>() {
                public void flatMap(Tuple6<String, String, String, String, String, String> in, Collector<Tuple2<String, String>> out) {
                    if (in.f0.compareTo(in.f3)<0) {
                        out.collect(Tuple2.of(in.f0 + "," + in.f3, in.f1+","+ in.f4 ));
                    }
                }
            });
            repAPSet =  pairedVertices__pubId1pubId2.groupBy(0).reduceGroup(new ComputeCountbyReduce()).reduce(new reduceLongtoSet()).map(new LongtoSet("repAP"));
            pairedVertices__pubId1pubId2 =  pairedVertices__pubId1pubId2.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                @Override
                public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String, String>> out) throws Exception {
                    if (in.f1.split(",")[0].equals(in.f1.split(",")[1])) {
                        out.collect(in);
                    }
                }
            });
            repTPSet = pairedVertices__pubId1pubId2.groupBy(0).reduceGroup(new ComputeCountbyReduce()).reduce(new reduceLongtoSet()).map(new LongtoSet("repTP"));
            resultVertices = resultVertices.flatMap(new FlatMapFunction<Vertex, Vertex>() {
                public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
                    String clusterids = vertex.getPropertyValue("ClusterId").toString();
                    if (clusterids.contains(",")) {
                        String[] clusteridsArray = clusterids.split(",");
                        for (int i=0;i<clusteridsArray.length;i++) {
                            vertex.setProperty("ClusterId",clusteridsArray[i]);
                            collector.collect(vertex);
                        }
                    }
                    else
                        collector.collect(vertex);
                }
            });

        }

      //////All Positives
        DataSet < Tuple1 < String >> ClusterIds = resultVertices.flatMap(new vertex2vertex_clusterId(true)).map(new getF1Tuple2()).map(new MapFunction<String, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(String value) throws Exception {
                return Tuple1.of(value);
            }
        });


        DataSet<Tuple2<Long, Long>> clstrSize_apNo =  ClusterIds.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple2<Long, Long>>() {
            @Override
            public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple2<Long, Long>> out) throws Exception {
                long cnt=0l;
                for (Tuple1<String> i:in)
                    cnt++;
                out.collect(Tuple2.of(cnt,cnt*(cnt-1)/2));
            }
        });

        apset= clstrSize_apNo.reduce(new reduceTuple2toSet(1)).map(new Tuple2toSet("ap",1));



        ///////True Positives
        DataSet<Tuple1<String>> ClusterId_GTId = resultVertices.map(new MapFunction<Vertex, Tuple1<String>>() {
            public Tuple1<String> map(Vertex in) {
                String recId= in.getPropertyValue("recId").toString();

                return Tuple1.of(in.getPropertyValue("ClusterId").toString()+","+recId);

            }
        });
        tpset = ClusterId_GTId.groupBy(0).
                reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple2<String, Long>>() {
                    @Override
                    public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple2<String, Long>> out) throws Exception {
                        long cnt = 0l;
                        for (Tuple1<String> i:in)
                            cnt++;
                        out.collect(Tuple2.of("tp",cnt*(cnt-1)/2));
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> in1, Tuple2<String, Long> in2) throws Exception {
                return Tuple2.of("tp", in1.f1+in2.f1);
            }
        });


    }


    private void computeValues() throws Exception {

        if (tpset == null)
            computeSets();

        DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(GTRecordsNoSet).union(repAPSet).union(repTPSet);
        repPositiveNo=repTruePositiveNo=0l;

        for (Tuple2<String, Long> i : sets.collect()) {
            if (i.f0.equals("ap")) {
                ap = i.f1;
            }
            else if (i.f0.equals("tp")) {
                tp = i.f1;
            }
            else if (i.f0.equals("gt")) {
                GTRecordsNo = i.f1;
            }
            else if (i.f0.equals("repAP")) {
                repPositiveNo = i.f1;
            }
            else if (i.f0.equals("repTP")) {
                repTruePositiveNo = i.f1;
            }

        }

        if (hasOverlap){
            ap -= repPositiveNo;
            tp -= repTruePositiveNo;
        }
    }








    public Double computePrecision() throws Exception {
        if (tp == -1)
            computeValues();
        return (double) tp / ap;
    }

    public Double computeRecall() throws Exception {
        if (tp == -1)
            computeValues();
        return (double) tp / GTRecordsNo;
    }

    public Double computeFM() throws Exception {
        if (tp == -1)
            computeValues();
        double pr = computePrecision();
        double re = computeRecall();
        return 2 * pr * re / (pr + re);
    }

}

