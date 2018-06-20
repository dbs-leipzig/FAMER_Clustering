package org.gradoop.famer.common.Quality.InputGraph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.count.Count;


/**
 * Computes precision, recall, and f-measure of the input graph *
 * It does not consider transitive closure. Number of positives (true positives + false positives) is the number of edges
 * When there is no Golden Truth file but vertices with the same ids are true positives, methods of ths class must be used.
 */

public class ComputeSimGraphQualityMeasures {
    private LogicalGraph inputGrph;
    private static DataSet<Tuple2<String, Long>> tpset;
    private static DataSet<Tuple2<String, Long>> apset;
    private static DataSet<Tuple2<String, Long>> GTRecordsNoSet;
    private static String GTIdLabel;
    private static ExecutionEnvironment env;


    private static Long tp;
    private static Long ap;
    private static Long gtRecorsNo;

    public ComputeSimGraphQualityMeasures(LogicalGraph inputGraph, String GTIdLabel) {
        inputGrph = inputGraph;
        this.GTIdLabel = GTIdLabel;
        tpset = apset = GTRecordsNoSet = null;
        ap = tp = gtRecorsNo = -1L;
        env = inputGraph.getConfig().getExecutionEnvironment();

    }

    private void computeSets() throws Exception {


        DataSet<Tuple2<GradoopId, String>> vertexIdPubId = inputGrph.getVertices().map(new MapFunction<Vertex, Tuple2<GradoopId, String>>() {
            public Tuple2<GradoopId, String> map(Vertex in) {
                String recId = in.getPropertyValue(GTIdLabel).toString().split("s")[0];

                return Tuple2.of(in.getId(), recId);
            }
        });
        GTRecordsNoSet = vertexIdPubId.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<GradoopId, String>, Tuple2<String, Long>>() {
            public void reduce(Iterable<Tuple2<GradoopId, String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
                Long cnt = 0l;
                for (Tuple2<GradoopId, String> v : values)
                    cnt++;
                out.collect(Tuple2.of("gt", (cnt * (cnt - 1) / 2)));
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of("gt", value1.f1 + value2.f1);
            }
        });

        DataSet<Tuple2<GradoopId, GradoopId>> sourceIdtargetId = inputGrph.getEdges().map(new MapFunction<Edge, Tuple2<GradoopId, GradoopId>>() {
            public Tuple2<GradoopId, GradoopId> map(Edge in) {
                return Tuple2.of(in.getSourceId(), in.getTargetId());
            }
        });
        DataSet<Tuple2<String, String>> sourcePubIdtargetPubId = vertexIdPubId.join(sourceIdtargetId).where(0).equalTo(0).with
                (new JoinFunction<Tuple2<GradoopId, String>, Tuple2<GradoopId, GradoopId>, Tuple2<String, GradoopId>>() {
                    public Tuple2<String, GradoopId> join(Tuple2<GradoopId, String> in1, Tuple2<GradoopId, GradoopId> in2) {
                        return Tuple2.of(in1.f1, in2.f1);
                    }
                }).join(vertexIdPubId).where(1).equalTo(0).with
                (new JoinFunction<Tuple2<String, GradoopId>, Tuple2<GradoopId, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> join(Tuple2<String, GradoopId> in1, Tuple2<GradoopId, String> in2) {
                        return Tuple2.of(in1.f0, in2.f1);
                    }
                });

        tpset = sourcePubIdtargetPubId.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
                if (value.f0.equals(value.f1))
                    return Tuple2.of("tp", 1l);
                return Tuple2.of("tp", 0l);
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of("tp", value1.f1 + value2.f1);
            }
        });
        apset = Count.count(inputGrph.getEdges()).map(new MapFunction<Long, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(Long value) throws Exception {
                return Tuple2.of("ap", value);
            }
        });
    }

    private void computeValues() throws Exception {
        if (apset == null)
            computeSets();
        if (apset != null) {

            DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(GTRecordsNoSet);

            for (Tuple2<String, Long> i : sets.collect()) {
                if (i.f0.equals("ap")) {
                    ap = i.f1;
                } else if (i.f0.equals("tp")) {
                    tp = i.f1;
                } else if (i.f0.equals("gt")) {
                    gtRecorsNo = i.f1;
                }
            }

        }

    }


    public Double computePrecision() throws Exception {
        if(tp == -1)
            computeValues();
        return (double)tp/ap;
    }
    public Double computeRecall() throws Exception {
        if(tp == -1)
            computeValues();
        return (double)tp/gtRecorsNo;    }

    public Double computeFM() throws Exception {
        if(tp == -1)
            computeValues();
        double pr = computePrecision();
        double re = computeRecall();
        return 2*pr*re/(pr+re);
    }
}
