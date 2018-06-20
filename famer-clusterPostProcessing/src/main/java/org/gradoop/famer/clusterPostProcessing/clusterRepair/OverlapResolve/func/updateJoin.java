package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.ResolveIteration;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Arrays;

public class updateJoin implements JoinFunction<Tuple2<Cluster, String>, Tuple3<Cluster, String, String>, Cluster> {
    private ResolveIteration it;
    public updateJoin(ResolveIteration IT){it=IT;}
    @Override
    public Cluster join(Tuple2<Cluster, String> in1, Tuple3<Cluster, String, String> in2) throws Exception {
        Cluster cluster = in1.f0;
        if (in2 == null) {
            if (it.equals(ResolveIteration.ITERATION2)){
                for(Vertex v: cluster.getVertices()){
                    if (v.getPropertyValue("ClusterId").toString().contains(","))
                        System.out.println(cluster.getClusterId()+"   "+cluster.getVertices().size()+"   PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP");
                }
//                if (cluster.getVertices().size() <=0)
//                    System.out.println("PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPp");
            }
            return cluster;
        }

        else {
            Cluster newVertices = in2.f0;
            String oldClusterId = in2.f1;
            String newClusterId = in2.f2;
            String clusterClusterId = in1.f1;


            if (in2.f2.equals("")) {//resolve case
                for (Vertex v : newVertices.getVertices()) {
                    String newVertexClusterId = v.getPropertyValue("ClusterId").toString();
                    cluster.removeFromVertices(v.getId());
//                    if (newVertexClusterId.contains(",") && it.equals(ResolveIteration.ITERATION2))
//                        System.out.println(newVertexClusterId+ "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
                    if (newVertexClusterId.equals(clusterClusterId) || (newVertexClusterId.contains(",") && Arrays.asList(newVertexClusterId.split(",")).contains(clusterClusterId))) {

                        cluster.addToVertices(v);
                    }
//                    else if (newVertexClusterId.contains("s")){
//                        Collection<Vertex> vertices = new ArrayList<>();
//                        vertices.add(v);
//                        cluster = new Cluster(vertices, newVertexClusterId);
//                    }

                }
            }
            else {//merge case

                for (Vertex v : cluster.getVertices()) {


                    v.setProperty("ClusterId", newClusterId);

                }
                cluster.setClusterId(newClusterId);
            }
            return cluster;
        }
    }
}
