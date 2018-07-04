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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;


public class Reducer implements GroupReduceFunction <Tuple7<String, String, String, String, String, String, Double>, String>{
    @Override
    public void reduce(Iterable<Tuple7<String, String, String, String, String, String, Double>> iterable, Collector<String> collector) throws Exception {
        HashMap<String, String> vertexId_clusterId = new HashMap<>();
        HashMap<String, Collection<String>> clusterId_srces = new HashMap<>();
        for (Tuple7<String, String, String, String, String, String, Double> srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim:iterable){
            String srcClusterId;
            String trgtClusterId;

            srcClusterId = vertexId_clusterId.get(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f4);
            trgtClusterId = vertexId_clusterId.get(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f5);

            if (srcClusterId == null && trgtClusterId == null){
                vertexId_clusterId.put(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f4, srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f4);
                vertexId_clusterId.put(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f5, srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f4);
                Collection<String> srces = new ArrayList<>();
                srces.add(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f0);
                srces.add(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f1);
                clusterId_srces.put(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f4, srces);
                collector.collect(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f3);
            }
            else if (srcClusterId == null || trgtClusterId == null){
                Collection<String> srces = new ArrayList<>();
                String newVertexId="";
                String newSrc="";
                String theClusterId = "";
                if (srcClusterId == null) {
                    srces = clusterId_srces.get(trgtClusterId);
                    newVertexId = srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f4;
                    newSrc = srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f0;
                    theClusterId = trgtClusterId;
                }
                else {
                    srces = clusterId_srces.get(srcClusterId);
                    newVertexId = srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f5;
                    newSrc = srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f1;
                    theClusterId = srcClusterId;
                }
                if (!srces.contains(newSrc)) {
                    srces.add(newSrc);
                    clusterId_srces.replace(theClusterId, srces);
                    vertexId_clusterId.put(newVertexId, theClusterId);
                    collector.collect(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f3);
                }
            }
            else {
                Collection<String> srcSrces = clusterId_srces.get(srcClusterId);
                Collection<String> trgtSrces =  clusterId_srces.get(trgtClusterId);
                if (isCompatible(srcSrces, trgtSrces)){
                    srcSrces.addAll(trgtSrces);
                    clusterId_srces.replace(srcClusterId, srcSrces);
//                    clusterId_srces.replace(trgtClusterId, srcSrces);
                    clusterId_srces.remove(trgtClusterId);
//                    convert trgtClstrId to srcClstrId
                    ArrayList keyList = new ArrayList(vertexId_clusterId.keySet());
                    Collection<String> changingVertexIds = new ArrayList<>();
                    for (int i = keyList.size() - 1; i >= 0; i--) {
                        //get key
                        String vertexId = (String)keyList.get(i);
//                        System.out.println("Key :: " + key);
                        //get value corresponding to key
                        String clusterId = vertexId_clusterId.get(vertexId);
                        if(clusterId.equals(trgtClusterId))
                            changingVertexIds.add(vertexId);
//                        System.out.println("Value :: " + value);
                    }
                    for(String id:changingVertexIds)
                        vertexId_clusterId.replace(id, srcClusterId);
                    collector.collect(srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_sim.f3);
                }
            }
        }
    }
    private boolean isCompatible (Collection<String> listSrces1, Collection<String> listSrces2){

        for (String s: listSrces2) {
            if (listSrces1.contains(s))
                return false;
        }
        return true;
    }
}
