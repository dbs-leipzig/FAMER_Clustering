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

package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class FilterOverlappedVertices implements FlatMapFunction <Vertex, Vertex> {
    private Integer overlappingLength;
    public FilterOverlappedVertices(Integer OverlappingLength){
        overlappingLength = OverlappingLength;
    }
    public FilterOverlappedVertices(){
        overlappingLength = -1;
    }

    @Override
    public void flatMap(Vertex in, Collector<Vertex> out) throws Exception {
        String ClusterIds = in.getPropertyValue("ClusterId").toString();
        if (ClusterIds.contains(",")) {
            if(overlappingLength==2)
            for(String s:ClusterIds.split(","))
            System.out.println(s);
//            if (overlappingLength == -1)
                    out.collect(in);
//            else if (ClusterIds.split(",").length == overlappingLength)
//                out.collect(in);

        }
    }
}
