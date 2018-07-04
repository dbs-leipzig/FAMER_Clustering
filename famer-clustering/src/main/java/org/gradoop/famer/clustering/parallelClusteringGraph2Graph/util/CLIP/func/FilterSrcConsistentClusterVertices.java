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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class FilterSrcConsistentClusterVertices implements GroupReduceFunction <Tuple2<Vertex, String>, Vertex>{
    private Integer sourceNo;
    public FilterSrcConsistentClusterVertices(Integer inputSourceNo){sourceNo = inputSourceNo;}
    public FilterSrcConsistentClusterVertices(){sourceNo = -1;}

    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> iterable, Collector<Vertex> collector) throws Exception {
        Collection<String> sources = new ArrayList<>();
        Collection <Vertex> vertices = new ArrayList<>();
        Boolean isSourceConsistent = true;
        for (Tuple2<Vertex, String> vertex:iterable){
            String src = vertex.f0.getPropertyValue("srcId").toString();
            if (!sources.contains(src))
                sources.add(src);
            else
                isSourceConsistent = false;
            vertices.add(vertex.f0);
        }
        if (isSourceConsistent){
            if ((sourceNo != -1 && vertices.size() == sourceNo) || sourceNo == -1) {
                for (Vertex vertex : vertices)
                    collector.collect(vertex);
            }

        }
    }
}
