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

package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class Vertices2Collections implements GroupReduceFunction <Tuple2<Vertex, String>, Tuple2<Collection<Vertex>, String>>{
    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> in, Collector<Tuple2<Collection<Vertex>, String>> out) throws Exception {
        Collection<Vertex> vertices = new ArrayList<>();
        Collection<String> gradoopIds = new ArrayList<>();
        String clusterId = "";
        for (Tuple2<Vertex, String> elem:in){
            clusterId = elem.f1;
            if (!gradoopIds.contains(elem.f0.getId().toString())){
                gradoopIds.add(elem.f0.getId().toString());
                vertices.add(elem.f0);
            }
        }
        out.collect(Tuple2.of(vertices, clusterId));
    }
}
