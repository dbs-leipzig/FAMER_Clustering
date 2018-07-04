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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class FormSingletons implements FlatMapFunction<Tuple3<Vertex, String, String>, Cluster>{
    @Override
    public void flatMap(Tuple3<Vertex, String, String> input, Collector<Cluster> output) throws Exception {
        if (input.f2.contains("s")){
            Collection<Vertex> vertices = new ArrayList<>();
            input.f0.setProperty("ClusterId", input.f2);
            vertices.add(input.f0);
            output.collect(new Cluster(vertices, input.f2));
        }
    }
}
