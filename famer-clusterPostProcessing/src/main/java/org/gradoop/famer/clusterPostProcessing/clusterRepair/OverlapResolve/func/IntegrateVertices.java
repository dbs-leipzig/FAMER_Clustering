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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

public class IntegrateVertices implements GroupReduceFunction<Tuple3<Vertex, String, String>, Tuple3<Cluster, String, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Vertex, String, String>> iterable, Collector<Tuple3<Cluster, String, String>> collector) throws Exception {
        Collection<Vertex> vertices = new ArrayList<>();
        String oldClsId = "";
        String newClsId = "";
        Boolean f1 = false;
        boolean f2 = false;

        for (Tuple3<Vertex, String, String> i : iterable) {

            oldClsId = i.f1;
            if (i.f0 != null) { // resolve
                i.f0.setProperty("ClusterId", i.f2);
                vertices.add(i.f0);
                f1 = true;

            }
            else {//merge case

                newClsId = i.f2;
                f2 = true;
            }
        }
        if (f1&& f2)
            System.out.print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
        collector.collect(Tuple3.of(new Cluster(vertices), oldClsId, newClsId));
    }
}