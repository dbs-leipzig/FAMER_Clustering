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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class MakeClusterJoin implements JoinFunction <Tuple2<Collection<Vertex>, String>, Tuple2<Collection<Tuple2<Edge, Boolean>>, String>,
        Cluster> {
    @Override
    public Cluster join(Tuple2<Collection<Vertex>, String> first, Tuple2<Collection<Tuple2<Edge, Boolean>>, String> second) throws Exception {


        Collection<Edge> intraLinks = new ArrayList<>();
        Collection<Edge> interLinks = new ArrayList<>();
        Collection<Vertex> vertices = new ArrayList<>();

        if (second != null) {
            for (Tuple2<Edge, Boolean> edge : second.f0) {
                if (edge.f1)
                    interLinks.add(edge.f0);
                else
                    intraLinks.add(edge.f0);
            }
        }
        String componentId = "";
        if (first.f0.iterator().next().getPropertyValue("ComponentId")!=null)
            componentId = first.f0.iterator().next().getPropertyValue("ComponentId").toString();
        for (Vertex v:first.f0){
            if (!vertices.contains(v))
                vertices.add(v);
        }

        Cluster cluster = new Cluster(vertices, intraLinks, interLinks, first.f1, componentId);


        return cluster;
    }
}
