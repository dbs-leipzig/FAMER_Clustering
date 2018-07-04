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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 */
public class Join2 implements JoinFunction <Tuple5<String, String, String, String, Double>, Tuple3<String, String, String>, Tuple7<String, String, String, String, String, String, Double>> {
    @Override
    public Tuple7<String, String, String, String, String, String, Double> join(Tuple5<String, String, String, String, Double> in1, Tuple3<String, String, String> in2) throws Exception {
        return Tuple7.of(in1.f0, in2.f1, in2.f2, in1.f1, in1.f2, in1.f3,
                in1.f4);
    }
}
