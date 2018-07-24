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

package org.gradoop.famer.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.common.Quality.InputGraph.ComputeSimGraphQualityMeasures;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;

/**
 */
public class SimilarityGraphQuality {

    public static void main(String args[]) throws Exception {
        String inputGraphPath = args[0];
        String outputPath = args[1];
        new SimilarityGraphQuality().execute(inputGraphPath, outputPath);
    }

    public void execute(String srcFolder, String outFolder) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig((ExecutionEnvironment) env);

        /***************************
         * Load Similarity Graph
         ********************************/
        JSONDataSource dataSource = new JSONDataSource(srcFolder + "graphHeads.json", srcFolder + "vertices.json", srcFolder + "edges.json", grad_config);
        LogicalGraph input = dataSource.getLogicalGraph();
        /***************************
         * Compute Similarity Graph (input) Precision, Recall, FMeasure
         ********************************/
        FileWriter fw = new FileWriter(outFolder+"quality.csv",true);
        fw.append("Pre,Rec,FM\n");
        ComputeSimGraphQualityMeasures eval = new ComputeSimGraphQualityMeasures(input, "recId");
        fw.append(eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+"\n");
        fw.flush();
    }
}
