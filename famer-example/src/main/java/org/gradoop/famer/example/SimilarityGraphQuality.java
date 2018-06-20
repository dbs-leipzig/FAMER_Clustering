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
    }
}
