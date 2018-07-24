# FAMER_Clustering
<a href="https://dbs.uni-leipzig.de/research/projects/object_matching/famer">FAMER</a> is a research project designed for <b>FAst Multi-source Entity Resolution</b>. It is implemented on top of <a href="https://flink.apache.org/">Apache Flink</a> and the graph analytics tool <a href="https://dbs.uni-leipzig.de/research/projects/gradoop">Gradoop</a>. 
The framework is still highly under development. So the whole code of FAMER is not publicly available yet. This repository provides the new clustering algorithm <b>CLIP</b> as well as the cluster repair algorithm <b>RLIP</b> that we presented in <a href="https://dbs.uni-leipzig.de/file/eswc_0.pdf">this</a> paper at Extended European Semantic Web Conference in June (<a href="https://2018.eswc-conferences.org/">ESWC 2018</a>). 

In this repository you can find the following modules of FAMER:

<b>famer-clustering</b>: it contains the implementation of <i>CLIP</i> and the baseline method <i>Connected Components</i>.<br>
<b>famer-clusterPostProcessing</b>: it contains the implementation of <i>overlapResolve</i> algorithm. Even though overlapped entities shared between multiple clusters is meaningless in the context of entity resolution, some ER clustering algorithms result into overlapped clusters. The <i>overlapResolve</i> algorithm resolves entities that are shared between several clusters and assigns them to only one cluster. <br>
<b>famer-common</b>: it contains some APIs that are used in other modules.<br>
<b>famer-example</b>: it contains the example scripts for both <i>CLIP</i> and <i>RLIP</i> algorithms as well as computing the quality of input graphs and clustering output in terms of FMeasure. <br>
<b>inputGraphs</b>: in this folder you can find all generated input graphs by FAMER that we reported in our this papers (<a href="https://dbs.uni-leipzig.de/file/famer-adbis2017.pdf">[1]</a> and <a href="https://dbs.uni-leipzig.de/file/eswc_0.pdf">[2]</a>) for all three datasets we listed and made publicly available in <a href="https://dbs.uni-leipzig.de/research/projects/object_matching/famer">FAMER homepage</a>.<br>
