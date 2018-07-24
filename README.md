# FAMER_Clustering
<a href="yahoo.com">FAMER</a> is a research project designed for FAst Multi-source Entity Resolution. It is implemented on top of Apache Flink and the graph analytics tool Gradoop (link). 
The framework is still highly under development. So the whole code of FAMER is not publicly available yet. This repository provides the new clustering algorithm CLIP as well as the repair algorithm RLIP that we presented in ESWC 2018 paper. 

In this repository you can find the following modules of FAMER:

famer-clustering: it contains CLIP and the baseline method Connected Components.
famer-clusterPostProcessing: it contains the implementation of overlapResolve algorithm. Even though this case is meaningless in the context of entity resolution, some ER clustering algorithms result into overlapped clusters. 
The overlapResolve algorithm resolves entities that are shared between several clusters and assigns them to only one cluster. 
famer-common: contains some APIs that are used in other modules.
famer-example: it contains the example scripts for both CLIP and RLIP algorithms as well as computing the input graphs and clustering output in terms of FMeasure.
inputGraphs: in this folder you can find all generated input graphs by FAMER that we reported in our papers 1 and 2 for all three datasets we listed. The raw data of all datasets are available in FAMER homepage.
