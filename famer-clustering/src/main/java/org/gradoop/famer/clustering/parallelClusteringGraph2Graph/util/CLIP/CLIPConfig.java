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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP;

/**
 * CLIP Algorithm input parameters
 */
public class CLIPConfig {
    private Double delta;
    private Integer sourceNo;
    private boolean removeSrcConsistentVertices;
    private Double simValueCoef;
    private Double degreeCoef;
    private Double strengthCoef;

    public CLIPConfig (){
        delta = 0d;
        removeSrcConsistentVertices = false;
        simValueCoef = 0.5;
        degreeCoef = 0.2;
        strengthCoef = 0.3;
    }

    public void setDelta(Double delta) {
        this.delta = delta;
    }

    public void setStrengthCoef(Double strengthCoef) {
        this.strengthCoef = strengthCoef;
    }

    public void setDegreeCoef(Double degreeCoef) {
        this.degreeCoef = degreeCoef;
    }

    public void setSimValueCoef(Double simValueCoef) {
        this.simValueCoef = simValueCoef;
    }

    public void setRemoveSrcConsistentVertices(boolean removeSrcConsistentVertices) {
        this.removeSrcConsistentVertices = removeSrcConsistentVertices;
    }

    public void setPhase2Method() {}

    public void setSourceNo(Integer sourceNo) {
        this.sourceNo = sourceNo;
    }

    public Double getDelta() {

        return delta;
    }

    public Integer getSourceNo() {
        return sourceNo;
    }


    public boolean isRemoveSrcConsistentVertices() {
        return removeSrcConsistentVertices;
    }

    public Double getSimValueCoef() {
        return simValueCoef;
    }

    public Double getDegreeCoef() {
        return degreeCoef;
    }

    public Double getStrengthCoef() {
        return strengthCoef;
    }
}
