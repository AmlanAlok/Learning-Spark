#!/bin/bash
echo Starting...

mvn install

spark-submit --class Graph target/*.jar dummy.csv