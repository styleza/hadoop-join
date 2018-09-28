#!/bin/bash

for i in  2 ;
do
cd "program"$i
rm *.class
hadoop com.sun.tools.javac.Main *.java
jar cf mf.jar *.class
hadoop jar mf.jar MultipleFiles /input/student_5000000.txt /input/score_500000.txt /output
hadoop fs -cat /output/part-r-00000
cd ..
done
