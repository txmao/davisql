#!bin/sh
cdir=$(pwd)
cd $cdir/src
javac davisql.java
java davisql
rm *.class