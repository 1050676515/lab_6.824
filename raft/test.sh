#!/bin/sh

for i in `seq 100`
do
    go test -run 2A >> a.dat
done
