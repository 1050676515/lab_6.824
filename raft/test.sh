#!/bin/sh

for i in `seq 100`
do
    go test -run 8Unreliable2C >> a.dat
done
