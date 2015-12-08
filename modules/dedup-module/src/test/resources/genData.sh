#!/bin/bash
# $1 = 1 => Non-expiry
# $1 = 2 => Ordered
# $1 = 3 => Tupletime
# $1 = 4 => Systemtime
# $1 = 5 => Categorical


if [ $1 -eq 1 ]
then
  cat src/test/resources/base100k.dat | awk -f src/test/resources/genRandomOrdered.awk | grep -v "EXPIRED"
elif [ $1 -eq 2 ]
then
  cat src/test/resources/base100k.dat | awk -f src/test/resources/genRandomOrdered.awk
elif [ $1 -eq 3 ]
then
  cat src/test/resources/base100k.dat | awk -f src/test/resources/genRandomTupleTime.awk
elif [ $1 -eq 4 ]
then
  cat src/test/resources/base100k.dat | awk -f src/test/resources/genRandomSystemTime.awk
elif [ $1 -eq 5 ]
then
  cat src/test/resources/base100k.dat | awk -f src/test/resources/genRandomUnordered.awk
fi

