#!/bin/bash

cabal configure
cabal haddock

pack=$(cabal info . | head -n1 | awk '{print $2}')
docs=${pack}-docs

x=$(echo $pack | rev)
l=$(expr length $x)
i=$(expr index $x "-")
y=$(expr substr $x $(($i+1)) $(($l-$i)))
short=$(echo $y | rev)

echo $pack
echo $docs
echo $short

if [ -z "$pack" -o -z "$docs" -o -z "$short" ]
then
  echo "Error!!!"
  exit 1
fi

if [ -d $docs ]
then
  rm -rf $docs
fi
mkdir $docs

cp dist/doc/html/$short/Network-Mom-Stompl-Patterns-Basic.html $docs
cp dist/doc/html/$short/Network-Mom-Stompl-Patterns-Balancer.html $docs
cp dist/doc/html/$short/Network-Mom-Stompl-Patterns-Desk.html $docs
cp dist/doc/html/$short/Network-Mom-Stompl-Patterns-Bridge.html $docs
cp dist/doc/html/$short/ocean.css $docs

tar --format=ustar -caf $docs.tar.gz $docs

curl -X PUT \
  -H "Content-Type: application/x-tar"  \
  -H "Content-Encoding: gzip"  \
  http://TobiasSchoofs:doris172@hackage.haskell.org/package/$pack/docs  \
  --data-binary @$docs.tar.gz
