cabal configure
cabal haddock

if [ -d stompl-queue-0.0.7-docs ]
then
  rm -rf stompl-queue-0.0.7-docs
fi
mkdir stompl-queue-0.0.7-docs

cp dist/doc/html/stomp-queue/Network-Mom-Stompl-Client-Exception.html stompl-queue-0.0.7-docs/ 
cp dist/doc/html/stomp-queue/Network-Mom-Stompl-Client-Queue.html stompl-queue-0.0.7-docs/ 
cp dist/doc/html/stomp-queue/ocean.css stompl-queue-0.0.7-docs

tar --format=ustar -caf stomp-queue-0.0.7-docs.tar.gz stomp-queue-0.0.7-docs

curl -X PUT \
  -H "Content-Type: application/x-tar"  \
  -H "Content-Encoding: gzip"  \
  http://TobiasSchoofs:doris172@hackage.haskell.org/package/stomp-queue-0.0.7/docs  \
  --data-binary @stomp-queue-0.0.7-docs.tar.gz 
