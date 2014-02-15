if [ -d log ]
then 
  if [ -f log/stompserver.pid ]
  then
    pid=$(cat log/stompserver.pid)
    kill -INT $pid
    sleep 1
  else
    echo "No pid file. Sure stompserver is running?"
  fi
else
  echo "No log directory. Sure stompserver ever ran?"
fi
