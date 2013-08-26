TDIR = test/suite
SDIR = ../test/samples
SMOKEDIR = ./test/smoke
COMDIR = ../test/common
HELPER = examples/common
OUTDIR = ./test/out

SUTDIR = src/Network/Mom
SRCDIR = src

SUBSRC = $(SUTDIR)/Patterns/Streams.hs \
 	 $(SUTDIR)/Patterns/Types.hs \
	 $(SUTDIR)/Patterns/Basic.hs \
 	 $(SUTDIR)/Patterns/Basic/Client.hs \
 	 $(SUTDIR)/Patterns/Basic/Server.hs \
	 $(SUTDIR)/Patterns/Basic/Publisher.hs \
	 $(SUTDIR)/Patterns/Basic/Subscriber.hs \
	 $(SUTDIR)/Patterns/Basic/Pusher.hs \
	 $(SUTDIR)/Patterns/Basic/Puller.hs \
	 $(SUTDIR)/Patterns/Broker/Common.hs \
	 $(SUTDIR)/Patterns/Broker.hs \
	 $(SUTDIR)/Patterns/Broker/Broker.hs \
	 $(SUTDIR)/Patterns/Broker/Client.hs \
	 $(SUTDIR)/Patterns/Broker/Server.hs \
	 $(SUTDIR)/Patterns/Broker/Bridge.hs \
	 $(SRCDIR)/broker/Registry.hs \
	 $(SRCDIR)/broker/Heartbeat.hs

GHC = ghc
FLGS = -DTEST -Wall --make -threaded
INC  =	-i./src \
	-i./src/broker \
	-i./test/smoke \
	-i./test/suite \
	-i./test/common \
	-i../test/common \
	-i./examples/common

suite:	$(TDIR)/streams \
	$(TDIR)/basic \
	$(TDIR)/broker/registry \
	$(TDIR)/broker/broker

run:	
	$(TDIR)/streams
	$(TDIR)/basic
	$(TDIR)/broker/registry 
	$(TDIR)/broker/broker

$(TDIR)/basic:	$(TDIR)/basic.hs $(TDIR)/Common.hs $(SUBSRC)
		$(GHC) $(FLGS) $(INC) $@

$(TDIR)/streams:	$(TDIR)/streams.hs $(TDIR)/Common.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/broker/registry:	$(TDIR)/broker/registry.hs $(TDIR)/Common.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(TDIR)/broker/broker:	$(TDIR)/broker/broker.hs $(TDIR)/Common.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

smoke:	$(SMOKEDIR)/db \
	$(SMOKEDIR)/db2 \
	$(SMOKEDIR)/db3 \
	$(SMOKEDIR)/file \
	$(SMOKEDIR)/file2 \
	$(SMOKEDIR)/frost \
	$(SMOKEDIR)/hello \
	$(SMOKEDIR)/err \
	$(SMOKEDIR)/wuserver \
	$(SMOKEDIR)/wuserver2 \
	$(SMOKEDIR)/wuserver3 \
	$(SMOKEDIR)/wuclient \
	$(SMOKEDIR)/wufile \
	$(SMOKEDIR)/wuclient2 \
	$(SMOKEDIR)/worker \
	$(SMOKEDIR)/rcv \
	$(SMOKEDIR)/async \
	$(SMOKEDIR)/push \
	$(SMOKEDIR)/tolist \
	$(SMOKEDIR)/tostr \
	$(SMOKEDIR)/append \
	$(SMOKEDIR)/store \
	$(SMOKEDIR)/ping \
	$(SMOKEDIR)/forward1 \
	$(SMOKEDIR)/pipe1 \
	$(SMOKEDIR)/queue1 \
	$(SMOKEDIR)/queue2 \
	$(SMOKEDIR)/stream \
	$(SMOKEDIR)/client1 \
	$(SMOKEDIR)/client2 \
	$(SMOKEDIR)/mdclient1 \
	$(SMOKEDIR)/olleh \
	$(SMOKEDIR)/bounce \
	$(SMOKEDIR)/mdbounce \
	$(SMOKEDIR)/mdbroker \
	$(SMOKEDIR)/broker \
	$(SMOKEDIR)/bridge 

$(SMOKEDIR)/db:		$(SMOKEDIR)/db.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/db2:	$(SMOKEDIR)/db2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/db3:	$(SMOKEDIR)/db3.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/file:	$(SMOKEDIR)/file.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/file2:	$(SMOKEDIR)/file2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/frost:	$(SMOKEDIR)/frost.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/hello:	$(SMOKEDIR)/hello.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/err:	$(SMOKEDIR)/err.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuserver:	$(SMOKEDIR)/wuserver.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuserver2:	$(SMOKEDIR)/wuserver2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuserver3:	$(SMOKEDIR)/wuserver3.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuclient:	$(SMOKEDIR)/wuclient.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wufile:	$(SMOKEDIR)/wufile.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuclient2:	$(SMOKEDIR)/wuclient2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/worker:	$(SMOKEDIR)/worker.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/rcv:	$(SMOKEDIR)/rcv.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/async:	$(SMOKEDIR)/async.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/push:	$(SMOKEDIR)/push.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tolist:	$(SMOKEDIR)/tolist.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tostr:	$(SMOKEDIR)/tostr.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/append:	$(SMOKEDIR)/append.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/store:	$(SMOKEDIR)/store.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/ping:	$(SMOKEDIR)/ping.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/forward1:	$(SMOKEDIR)/forward1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/pipe1:	$(SMOKEDIR)/pipe1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/queue1:	$(SMOKEDIR)/queue1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/queue2:	$(SMOKEDIR)/queue2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/stream:	$(SMOKEDIR)/stream.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/client1:	$(SMOKEDIR)/client1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/client2:	$(SMOKEDIR)/client2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/mdclient1:	$(SMOKEDIR)/mdclient1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/olleh:	$(SMOKEDIR)/olleh.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/bounce:	$(SMOKEDIR)/bounce.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/mdbounce:	$(SMOKEDIR)/mdbounce.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/pubsub:	$(SMOKEDIR)/pubsub.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/demo1:	$(SMOKEDIR)/demo1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/pipe:	$(SMOKEDIR)/pipe.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/broker:	$(SMOKEDIR)/broker.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/mdbroker:	$(SMOKEDIR)/mdbroker.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/bridge:	$(SMOKEDIR)/bridge.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@


clean:
	rm -f $(SUTDIR)/*.hi
	rm -f $(SUTDIR)/*.o
	rm -f $(SUTDIR)/Patterns/*.hi
	rm -f $(SUTDIR)/Patterns/*.o
	rm -f $(SUTDIR)/Patterns/Streams/*.hi
	rm -f $(SUTDIR)/Patterns/Streams/*.o
	rm -f $(SUTDIR)/Patterns/Basic/*.hi
	rm -f $(SUTDIR)/Patterns/Basic/*.o
	rm -f $(SUTDIR)/Patterns/Broker/*.hi
	rm -f $(SUTDIR)/Patterns/Broker/*.o
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/streams
	rm -f $(TDIR)/basic
	rm -f $(TDIR)/broker/*.hi
	rm -f $(TDIR)/broker/*.o
	rm -f $(TDIR)/broker/registry
	rm -f $(TDIR)/broker/broker
	rm -f $(SRCDIR)/*.hi
	rm -f $(SRCDIR)/*.o
	rm -f $(SRCDIR)/broker/*.hi
	rm -f $(SRCDIR)/broker/*.o
	rm -f $(SMOKEDIR)/*.hi
	rm -f $(SMOKEDIR)/*.o
	rm -f $(HELPER)/*.hi
	rm -f $(HELPER)/*.o
	rm -f $(TDIR)/enum
	rm -f $(TDIR)/device
	rm -f $(TDIR)/basic
	rm -f $(SMOKEDIR)/db
	rm -f $(SMOKEDIR)/db2
	rm -f $(SMOKEDIR)/db3
	rm -f $(SMOKEDIR)/file
	rm -f $(SMOKEDIR)/file2
	rm -f $(SMOKEDIR)/frost
	rm -f $(SMOKEDIR)/hello
	rm -f $(SMOKEDIR)/err
	rm -f $(SMOKEDIR)/wuserver
	rm -f $(SMOKEDIR)/wuserver2
	rm -f $(SMOKEDIR)/wuserver3
	rm -f $(SMOKEDIR)/wuclient
	rm -f $(SMOKEDIR)/wufile
	rm -f $(SMOKEDIR)/wuclient2
	rm -f $(SMOKEDIR)/worker
	rm -f $(SMOKEDIR)/rcv
	rm -f $(SMOKEDIR)/async
	rm -f $(SMOKEDIR)/push
	rm -f $(SMOKEDIR)/tolist
	rm -f $(SMOKEDIR)/tostr
	rm -f $(SMOKEDIR)/append
	rm -f $(SMOKEDIR)/store
	rm -f $(SMOKEDIR)/ping
	rm -f $(SMOKEDIR)/forward1
	rm -f $(SMOKEDIR)/pipe1
	rm -f $(SMOKEDIR)/queue1
	rm -f $(SMOKEDIR)/queue2
	rm -f $(SMOKEDIR)/stream
	rm -f $(SMOKEDIR)/client1
	rm -f $(SMOKEDIR)/client2
	rm -f $(SMOKEDIR)/mdclient1
	rm -f $(SMOKEDIR)/olleh
	rm -f $(SMOKEDIR)/bounce
	rm -f $(SMOKEDIR)/conduit
	rm -f $(SMOKEDIR)/broker
	rm -f $(SMOKEDIR)/mdbroker
	rm -f $(SMOKEDIR)/mdbounce
	rm -f $(SMOKEDIR)/bridge
	rm -f $(SMOKEDIR)/pipe
	rm -f $(SMOKEDIR)/pubsub
	rm -f $(SMOKEDIR)/demo1
	rm -f $(SMOKEDIR)/zmqpubsub
	rm -f $(SMOKEDIR)/eot
	rm -f $(SMOKEDIR)/zmqrouter
	rm -f $(OUTDIR)/*.txt

dist-clean:	clean
		cabal clean
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
