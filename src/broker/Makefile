TDIR = test/suite
SDIR = ../test/samples
CFGDIR = ../test/cfg
SMOKEDIR = ./test/smoke
COMDIR = ../test/common

tests = $(TDIR)/tparser \
        $(TDIR)/tframe 

SUTDIR = src

CLIENT = Client

SUBSRC = $(SUTDIR)/Socket.hs \
         $(SUTDIR)/Protocol.hs \
         $(SUTDIR)/Factory.hs \
         $(SUTDIR)/State.hs \
         $(SUTDIR)/Queue.hs 

GHC = ghc
FLGS = -DTEST -Wall --make
INC  = -i./test/src -i./Network/Mom/Stompl/Client \
		    -i../broker/Network/Mom/Stompl \
		    -i../stompl/Network/Mom/Stompl \
		    -i../test/common

smoke:	$(SMOKEDIR)/listen2 \
        $(SMOKEDIR)/send2 \
        $(SMOKEDIR)/sendtx \
        $(SMOKEDIR)/sendf \
        $(SMOKEDIR)/sendforever \
        $(SMOKEDIR)/sendjp \
        $(SMOKEDIR)/frost \
        $(SMOKEDIR)/ping 

$(SMOKEDIR)/listen2:	$(SMOKEDIR)/listen2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/send2:	$(SMOKEDIR)/send2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/frost:	$(SMOKEDIR)/frost.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendtx:	$(SMOKEDIR)/sendtx.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendf:	$(SMOKEDIR)/sendf.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendjp:	$(SMOKEDIR)/sendjp.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendforever:	$(SMOKEDIR)/sendforever.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/ping:	$(SMOKEDIR)/ping.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

suite:	$(TDIR)/tprotocol \
	$(TDIR)/tstate    \
	$(TDIR)/responder \
        $(TDIR)/tdialogs

run:	$(suite)
	$(TDIR)/tprotocol
	$(TDIR)/tstate
	$(TDIR)/tdialogs 61613

$(TDIR)/responder:	$(TDIR)/responder.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tprotocol:	$(TDIR)/tprotocol.hs $(TDIR)/responder $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tprotocoldbg:	$(TDIR)/tprotocol.hs $(SUBSRC)
			$(GHC) $(FLGS) -D_DEBUG -o $@ $(INC) $(TDIR)/tprotocol.hs

$(TDIR)/tdialogs:	$(TDIR)/tdialogs.hs $(TDIR)/responder $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tdialogsdbg:	$(TDIR)/tdialogs.hs $(SUBSRC)
			$(GHC) -o $@ $(FLGS) -D_DEBUG $(INC) $(TDIR)/tdialogs.hs

$(TDIR)/tstate:		$(TDIR)/tstate.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

clean:
	rm -f $(SUTDIR)/*.hi
	rm -f $(SUTDIR)/*.o
	rm -f $(SUTDIR)/lambda

dist-clean:	clean
		cabal clean
		rm -f $(SUTDIR)/*.hi
		rm -f $(SUTDIR)/*.o
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
