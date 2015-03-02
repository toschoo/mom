TDIR = test/suite
SDIR = ../test/samples
CFGDIR = ../test/cfg
SMOKEDIR = ./test/smoke
COMDIR = ../test/common

tests = $(TDIR)/tparser \
        $(TDIR)/tframe 

SUTDIR = Network/TLS/Bridge

CLIENT = Client

SUBSRC = $(SUTDIR)/Bridge.hs 

GHC = ghc
FLGS = -DTEST -Wall -O2 --make -rtsopts -prof -auto-all -caf-all
#-fforce-recomp
# -D_DEBUG
INC  = -i./test/src -i./Network/Mom/Stompl/Client \
		    -i../broker/Network/Mom/Stompl \
		    -i../stompl/Network/Mom/Stompl \
		    -i../test/common

smoke:	$(SMOKEDIR)/tlsbridge \
        $(SMOKEDIR)/bridgetls

$(SMOKEDIR)/tlsbridge:	$(SMOKEDIR)/tlsbridge.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/bridgetls:	$(SMOKEDIR)/bridgetls.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

suite:	$(TDIR)/tprotocol \
	$(TDIR)/tstate    \
	$(TDIR)/responder \
        $(TDIR)/tdialogs

run:	$(suite)
	$(TDIR)/responder &
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

prof-clean:
	mv -f ./*.prof  ./profiling/
	mv -f ./*.hp    ./profiling/
	mv -f ./*.ps    ./profiling/
	mv -f ./*.aux   ./profiling/

clean:
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/tdialogs
	rm -f $(TDIR)/tdialogsdbg
	rm -f $(TDIR)/responder
	rm -f $(TDIR)/tprotocol
	rm -f $(TDIR)/tprotocoldbg
	rm -f $(TDIR)/tstate
	rm -f $(SMOKEDIR)/tlssrv
	rm -f $(SMOKEDIR)/tlsbridge
	rm -f $(SMOKEDIR)/bridgetls
	rm -f $(SMOKEDIR)/*.hi
	rm -f $(SMOKEDIR)/*.o
	rm -f $(SUTDIR)/*.o
	rm -f $(SUTDIR)/*.hi

dist-clean:	clean
		cabal clean
		rm -f $(SUTDIR)/Client/*.hi
		rm -f $(SUTDIR)/Client/*.o
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
