TDIR = test/suite
SDIR = ../test/samples
CFGDIR = ../test/cfg
SMOKEDIR = ./test/smoke
COMDIR = ../test/common

tests = $(TDIR)/tparser \
        $(TDIR)/tframe 

SUTDIR = Network/Mom/Stompl/Client

CLIENT = Client

SUBSRC = $(SUTDIR)/Socket.hs \
         $(SUTDIR)/Protocol.hs \
         $(SUTDIR)/Factory.hs \
         $(SUTDIR)/Queue.hs 

GHC = ghc
FLGS = -DTEST -Wall --make
INC  = -i./test/src -i./Network/Mom/Stompl/Client \
		    -i../broker/Network/Mom/Stompl \
		    -i../stompl/Network/Mom/Stompl \
		    -i../test/common

smoke:	$(SMOKEDIR)/listen2 $(SMOKEDIR)/send2 $(SMOKEDIR)/sendtx

$(SMOKEDIR)/listen2:	$(SMOKEDIR)/listen2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/send2:	$(SMOKEDIR)/send2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendtx:	$(SMOKEDIR)/sendtx.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

suite:	$(TDIR)/tprotocol

$(TDIR)/tprotocol:	$(TDIR)/tprotocol.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tdialogs:	$(TDIR)/tdialogs.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tmime:		$(TDIR)/tmime.hs
			$(GHC) $(FLGS) $(INC) $@

all:
	$(TDIR)/tframe      $(SDIR)
	$(TDIR)/tparser ALL $(SDIR)

clean:
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/tframe
	rm -f $(TDIR)/tparser
	rm -f $(SMOKEDIR)/listen2
	rm -f $(SMOKEDIR)/send2
	rm -f $(SMOKEDIR)/sendtx
	rm -f $(SMOKEDIR)/*.hi
	rm -f $(SMOKEDIR)/*.o

dist-clean:	clean
		cabal clean
		rm -f $(SUTDIR)/*.hi
		rm -f $(SUTDIR)/*.o
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
