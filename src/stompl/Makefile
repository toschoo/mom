TDIR = test/suite
SDIR = ../test/samples
CFGDIR = ../test/cfg

tests = $(TDIR)/tparser 

SUTDIR = Network/Mom/Stompl

CLIENT = Client

SUBSRC = $(SUTDIR)/Frame.hs \
         $(SUTDIR)/Parser.hs 

GHC = ghc
FLGS = -DTEST -Wall --make
INC  = -i./test/src -i./Network/Mom/Stompl -i../broker/Network/Mom/Stompl

run:
	$(TDIR)/tparser ALL $(SDIR)

clean:
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/tframe
	rm -f $(TDIR)/tparser

dist-clean:	clean
		cabal clean
		rm -f $(SUTDIR)/*.hi
		rm -f $(SUTDIR)/*.o
		rm -f test/bin/*.o
		rm -f test/bin/*.hi
		rm -f test/client/*.o
		rm -f test/client/*.hi

suite: $(tests) $(SUBSRC)


$(TDIR)/tframe:		$(TDIR)/tframe.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tparser:	$(TDIR)/tparser.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@


