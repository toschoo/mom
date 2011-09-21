TDIR = test/src
SDIR = test/samples
CFGDIR = test/cfg
tests = $(TDIR)/tparser \
        $(TDIR)/tsender2 \
        $(TDIR)/ttypes \
        $(TDIR)/runTests \


GHC = ghc
FLGS = -Wall --make
INC  = -i./test/src -i./Network/Mom/Stompl

all:
	$(TDIR)/tparser  ALL $(SDIR)
	$(TDIR)/tsender2 $(CFGDIR)
	$(TDIR)/ttypes 

clean:
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/tsender2
	rm -f $(TDIR)/tparser
	rm -f $(TDIR)/ttypes
	rm -f $(TDIR)/runTests

build: $(tests)

$(TDIR)/tparser:	$(TDIR)/tparser.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tsender2:	$(TDIR)/tsender2.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/ttypes:		$(TDIR)/ttypes.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/runTests:	$(TDIR)/runTests.hs
			$(GHC) $(FLGS) $(INC) $@


