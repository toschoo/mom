TDIR = test/src
SDIR = test/samples
CFGDIR = test/cfg

tests = $(TDIR)/tbook   \
        $(TDIR)/tsender2 \
        $(TDIR)/ttypes \
        $(TDIR)/runTests 

SUTDIR = Network/Mom/Stompl

SUBSRC = $(SUTDIR)/Types.hs \
         $(SUTDIR)/Server.hs \
         $(SUTDIR)/Book.hs \
         $(SUTDIR)/Sender.hs \
         $(SUTDIR)/Session.hs 

GHC = ghc
FLGS = -DTEST -Wall --make
INC  = -i./test/src -i./Network/Mom/Stompl

all:
	$(TDIR)/tsender2 $(CFGDIR)
	$(TDIR)/ttypes 

clean:
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/tbook 
	rm -f $(TDIR)/tsender2
	rm -f $(TDIR)/ttypes
	rm -f $(TDIR)/runTests

dist-clean:	clean
		cabal clean
		rm -f $(SUTDIR)/*.hi
		rm -f $(SUTDIR)/*.o
		rm -f $(SUTDIR)/stompl
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)


$(TDIR)/tbook:		$(TDIR)/tbook.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tsender2:	$(TDIR)/tsender2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/ttypes:		$(TDIR)/ttypes.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/runTests:	$(TDIR)/runTests.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@


