TDIR = test/src
SDIR = test/samples
CFGDIR = test/cfg

tests = $(TDIR)/tparser \
        $(TDIR)/tsender2 \
        $(TDIR)/tframe \
        $(TDIR)/ttypes \
        $(TDIR)/runTests 

SUTDIR = Network/Mom/Stompl

SUBSRC = $(SUTDIR)/Frame.hs \
         $(SUTDIR)/Parser.hs \
         $(SUTDIR)/Types.hs \
         $(SUTDIR)/Server.hs \
         $(SUTDIR)/Sender.hs \
         $(SUTDIR)/Session.hs 


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

build: $(tests) $(SUBSRC)


$(TDIR)/tframe:		$(TDIR)/tframe.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tparser:	$(TDIR)/tparser.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tsender2:	$(TDIR)/tsender2.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/ttypes:		$(TDIR)/ttypes.hs
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/runTests:	$(TDIR)/runTests.hs
			$(GHC) $(FLGS) $(INC) $@


