TDIR = test/src
SDIR = test/samples
CFGDIR = test/cfg

tests = $(TDIR)/tparser \
        $(TDIR)/tbook   \
        $(TDIR)/tsender2 \
        $(TDIR)/tframe \
        $(TDIR)/ttypes \
        $(TDIR)/runTests 

SUTDIR = Network/Mom/Stompl

CLIENT = Client

SUBSRC = $(SUTDIR)/Frame.hs \
         $(SUTDIR)/Parser.hs \
         $(SUTDIR)/Types.hs \
         $(SUTDIR)/Server.hs \
         $(SUTDIR)/Book.hs \
         $(SUTDIR)/Sender.hs \
         $(SUTDIR)/Session.hs 

GHC = ghc
FLGS = -DTEST -Wall --make
INC  = -i./test/src -i./Network/Mom/Stompl

all:
	$(TDIR)/tparser  ALL $(SDIR)
	$(TDIR)/tbook 
	$(TDIR)/tsender2 $(CFGDIR)
	$(TDIR)/ttypes 

clean:
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(TDIR)/tframe
	rm -f $(TDIR)/tbook 
	rm -f $(TDIR)/tsender2
	rm -f $(TDIR)/tparser
	rm -f $(TDIR)/ttypes
	rm -f $(TDIR)/runTests

dist-clean:	clean
		rm -f $(SUTDIR)/*.hi
		rm -f $(SUTDIR)/*.o
		rm -f $(SUTDIR)/stompl
		rm -f $(SUTDIR)/$(CLIENT)/*.hi
		rm -f $(SUTDIR)/$(CLIENT)/*.o

build: $(tests) $(SUBSRC)


$(TDIR)/tframe:		$(TDIR)/tframe.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tparser:	$(TDIR)/tparser.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tbook:		$(TDIR)/tbook.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/tsender2:	$(TDIR)/tsender2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/ttypes:		$(TDIR)/ttypes.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(TDIR)/runTests:	$(TDIR)/runTests.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@


