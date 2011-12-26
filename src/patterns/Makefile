TDIR = test/suite
SDIR = ../test/samples
SMOKEDIR = ./test/smoke
COMDIR = ../test/common
OUTDIR = ./test/out

SUTDIR = src/Network/Mom

SUBSRC = $(SUTDIR)/Patterns.hs 

GHC = ghc
FLGS = -DTEST -Wall --make -threaded
INC  =	-i./src \
	-i./test/smoke \
	-i./test/suite \
	-i../test/common

smoke:	$(SMOKEDIR)/db \
	$(SMOKEDIR)/db2 \
	$(SMOKEDIR)/file \
	$(SMOKEDIR)/frost \
	$(SMOKEDIR)/rcv \
	$(SMOKEDIR)/tolist \
	$(SMOKEDIR)/tostr \
	$(SMOKEDIR)/append \
	$(SMOKEDIR)/store 

$(SMOKEDIR)/db:		$(SMOKEDIR)/db.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/db2:	$(SMOKEDIR)/db2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/file:	$(SMOKEDIR)/file.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/frost:	$(SMOKEDIR)/frost.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/rcv:	$(SMOKEDIR)/rcv.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tolist:	$(SMOKEDIR)/tolist.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tostr:	$(SMOKEDIR)/tostr.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/append:	$(SMOKEDIR)/append.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/store:	$(SMOKEDIR)/store.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@


clean:
	rm -f $(SUTDIR)/*.hi
	rm -f $(SUTDIR)/*.o
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(SMOKEDIR)/*.hi
	rm -f $(SMOKEDIR)/*.o
	rm -f $(SMOKEDIR)/db
	rm -f $(SMOKEDIR)/db2
	rm -f $(SMOKEDIR)/file
	rm -f $(SMOKEDIR)/frost
	rm -f $(SMOKEDIR)/rcv
	rm -f $(SMOKEDIR)/tolist
	rm -f $(SMOKEDIR)/tostr
	rm -f $(SMOKEDIR)/append
	rm -f $(SMOKEDIR)/store
	rm -f $(OUTDIR)/*.txt

dist-clean:	clean
		cabal clean
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
