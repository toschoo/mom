COMDIR = common
BASICDIR = basic
OUTDIR = ./out

SUTDIR = ../src/Network/Mom
SRCDIR = ../src

SUBSRC = $(SUTDIR)/Patterns.hs \
	 $(SUTDIR)/Patterns/Basic.hs \
	 $(SUTDIR)/Patterns/Device.hs \
	 $(SUTDIR)/Patterns/Enumerator.hs \
	 $(SRCDIR)/Factory.hs \
	 $(SRCDIR)/Types.hs \
	 $(SRCDIR)/Service.hs \
         $(COMDIR)/Helper.hs

GHC = ghc
FLGS = -DTEST -Wall --make -threaded
INC  =	-i../src \
	-i./common \
	-i./basic/clients \
	-i./basic/servers \
	-i./basic/pubsub \
	-i./basic/pipes \
	-i./basic/pairs

$(BASICDIR)/servers/db:		$(BASICDIR)/servers/db.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/servers/file:	$(BASICDIR)/servers/file.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/servers/frost:	$(BASICDIR)/servers/frost.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/servers/olleh:	$(BASICDIR)/servers/olleh.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/clients/rcv:	$(BASICDIR)/clients/rcv.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/clients/store:	$(BASICDIR)/clients/store.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/clients/async:	$(BASICDIR)/clients/async.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/clients/tolist:	$(BASICDIR)/clients/tolist.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/clients/tostr:	$(BASICDIR)/clients/tostr.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/clients/append:	$(BASICDIR)/clients/append.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/pipes/push:		$(BASICDIR)/pipes/push.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/pipes/worker:	$(BASICDIR)/pipes/worker.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(BASICDIR)/pairs/ping:		$(BASICDIR)/pairs/ping.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

 
servers:	$(BASICDIR)/servers/db \
		$(BASICDIR)/servers/file \
		$(BASICDIR)/servers/frost \
		$(BASICDIR)/servers/olleh 

clients:	$(BASICDIR)/clients/append \
		$(BASICDIR)/clients/async \
		$(BASICDIR)/clients/rcv \
		$(BASICDIR)/clients/store \
		$(BASICDIR)/clients/tolist \
		$(BASICDIR)/clients/tostr 

pipes:		$(BASICDIR)/pipes/push \
		$(BASICDIR)/pipes/worker

pairs:		$(BASICDIR)/pairs/ping

examples:	servers clients pipes pairs

.SUFFIXES: .hs 

.hs:
	$(GHC) $(FLGS) $(INC) $* 

clean:
	rm -f $(BASICDIR)/*/*.hi
	rm -f $(BASICDIR)/*/*.o
	rm -f $(COMDIR)/*.hi
	rm -f $(COMDIR)/*.o
	rm -f $(BASICDIR)/servers/db 
	rm -f $(BASICDIR)/servers/file 
	rm -f $(BASICDIR)/servers/frost
	rm -f $(BASICDIR)/servers/olleh 
	rm -f $(BASICDIR)/clients/append 
	rm -f $(BASICDIR)/clients/async 
	rm -f $(BASICDIR)/clients/rcv 
	rm -f $(BASICDIR)/clients/store 
	rm -f $(BASICDIR)/clients/tolist
	rm -f $(BASICDIR)/clients/tostr 
	rm -f $(BASICDIR)/pipes/push
	rm -f $(BASICDIR)/pipes/worker
	rm -f $(BASICDIR)/pairs/ping
	rm -f $(OUTDIR)/*


