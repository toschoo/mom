TDIR = test/suite
SDIR = ../test/samples
SMOKEDIR = ./test/smoke
COMDIR = ../test/common
HELPER = examples/common
OUTDIR = ./test/out

SUTDIR = src/Network/Mom
SRCDIR = src

SUBSRC = $(SUTDIR)/Patterns.hs \
	 $(SUTDIR)/Patterns/Basic.hs \
	 $(SUTDIR)/Patterns/Device.hs \
	 $(SUTDIR)/Patterns/Enumerator.hs \
	 $(SRCDIR)/Factory.hs \
	 $(SRCDIR)/Types.hs \
	 $(SRCDIR)/Service.hs \
         $(HELPER)/Helper.hs

GHC = ghc
FLGS = -DTEST -Wall --make -threaded
INC  =	-i./src \
	-i./test/smoke \
	-i./test/suite \
	-i./test/common \
	-i../test/common \
	-i./examples/common

smoke:	$(SMOKEDIR)/db \
	$(SMOKEDIR)/db2 \
	$(SMOKEDIR)/db3 \
	$(SMOKEDIR)/file \
	$(SMOKEDIR)/file2 \
	$(SMOKEDIR)/frost \
	$(SMOKEDIR)/hello \
	$(SMOKEDIR)/err \
	$(SMOKEDIR)/wuserver \
	$(SMOKEDIR)/wuserver2 \
	$(SMOKEDIR)/wuserver3 \
	$(SMOKEDIR)/wuclient \
	$(SMOKEDIR)/wufile \
	$(SMOKEDIR)/wuclient2 \
	$(SMOKEDIR)/worker \
	$(SMOKEDIR)/rcv \
	$(SMOKEDIR)/async \
	$(SMOKEDIR)/push \
	$(SMOKEDIR)/tolist \
	$(SMOKEDIR)/tostr \
	$(SMOKEDIR)/append \
	$(SMOKEDIR)/store \
	$(SMOKEDIR)/ping \
	$(SMOKEDIR)/forward1 \
	$(SMOKEDIR)/pipe1 \
	$(SMOKEDIR)/queue1 \
	$(SMOKEDIR)/queue2 \
	$(SMOKEDIR)/cmd

$(SMOKEDIR)/db:		$(SMOKEDIR)/db.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/db2:	$(SMOKEDIR)/db2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/db3:	$(SMOKEDIR)/db3.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/file:	$(SMOKEDIR)/file.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/file2:	$(SMOKEDIR)/file2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/frost:	$(SMOKEDIR)/frost.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/hello:	$(SMOKEDIR)/hello.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/err:	$(SMOKEDIR)/err.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuserver:	$(SMOKEDIR)/wuserver.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuserver2:	$(SMOKEDIR)/wuserver2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuserver3:	$(SMOKEDIR)/wuserver3.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuclient:	$(SMOKEDIR)/wuclient.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wufile:	$(SMOKEDIR)/wufile.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/wuclient2:	$(SMOKEDIR)/wuclient2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/worker:	$(SMOKEDIR)/worker.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/rcv:	$(SMOKEDIR)/rcv.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/async:	$(SMOKEDIR)/async.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/push:	$(SMOKEDIR)/push.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tolist:	$(SMOKEDIR)/tolist.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tostr:	$(SMOKEDIR)/tostr.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/append:	$(SMOKEDIR)/append.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/store:	$(SMOKEDIR)/store.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/ping:	$(SMOKEDIR)/ping.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/forward1:	$(SMOKEDIR)/forward1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/pipe1:	$(SMOKEDIR)/pipe1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/queue1:	$(SMOKEDIR)/queue1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/queue2:	$(SMOKEDIR)/queue2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/cmd:	$(SMOKEDIR)/cmd.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@


clean:
	rm -f $(SUTDIR)/*.hi
	rm -f $(SUTDIR)/*.o
	rm -f $(SUTDIR)/Patterns/*.hi
	rm -f $(SUTDIR)/Patterns/*.o
	rm -f $(TDIR)/*.hi
	rm -f $(TDIR)/*.o
	rm -f $(SRCDIR)/*.hi
	rm -f $(SRCDIR)/*.o
	rm -f $(SMOKEDIR)/*.hi
	rm -f $(SMOKEDIR)/*.o
	rm -f $(HELPER)/*.hi
	rm -f $(HELPER)/*.o
	rm -f $(SMOKEDIR)/db
	rm -f $(SMOKEDIR)/db2
	rm -f $(SMOKEDIR)/db3
	rm -f $(SMOKEDIR)/file
	rm -f $(SMOKEDIR)/file2
	rm -f $(SMOKEDIR)/frost
	rm -f $(SMOKEDIR)/hello
	rm -f $(SMOKEDIR)/err
	rm -f $(SMOKEDIR)/wuserver
	rm -f $(SMOKEDIR)/wuserver2
	rm -f $(SMOKEDIR)/wuserver3
	rm -f $(SMOKEDIR)/wuclient
	rm -f $(SMOKEDIR)/wufile
	rm -f $(SMOKEDIR)/wuclient2
	rm -f $(SMOKEDIR)/worker
	rm -f $(SMOKEDIR)/rcv
	rm -f $(SMOKEDIR)/async
	rm -f $(SMOKEDIR)/push
	rm -f $(SMOKEDIR)/tolist
	rm -f $(SMOKEDIR)/tostr
	rm -f $(SMOKEDIR)/append
	rm -f $(SMOKEDIR)/store
	rm -f $(SMOKEDIR)/ping
	rm -f $(SMOKEDIR)/forward1
	rm -f $(SMOKEDIR)/pipe1
	rm -f $(SMOKEDIR)/queue1
	rm -f $(SMOKEDIR)/queue2
	rm -f $(SMOKEDIR)/cmd
	rm -f $(OUTDIR)/*.txt

dist-clean:	clean
		cabal clean
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
