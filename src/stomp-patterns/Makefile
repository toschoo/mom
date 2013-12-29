TDIR = test/suite
SDIR = ../test/samples
CFGDIR = ../test/cfg
SMOKEDIR = ./test/smoke
COMDIR = ../test/common

SUTDIR = Network/Mom/Stompl

CLIENT = Client

SUBSRC = $(SUTDIR)/Patterns/Basic.hs \
         $(SUTDIR)/Patterns/Types.hs \
         $(SUTDIR)/Patterns/Registry.hs \
	 $(SUTDIR)/Patterns/Desk.hs \
	 $(SUTDIR)/Patterns/Balancer.hs \
	 $(SUTDIR)/Patterns/Bridge.hs 

GHC = ghc
FLGS = -DTEST -Wall -O2 --make -rtsopts -prof -auto-all -caf-all 
#-fforce-recomp
INC  = -i./test/src -i../client/Network/Mom/Stompl/Client \
		    -i../client \
		    -i./Network/Mom/Stompl/Patterns \
		    -i./test/suite

smoke:	$(SMOKEDIR)/listen2 \
        $(SMOKEDIR)/send2 \
        $(SMOKEDIR)/client1 \
        $(SMOKEDIR)/push1 \
        $(SMOKEDIR)/sub1 \
        $(SMOKEDIR)/sub2 \
        $(SMOKEDIR)/sub3 \
        $(SMOKEDIR)/server1 \
        $(SMOKEDIR)/server2 \
        $(SMOKEDIR)/server3 \
        $(SMOKEDIR)/server4 \
        $(SMOKEDIR)/worker1 \
        $(SMOKEDIR)/worker2 \
        $(SMOKEDIR)/pub1 \
        $(SMOKEDIR)/pub2 \
        $(SMOKEDIR)/desk1 \
        $(SMOKEDIR)/bal1 \
        $(SMOKEDIR)/router1 \
        $(SMOKEDIR)/fwd1 \
        $(SMOKEDIR)/srb1 \
        $(SMOKEDIR)/tsb1 \
        $(SMOKEDIR)/deskcl1 \
        $(SMOKEDIR)/sendtx \
        $(SMOKEDIR)/sendtx2 \
        $(SMOKEDIR)/sendf \
        $(SMOKEDIR)/sendforever \
        $(SMOKEDIR)/sendjp \
        $(SMOKEDIR)/frost \
        $(SMOKEDIR)/luke \
        $(SMOKEDIR)/psend \
        $(SMOKEDIR)/ping \
        $(SMOKEDIR)/multi1 \
        $(SMOKEDIR)/demo1

$(SMOKEDIR)/listen2:	$(SMOKEDIR)/listen2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/send2:	$(SMOKEDIR)/send2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/client1:	$(SMOKEDIR)/client1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/push1:	$(SMOKEDIR)/push1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sub1:	$(SMOKEDIR)/sub1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sub2:	$(SMOKEDIR)/sub2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sub3:	$(SMOKEDIR)/sub3.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/server1:	$(SMOKEDIR)/server1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/server2:	$(SMOKEDIR)/server2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/server3:	$(SMOKEDIR)/server3.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/server4:	$(SMOKEDIR)/server4.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/worker1:	$(SMOKEDIR)/worker1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/worker2:	$(SMOKEDIR)/worker2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/pub1:	$(SMOKEDIR)/pub1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/pub2:	$(SMOKEDIR)/pub2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/desk1:	$(SMOKEDIR)/desk1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/bal1:	$(SMOKEDIR)/bal1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/router1:	$(SMOKEDIR)/router1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/fwd1:	$(SMOKEDIR)/fwd1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/srb1:	$(SMOKEDIR)/srb1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/tsb1:	$(SMOKEDIR)/tsb1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/deskcl1:	$(SMOKEDIR)/deskcl1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/frost:	$(SMOKEDIR)/frost.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/luke:	$(SMOKEDIR)/luke.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/psend:	$(SMOKEDIR)/psend.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendtx:	$(SMOKEDIR)/sendtx.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendtx2:	$(SMOKEDIR)/sendtx2.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendf:	$(SMOKEDIR)/sendf.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendjp:	$(SMOKEDIR)/sendjp.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/sendforever:	$(SMOKEDIR)/sendforever.hs $(SUBSRC)
				$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/ping:	$(SMOKEDIR)/ping.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/multi1:	$(SMOKEDIR)/multi1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/demo1:	$(SMOKEDIR)/demo1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/conduit1:	$(SMOKEDIR)/conduit1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/cl1:	$(SMOKEDIR)/cl1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

$(SMOKEDIR)/contx1:	$(SMOKEDIR)/contx1.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

suite:	$(TDIR)/registry

$(TDIR)/registry:	$(TDIR)/Common.hs $(TDIR)/registry.hs $(SUBSRC)
			$(GHC) $(FLGS) $(INC) $@

run:	suite
	$(TDIR)/registry 

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
	rm -f $(SMOKEDIR)/listen2
	rm -f $(SMOKEDIR)/send2
	rm -f $(SMOKEDIR)/sendtx
	rm -f $(SMOKEDIR)/sendtx2
	rm -f $(SMOKEDIR)/sendf
	rm -f $(SMOKEDIR)/sendjp
	rm -f $(SMOKEDIR)/sendforever
	rm -f $(SMOKEDIR)/ping
	rm -f $(SMOKEDIR)/multi1
	rm -f $(SMOKEDIR)/demo1
	rm -f $(SMOKEDIR)/frost
	rm -f $(SMOKEDIR)/luke
	rm -f $(SMOKEDIR)/psend
	rm -f $(SMOKEDIR)/cl1
	rm -f $(SMOKEDIR)/conduit1
	rm -f $(SMOKEDIR)/client1
	rm -f $(SMOKEDIR)/push1
	rm -f $(SMOKEDIR)/sub1
	rm -f $(SMOKEDIR)/sub2
	rm -f $(SMOKEDIR)/sub3
	rm -f $(SMOKEDIR)/server1
	rm -f $(SMOKEDIR)/server2
	rm -f $(SMOKEDIR)/server3
	rm -f $(SMOKEDIR)/server4
	rm -f $(SMOKEDIR)/worker1
	rm -f $(SMOKEDIR)/worker2
	rm -f $(SMOKEDIR)/pub1
	rm -f $(SMOKEDIR)/pub2
	rm -f $(SMOKEDIR)/desk1
	rm -f $(SMOKEDIR)/bal1 
	rm -f $(SMOKEDIR)/router1
	rm -f $(SMOKEDIR)/fwd1 
	rm -f $(SMOKEDIR)/srb1 
	rm -f $(SMOKEDIR)/tsb1 
	rm -f $(SMOKEDIR)/deskcl1
	rm -f $(SMOKEDIR)/contx1
	rm -f $(SMOKEDIR)/*.hi
	rm -f $(SMOKEDIR)/*.o

dist-clean:	clean
		cabal clean
		rm -f $(SUTDIR)/Patterns/*.hi
		rm -f $(SUTDIR)/Patterns/*.o
		rm -f test/bin/*.o
		rm -f test/bin/*.hi

build: $(tests) $(SUBSRC)
