dirs = client stompl broker

clean:
	cd stompl; make clean 
	cd client; make clean
	cd broker; make clean
	cd patterns; make clean 
	cd stomp-patterns; make clean 
	cd conduit; make clean 
	rm -f test/common/*.o
	rm -f test/common/*.hi
	rm -f test/studies/*.o
	rm -f test/studies/*.hi
	rm -f test/studies/bytestr
	rm -f test/studies/tstfifo
	rm -f test/studies/chan

dist-clean:	clean
	cd stompl; make dist-clean 
	cd client; make dist-clean 
	cd broker; make dist-clean 
	cd patterns; make dist-clean 
	cd stomp-patterns; make dist-clean 
	cd conduit; make dist-clean 
