dirs = client stompl broker

clean:
	cd stompl; make clean 
	cd client; make clean
	cd broker; make clean
	rm -f test/common/*.o
	rm -f test/common/*.hi
	rm -f test/studies/*.o
	rm -f test/studies/*.hi
	rm -f test/studies/bytestr

dist-clean:	clean
	cd stompl; make dist-clean 
	cd client; make dist-clean 
	cd broker; make dist-clean 
