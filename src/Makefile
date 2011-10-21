dirs = client stompl broker

clean:
	cd stompl; make clean 
	cd client; make clean
	cd broker; make clean
	rm -f test/common/*.o
	rm -f test/common/*.hi

dist-clean:	clean
	cd stompl; make dist-clean 
	cd client; make dist-clean 
	cd broker; make dist-clean 
