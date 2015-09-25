TESTLOG=/tmp/lw.testlog
all: ../logwatcher.py access_test test.ini
	rm -f ${TESTLOG}
	ln -s `pwd`/access_test /tmp/lw.testlog
	../logwatcher.py -c test.ini -D -t -b -q #| egrep "(ptd|rtms)"
	rm ${TESTLOG}
