.PHONY: clean all doc install check_doc check test

all: clean check install

doc:
	R -e 'devtools::document()'

check_doc:
	R -e 'devtools::check_doc()'

check:
	R -e 'devtools::check()' --no-tests

install:
	R CMD INSTALL .

clean:
	-rm -fr man/*
	-rm -f NAMESPACE

test:
	R -e "devtools::test()"
