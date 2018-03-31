REGRESS = test_automatview
REGRESS_OPTS =

subdir = src/test/modules/test_automatview
top_builddir = ../../../..

include $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

clean distclean maintainer-clean:
	rm -rf tmp_check
