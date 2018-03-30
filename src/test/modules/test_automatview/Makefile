# subdir = src/test/modules/test_automatview
# top_builddir = ../../../..
# include $(top_builddir)/src/Makefile.global
# # include $(top_srcdir)/contrib/contrib-global.mk

# all:
# 	/usr/bin/env perl run.pl

# src/test/modules/test_automatview/Makefile

REGRESS = test_automatview
REGRESS_OPTS =
# REGRESS_OPTS = --temp-config=$(top_srcdir)/src/test/modules/commit_ts/commit_ts.conf

subdir = src/test/modules/test_automatview
top_builddir = ../../../..

include $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

clean distclean maintainer-clean:
	rm -rf tmp_check
