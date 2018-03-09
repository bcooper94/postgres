#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for rewrite
#
# IDENTIFICATION
#    src/backend/rewrite/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/rewrite
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = rewriteRemove.o rewriteDefine.o \
       rewriteHandler.o rewriteManip.o rewriteSupport.o \
       rowsecurity.o automatviewselect.o automatviewselect_match.o \
       automatviewselect_unparse.o automatviewselect_rewrite.o \
       automatviewselect_utils.o

include $(top_srcdir)/src/backend/common.mk
