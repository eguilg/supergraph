TEMPLATE = app
CCFLAG += -O0 -std=gnu11 -march=native
CONFIG += console
CONFIG -= app_bundle
CONFIG -= qt

HEADERS += supergraph.h threadpool.h supergraph_loader.h
SOURCES += main.c supergraph.c threadpool.c supergraph_loader.c
LIBS += -lm -lpthread
