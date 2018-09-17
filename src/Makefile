DEBUG ?= -g -rdynamic -ggdb
CFLAGS ?= -std=c99 -pedantic -O2 -Wall -W -DSDS_ABORT_ON_OOM
CCOPT = $(CFLAGS)

OBJ = adlist.o ae.o anet.o dict.o sds.o ledis.o zmalloc.o lzf_c.o lzf_d.o, pqsort.o
BENCHOBJ = ae.o anet.o benchmark.o sds.o adlist.o zmalloc.o
BENCHPRGNAME = ledis-benchmark.out
PRGNAME = ledis-server.out
CLIOBJ = anet.o sds.o adlist.o ledis-cli.o zmalloc.o
CLIPRGNAME = ledis-cli.out
CLIENTOBJ = anet.o client.o
CLIENTPRGNAME = ledis-client.out

all: ledis-server.out ledis-benchmark.out ledis-cli.out ledis-client.out

adlist.o: adlist.c adlist.h zmalloc.h
ae.o: ae.c ae.h zmalloc.h
anet.o: anet.c anet.h fmacros.h
benchmark.o: benchmark.c fmacros.h ae.h anet.h sds.h adlist.h zmalloc.h
dict.o: dict.c dict.h fmacros.h zmalloc.h
lzf_c.o: lzf_c.c lzfP.h
lzf_d.o: lzf_d.c lzfP.h
pqsort.o: pqsort.c
ledis-cli.o: ledis-cli.c fmacros.h anet.h sds.h adlist.h zmalloc.h
ledis.o: ledis.c fmacros.h ae.h sds.h anet.h dict.h adlist.h zmalloc.h lzf.h pqsort.h config.h
sds.o: sds.c sds.h zmalloc.h
zmalloc.o: zmalloc.c config.h
client.o: client.c

%.o: %.c
	$(CC) -c $(CCOPT) $(DEBUG) $<

ledis-client.out: $(CLIENTOBJ)
	$(CC) -o $(CLIENTPRGNAME) $(CCOPT) $(DEBUG) $(CLIENTOBJ)

ledis-cli.out: $(CLIOBJ)
	$(CC) -o $(CLIPRGNAME) $(CCOPT) $(DEBUG) $(CLIOBJ)

ledis-server.out: $(OBJ)
	$(CC) -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ)
	@echo ""
	@echo "Launch the ledis server with ./ledis-server.out ./ledis.conf"
	@echo ""

ledis-benchmark.out: $(BENCHOBJ)
	$(CC) -o $(BENCHPRGNAME) $(CCOPT) $(DEBUG) $(BENCHOBJ)

clean:
	rm -rf $(PRGNAME) $(CLIENTPRGNAME) $(BENCHPRGNAME) $(CLIPRGNAME) *.o