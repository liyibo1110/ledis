DEBUG ?= -g
CFLAGS ?= -O2 -Wall -W -DSDS_ABORT_ON_OOM
CCOPT = $(CFLAGS)

OBJ = adlist.o ae.o anet.o dict.o sds.o ledis.o 
PRGNAME = ledis-server.out
CLIENTOBJ = anet.o client.o
CLIENTNAME = ledis-client.out

all: ledis-server.out ledis-client.out

adlist.o: adlist.c adlist.h
ae.o: ae.c ae.h
anet.o: anet.c anet.h
dict.o: dict.c dict.h
sds.o: sds.c sds.h
ledis.o: ledis.c adlist.h ae.h anet.h dict.h sds.h
client.o: client.c

%.o: %.c
	$(CC) -c $(CCOPT) $(DEBUG) $<

ledis-client.out: $(CLIENTOBJ)
	$(CC) -o $(CLIENTNAME) $(CCOPT) $(DEBUG) $(CLIENTOBJ)

ledis-server.out: $(OBJ)
	$(CC) -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ)
	@echo ""
	@echo "Launch the ledis server with ./ledis-server.out ./ledis.conf"
	@echo ""

clean:
	rm -rf $(PRGNAME) $(CLIENTNAME) *.o