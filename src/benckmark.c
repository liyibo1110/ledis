#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <errno.h>

#include "ae.h"
#include "anet.h"
#include "sds.h"
#include "adlist.h"

#define REPLY_INT 0
#define REPLY_RETCODE 1
#define REPLY_BULK 2

#define CLIENT_CONNECTING 0
#define CLIENT_SENDQUERY 1
#define CLIENT_READREPLY 2

#define MAX_LATENCY 5000

#define LEDIS_NOTUSED(V) ((void)V)