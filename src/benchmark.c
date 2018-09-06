#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <errno.h>

#include <netdb.h>

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

static struct config{
    int numclients; //并发客户端数目
    int requests;   //请求次数
    int liveclients;
    int donerequests;
    int keysize;
    int datasize;
    aeEventLoop *el;
    char *hostip;
    int hostport;
    int keepalive;
    long long start;
    long long totlatency;
    int *latency;
    list *clients;
    int quiet;  //安静模式
    int loop;   //是否永远循环
} config;

typedef struct _client{
    int state;
    int fd;
    sds obuf;
    sds ibuf;
    int readlen;
    unsigned int written;
    int replytype;
    long long start;
} *client;

/*========== 函数原型 ==========*/
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);

static long long mstime(void){
    struct timeval tv;

    gettimeofday(&tv, NULL);
    long long mst = ((long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void freeClient(client c){
    
    //清理client本身
    aeDeleteFileEvent(config.el, c->fd, AE_WRITABLE);
    aeDeleteFileEvent(config.el, c->fd, AE_READABLE);
    sdsfree(c->ibuf);
    sdsfree(c->obuf);
    close(c->fd);
    free(c);

    //清理config里面的client
    config.liveclients--;
    listNode *ln = listSearchKey(config.clients, c);    //使用默认的结构体实体来比较相等
    assert(ln != NULL);
    listDelNode(config.clients, ln);
}

static void freeAllClients(void){
    listNode *ln = config.clients->head;
    listNode *next;
    while(ln){
        next = ln->next;
        freeClient(ln->value);
        ln = ln->next;
    }
}

static void resetClient(client c){

}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask){
    
    client c = privdata;
    //都在privdata里面，外面的没用
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(fd);
    LEDIS_NOTUSED(mask);

    //改变状态
    if(c->state == CLIENT_CONNECTING){
        c->state = CLIENT_SENDQUERY;
        c->start = mstime();
    }
    //可能是发送一部分，如果都发完了就不执行了
    if(sdslen(c->obuf) > c->written){
        void *ptr = c->obuf+c->written; //剩下的字符串
        int len = sdslen(c->obuf) - c->written;
        int nwritten = write(c->fd, ptr, len);
        if(nwritten == -1){
            fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        //直到written字段和obuf一样长，才说明write完全了
        if(sdslen(c->obuf) == c->written){
            aeDeleteFileEvent(config.el, c->fd, AE_WRITABLE);
            aeCreateFileEvent(config.el, c->fd, AE_READABLE, readHandler, c, NULL);
            c->state = CLIENT_READREPLY;
        }
    }
}

static client createClient(void){
    client c = malloc(sizeof(struct _client));
    char err[ANET_ERR_LEN];

    c->fd = anetTcpNonBlockConnect(err, config.hostip, config.hostport);
    if(c->fd == ANET_ERR){
        free(c);
        fprintf(stderr, "Connect: %s\n", err);
        return NULL;
    }
    anetTcpNoDelay(NULL, c->fd);
    //初始化各个字段
    c->obuf = sdsempty();
    c->ibuf = sdsempty();
    c->readlen = 0;
    c->written = 0;
    c->state = CLIENT_CONNECTING;

    config.liveclients++;
    listAddNodeTail(config.clients, c);
    return c;
}

/**
 * 以参数client为模板，建出新的client，直到数目达到了numclients
 */ 
static void createMissingClients(client c){
    while(config.liveclients < config.numclients){
        client new = createClient();
        if(!new) continue;
        sdsfree(new->obuf); //安全
        new->obuf = sdsdup(c->obuf);
        new->replytype = c->replytype;
        if(c->replytype == REPLY_BULK) new->readlen = -1;
    }
}

/**
 * 预处理
 */ 
static void prepareForBenchmark(void){
    memset(config.latency, 0, sizeof(int)*(MAX_LATENCY+1));
    config.start = mstime();
    config.donerequests = 0;
}

/**
 * 结束清理
 */ 
static void endBenchmark(char *title){
    config.totlatency = mstime() - config.start;
} 

void parseOptions(int argc, char **argv){

    for(int i = 1; i < argc; i++){
        int lastarg = i==argc-1;    //是不是最后一个参数
    
        if(!strcmp(argv[i], "-c") && !lastarg){
            config.numclients = atoi(argv[i+1]);
            i++;
        }else if(!strcmp(argv[i], "-n") && !lastarg){
            config.requests = atoi(argv[i+1]);
            i++;
        }else if(!strcmp(argv[i], "-k") && !lastarg){
            config.keepalive = atoi(argv[i+1]);
            i++;
        }else if(!strcmp(argv[i], "-h") && !lastarg){
            char *ip = malloc(NI_MAXHOST);
            if(anetResolve(NULL, argv[i+1], ip) == ANET_ERR){
                printf("Can't resolve %s\n", argv[i]);
                exit(EXIT_FAILURE);
            }
            config.hostip = ip;
            i++;
        }else if(!strcmp(argv[i], "-p") && !lastarg){
            config.hostport = atoi(argv[i+1]);
            i++;
        }else if(!strcmp(argv[i], "-d") && !lastarg){
            config.datasize = atoi(argv[i+1]);
            i++;
            if(config.datasize < 1) config.datasize = 1;
            if(config.datasize > 1024 * 1024) config.datasize = 1024 * 1024;
        }else if(!strcmp(argv[i], "-q")){
            config.quiet = 1;
        }else if(!strcmp(argv[i], "-l")){
            config.loop = 1;
        }else{
            printf("Wrong option '%s' or option argument missing\n\n",argv[i]);
            printf("Usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]\n\n");
            printf(" -h <hostname>      Server hostname (default 127.0.0.1)\n");
            printf(" -p <hostname>      Server port (default 6379)\n");
            printf(" -c <clients>       Number of parallel connections (default 50)\n");
            printf(" -n <requests>      Total number of requests (default 10000)\n");
            printf(" -d <size>          Data size of SET/GET value in bytes (default 2)\n");
            printf(" -k <boolean>       1=keep alive 0=reconnect (default 1)\n");
            printf(" -q                 Quiet. Just show query/sec values\n");
            printf(" -l                 Loop. Run the tests forever\n");
            exit(EXIT_FAILURE);
        }
    }
}

int main(int argc, char **argv){

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.numclients = 50;
    config.requests = 10000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop();
    config.keepalive = 1;
    config.donerequests = 0;
    config.datasize = 3;
    config.quiet = 0;
    config.loop = 0;
    config.clients = listCreate();
    config.latency = malloc(sizeof(int)*(MAX_LATENCY+1));
    
    config.hostip = "127.0.0.1";
    config.hostport = 6379;

    parseOptions(argc, argv);

    if(config.keepalive == 0){
        printf("WARING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' in order to use a lot of clients/requests\n");
    }

    do{

    }while(config.loop);

    return EXIT_SUCCESS;
}