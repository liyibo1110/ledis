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
#include "zmalloc.h"

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
    int keepalive;  //可以复用
    long long start;
    long long totlatency;
    int *latency;   //动态数组，统计每个运行时间的各自总数
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
    zfree(c);

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
        ln = next;
    }
}

/**
 * 重置client使得下一次还能用，只有开启了keepalive才会被调用
 */ 
static void resetClient(client c){
    aeDeleteFileEvent(config.el, c->fd, AE_WRITABLE);
    aeDeleteFileEvent(config.el, c->fd, AE_READABLE);
    aeCreateFileEvent(config.el, c->fd, AE_WRITABLE, writeHandler, c, NULL);
    sdsfree(c->ibuf);
    c->ibuf = sdsempty();
    c->readlen = (c->replytype == REPLY_BULK) ? -1 : 0;
    c->written = 0;
    c->state = CLIENT_SENDQUERY;
    c->start = mstime();
}

/**
 * 处理单个client完成后的收尾
 */ 
static void clientDone(client c){
    config.donerequests++;
    long long latency = mstime() - c->start;
    if(latency > MAX_LATENCY) latency = MAX_LATENCY;
    config.latency[latency]++;  //相应时间点统计值加1

    //都完事了
    if(config.donerequests == config.requests){
        freeClient(c);
        aeStop(config.el);
        return;
    }
    if(config.keepalive){
        resetClient(c);
    }else{
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        freeClient(c);
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask){
    
    char buf[1024];
    client c = privdata;
    //都在privdata里面，外面的没用
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(fd);
    LEDIS_NOTUSED(mask);

    int nread = read(c->fd, buf, 1024);
    if(nread == -1){
        fprintf(stderr, "Reading from socket: %s\n", strerror(errno));
        return;
    }
    if(nread == 0){
        fprintf(stderr, "EOF from client\n");
        freeClient(c);
        return;
    }
    c->ibuf = sdscatlen(c->ibuf, buf, nread);

    if(c->replytype == REPLY_INT || 
        c->replytype == REPLY_RETCODE || 
        (c->replytype == REPLY_BULK && c->readlen == -1)){

        char *p;
        if((p = strchr(c->ibuf, '\n')) != NULL){
            if(c->replytype == REPLY_BULK){
                //如果是bulk类型，说明是类似这样的5\r\nhello\r\n，需要分别取长度和信息本身
                *p = '\0';
                *(p-1) = '\0';  //将reply字符串中的\r\n去掉
                if(memcmp(c->ibuf, "nil", 3) == 0){
                    clientDone(c);
                    return;
                }
                //如果返回不是nil，则尝试读取bulk字符串
                c->readlen = atoi(c->ibuf)+2;
                c->ibuf = sdsrange(c->ibuf, (p-c->ibuf)+1, -1);
                //这里不return
            }else{  //普通类型，类似这样hello\r\n
                c->ibuf = sdstrim(c->ibuf, "\r\n");
                clientDone(c);
                return;
            }
        }

        //判断bulk类型读取是否完整
        if((unsigned)c->readlen == sdslen(c->ibuf)){
            clientDone(c);
        }
    }
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
    client c = zmalloc(sizeof(struct _client));
    char err[ANET_ERR_LEN];

    c->fd = anetTcpNonBlockConnect(err, config.hostip, config.hostport);
    //printf("c->fd=%d\n", c->fd);
    if(c->fd == ANET_ERR){
        zfree(c);
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

    aeCreateFileEvent(config.el, c->fd, AE_WRITABLE, writeHandler, c, NULL);
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

static void showLatencyReport(char *title){
    
    //算出平均每秒处理了多少req
    float reqpersec = (float)config.donerequests/((float)config.totlatency/1000);
    if(!config.quiet){
        //输出基础信息
        printf("====== %s ======\n", title);
        printf("  %d requests completed in %2.f seconds\n", config.donerequests, 
                (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");
        //输出bitmap结构
        float perc;
        int seen = 0;
        for(int i = 0; i < MAX_LATENCY; i++){
            if(config.latency[i]){
                seen += config.latency[i];  //追加client数
                perc = ((float)seen*100)/config.donerequests;   //该时间的client，占所有完成的client比值
                printf("%.2f%% <= %d milliseconds\n", perc, i);
            }
        }
        printf("%.2f requests per second\n", reqpersec);
    }else{
        printf("%s: %.2f requests per second\n", title, reqpersec);
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
    showLatencyReport(title);
    freeAllClients();
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
            char *ip = zmalloc(NI_MAXHOST);
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
    config.latency = zmalloc(sizeof(int)*(MAX_LATENCY+1));
    
    config.hostip = "127.0.0.1";
    config.hostport = 6379;

    parseOptions(argc, argv);

    if(config.keepalive == 0){
        printf("WARING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' in order to use a lot of clients/requests\n");
    }

    client c;
    do{
        /*========== 测试PING ==========*/
        prepareForBenchmark();
        c = createClient(); //创建一个client
        if(!c) exit(EXIT_FAILURE);
        c->obuf = sdscat(c->obuf, "PING\r\n");
        c->replytype = REPLY_RETCODE;   //设置返回的类型
        createMissingClients(c);    //弄出剩余的client
        aeMain(config.el);  //开始执行，直到都完成才会返回
        endBenchmark("PING");
        
        /*========== 测试SET ==========*/
        prepareForBenchmark();
        c = createClient(); //创建一个client
        if(!c) exit(EXIT_FAILURE);
        c->obuf = sdscatprintf(c->obuf, "SET foo %d\r\n", config.datasize);
        {
            char *data = zmalloc(config.datasize+2); //还有\r\n
            memset(data, 'x', config.datasize); //用字符x填充datasize个
            data[config.datasize] = '\r';
            data[config.datasize+1] = '\n';
            c->obuf = sdscatlen(c->obuf, data, config.datasize+2);
        }
        c->replytype = REPLY_RETCODE;   //设置返回的类型
        createMissingClients(c);    //弄出剩余的client
        aeMain(config.el);  //开始执行，直到都完成才会返回
        endBenchmark("SET");

        /*========== 测试GET ==========*/
        prepareForBenchmark();
        c = createClient();
        if(!c) exit(EXIT_FAILURE);
        c->obuf = sdscat(c->obuf, "GET foo\r\n");
        c->replytype = REPLY_BULK;  //返回类型是bulk了
        c->readlen = -1;
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("GET");

        /*========== 测试INCR ==========*/
        prepareForBenchmark();
        c = createClient();
        if(!c) exit(EXIT_FAILURE);
        c->obuf = sdscat(c->obuf, "INCR counter\r\n");
        c->replytype = REPLY_INT;  //返回类型是int了
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("INCR");

        /*========== 测试LPUSH ==========*/
        prepareForBenchmark();
        c = createClient();
        if(!c) exit(EXIT_FAILURE);
        c->obuf = sdscat(c->obuf, "LPUSH mylist 3\r\nbar\r\n");
        c->replytype = REPLY_INT;  //返回类型是int了
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LPUSH");

        /*========== 测试LPOP ==========*/
        prepareForBenchmark();
        c = createClient();
        if(!c) exit(EXIT_FAILURE);
        c->obuf = sdscat(c->obuf, "LPOP mylist\r\n");
        c->replytype = REPLY_BULK;  //返回类型是bulk了
        c->readlen = -1;
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LPOP");

        printf("\n");
    }while(config.loop);

    return EXIT_SUCCESS;
}