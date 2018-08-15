#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <ctype.h>
#include <inttypes.h>
#include <stdbool.h>

#include <time.h>

#include <arpa/inet.h>

#include <signal.h>
#include <sys/wait.h>

#include <assert.h>
#include <errno.h>

#include "ae.h"
#include "anet.h"
#include "sds.h"
#include "adlist.h"
#include "dict.h"

/*========== server配置相关 ==========*/
#define LEDIS_SERVERPORT    6379    //默认TCP端口
#define LEDIS_MAXIDLETIME   (60*5)  //默认客户端timeout时长
#define LEDIS_QUERYBUF_LEN  1024    //1k
#define LEDIS_LOADBUF_LEN   1024    //1k
#define LEDIS_MAX_ARGS  16
#define LEDIS_DEFAULT_DBNUM 16  //默认db数目
#define LEDIS_CONFIGLINE_MAX    1024    //1k

/*========== Hash table 相关==========*/
#define LEDIS_HT_MINFILL    10  //百分数
#define LEDIS_HT_MINSLOTS   16384   //最小容量


/*========== 返回错误代码 ==========*/
#define LEDIS_OK    0
#define LEDIS_ERR   -1

/*========== 命令类型 ==========*/
#define LEDIS_CMD_BULK  1
#define LEDIS_CMD_INLINE    0

/*========== 对象类型 ==========*/
#define LEDIS_STRING    0
#define LEDIS_LIST  1
#define LEDIS_SET   2
#define LEDIS_SELECTDB  254
#define LEDIS_EOF   255

/*========== list相关 ==========*/
#define LEDIS_HEAD  0   //链表遍历方向
#define LEDIS_TAIL  1

/*========== 日志级别 ==========*/
#define LEDIS_DEBUG 0
#define LEDIS_NOTICE    1
#define LEDIS_WARNING   2

/*========== 消除编译器转换警告 ==========*/
#define LEDIS_NOTUSED(V) ((void) V)

/*========== 定义数据结构 ==========*/
//客户端封装
typedef struct ledisClient{
    int fd;
    dict *dict;
    sds querybuf;
    sds argv[LEDIS_MAX_ARGS];
    int argc;
    int bulklen;
    list *reply;
    int sentlen;
    time_t lastinteraction; //上一次交互的时间点
} ledisClient;

//对象封装，可以是string/list/set
typedef struct ledisObject{
    int type;
    void *ptr;
    int refcount;
} lobj;

//封装save功能的参数，只有一个，所以不需要typedef
struct saveparam{  
    time_t seconds;
    int changes;
};

//服务端封装，只有一个，所以不需要typedef
struct ledisServer{
    int port;
    int fd;
    dict **dict;    //内存存储的指针数组，和dbnum相关，即默认16个元素
    long long dirty;    //自从上次save后经历的变更数
    list *clients;  //客户端链表

    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;
    int verbosity;  //log的输出等级
    int cronloops;
    int maxidletime;

    int dbnum;
    list *objfreelist;  //复用lobj对象的池子链表
    int bgsaveinprogress;   //是否正在bgsave，其实是个bool
    time_t lastsave;
    struct saveparam *saveparams;
    
    int saveparamslen;
    char *logfile;
};

//定义每个ledis命令的实际函数形态
typedef void ledisCommandProc(ledisClient *c);

//封装ledis命令的字段，只有一个数组，所以不需要typedef
struct ledisCommand{
    char *name;
    ledisCommandProc *proc;
    int arity;
    int type;   //是块类命令，还是内联命令
};

//定义会用到的lobj可复用对象
struct sharedObjectsStruct{
    lobj *crlf, *ok, *err, *zerobulk, *nil, *zero, *one, *pong;
} shared;

/*====================================== 函数原型 ===============================*/

static lobj *createObject(int type, void *ptr);
static lobj *createListObject(void);
static void incrRefCount(lobj *o);
static void decrRefCount(void *obj);
static void freeStringObject(lobj *o);
static void freeListObject(lobj *o);
static void freeSetObject(lobj *o);

//所有函数均只在本文件被调用
static void echoCommand(ledisClient *c);

/*====================================== 全局变量 ===============================*/
static struct ledisServer server;
static struct ledisCommand cmdTable[] = {
    {"echo",echoCommand,2,LEDIS_CMD_BULK},
    {"",NULL,0,0}
};

/*====================================== 工具函数 ===============================*/

void ledisLog(int level, const char *fmt, ...){
    va_list ap;

    FILE *fp = server.logfile == NULL ? stdout : fopen(server.logfile, "a");
    if(!fp) return;
    
    va_start(ap, fmt);
    //尝试输出log
    if(level >= server.verbosity){
        char *c = ".-*";    //3个级别的前缀符号
        fprintf(fp, "%c", c[level]);
        vfprintf(fp, fmt, ap);
        fprintf(fp, "\n");
        fflush(fp);
    }

    va_end(ap);
    if(server.logfile)  fclose(fp);
}

/**
 * 如果发生了内存问题，此版本只能打印错误信息并且吐核
 */ 
static void oom(const char *msg){
    fprintf(stderr, "%s: Out of memory\n", msg);
    fflush(stderr);
    sleep(1);
    abort();
}

/*====================================== Hash table类型实现 ===============================*/
//这套dicttype，用作key只能为sds动态字符串，val只能为lobj的dict结构（val具体可为sds, lists, sets）
static unsigned int sdsDictHashFunction(const void *key){
    return dictGenHashFunction(key, sdslen((sds)key));
}

/**
 * 完全一样则返回1,否则返回0
 */ 
static int sdsDictKeyCompare(void *privdata, const void *key1, const void *key2){
    
    DICT_NOTUSED(privdata);
    int l1 = sdslen((sds)key1);
    int l2 = sdslen((sds)key2);
    if(l1 != l2)    return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void sdsDictKeyDestructor(void *privdata, void *key){
    DICT_NOTUSED(privdata);
    sdsfree(key);
}

static void sdsDictValDestructor(void *privdata, void *val){
    DICT_NOTUSED(privdata);
    decrRefCount(val);
}

dictType sdsDictType = {
    sdsDictHashFunction,    //hash函数
    NULL,                   //keyDup函数
    NULL,                   //valDup函数
    sdsDictKeyCompare,      //keyCompare函数
    sdsDictKeyDestructor,   //key清理函数
    sdsDictValDestructor,   //val清理函数
};

/*========================= server相关实现 ===============================*/

/**
 * timeEvent的回调函数，此版本是每隔1秒执行一次，但不是准确的
 */ 
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData){
    
    int loops = server.cronloops++; //当前循环次数
    return 1000;
}

/**
 * 给server的saveparams增加一项可能性
 */ 
static void appendServerSaveParams(time_t seconds, int changes){
    server.saveparams = realloc(server.saveparams, sizeof(struct saveparam)*(server.saveparamslen+1));
    if(server.saveparams == NULL)   oom("appendServerSaveParams");
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

/**
 * 重置server的saveparams全局结构
 */ 
static void ResetServerSaveParams(){
    free(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

/**
 * 初始化server结构的各项配置参数，只会在启动时会调用
 */ 
static void initServerConfig(){
    server.dbnum = LEDIS_DEFAULT_DBNUM;
    server.port = LEDIS_SERVERPORT;
    server.verbosity = LEDIS_DEBUG;
    server.maxidletime = LEDIS_MAXIDLETIME;
    server.saveparams = NULL;
    server.logfile = NULL;  //到后面再设定

    ResetServerSaveParams();
    //给默认的配置
    appendServerSaveParams(60*60, 1);   //1小时后要达到1次变动即save
    appendServerSaveParams(300, 100);   //5分钟后要达到100次变动即save
    appendServerSaveParams(60, 10000);  //1分钟后要达到10000次变动即save
}

/**
 * 初始化server结构自身功能，只会在启动时调用
 */ 
static void initServer(){

    //忽略hup和pipe信号的默认行为（终止server进程）
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    server.clients = listCreate();
    server.objfreelist = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    //初始化dict数组，只是分配了dbnum个地址空间，并没有为实际dict结构分配内存
    server.dict = malloc(sizeof(dict*)*server.dbnum);
    if(!server.dict || !server.clients || !server.el || !server.objfreelist){
        oom("server initialization");
    }
    server.fd = anetTcpServer(server.neterr, server.port, NULL);
    if(server.fd == -1){
        ledisLog(LEDIS_WARNING, "Opening TCP port: %s", server.neterr);
        exit(EXIT_FAILURE);
    }
    //继续给dict数组内部真实分配
    for(int i = 0; i < server.dbnum; i++){
        server.dict[i] = dictCreate(&sdsDictType, NULL);
        if(!server.dict[i]){
            oom("server initialization");
        }
    }
    server.cronloops = 0;
    server.bgsaveinprogress = 0;
    server.lastsave = time(NULL);
    server.dirty = 0;
    aeCreateTimeEvent(server.el, 1000, serverCron, NULL, NULL);
}

/**
 * 从配置文件中加载，此版本实现的比较低端
 */ 
static void loadServerConfig(char *filename){
    FILE *fp = fopen(filename, "r");
    char buf[LEDIS_CONFIGLINE_MAX+1], *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if(!fp){
        ledisLog(LEDIS_WARNING, "Fatal error, can't open config file");
        exit(EXIT_FAILURE);
    }

    //开始按行读取
    while(fgets(buf, LEDIS_CONFIGLINE_MAX+1, fp) != NULL){

    }
}

/**
 * 创建需要的常量obj们
 */ 
static void createSharedObjects(void){
    shared.crlf = createObject(LEDIS_STRING, sdsnew("\r\n"));
    shared.ok = createObject(LEDIS_STRING, sdsnew("+OK\r\n"));
    shared.err = createObject(LEDIS_STRING, sdsnew("-ERR\r\n"));
    shared.zerobulk = createObject(LEDIS_STRING, sdsnew("0\r\n\r\n"));
    shared.nil = createObject(LEDIS_STRING, sdsnew("nil\r\n"));
    shared.zero = createObject(LEDIS_STRING, sdsnew("0\r\n"));
    shared.one = createObject(LEDIS_STRING, sdsnew("1\r\n"));
    shared.pong = createObject(LEDIS_STRING, sdsnew("+PONG\r\n"));
}

/*========================= ledis对象相关实现 ===============================*/

/**
 * 创建一个对象，优先从复用池里取，避免malloc系统调用
 */ 
static lobj *createObject(int type, void *ptr){
    lobj *o;
    if(listLength(server.objfreelist)){
        listNode *head = listFirst(server.objfreelist);
        o = listNodeValue(head);
        //必须从复用链表里删除，listCreate时是没有free函数传递的，所以只是修改了结构而已
        listDelNode(server.objfreelist, head);
    }else{
        o = malloc(sizeof(*o));
    }
    if(!o)  oom("createObject");
    //初始化其他字段
    o->type = type;
    o->ptr = ptr;
    o->refcount = 1;
    return o;
}

/**
 * 创建一个list类型的对象
 */ 
static lobj *createListObject(void){
    list *l = listCreate();
    if(!l)  oom("createListObject");
    listSetFreeMethod(l, decrRefCount); //重要一步，所有的obj都要绑定decrRefCount函数
    return createObject(LEDIS_LIST, l);
}

/**
 * 给obj的ref引用计数加1
 */ 
static void incrRefCount(lobj *o){
    o->refcount++;
}

/**
 * 给obj的ref引用计数减1,如果变成0,则调用相应类型的free
 */ 
static void decrRefCount(void *obj){
    lobj *o = obj;
    if(--(o->refcount) == 0){
        switch(o->type){
            case LEDIS_STRING : freeStringObject(o);    break;
            case LEDIS_LIST : freeListObject(o);    break;
            case LEDIS_SET : freeSetObject(o);    break;
            default : assert(0 != 0);   break;
        }
        //引用为0,只是free里面的ptr数据，obj本身会加入到free列表首部，等待复用
        if(!listAddNodeHead(server.objfreelist, o)){
            free(o);
        }
    }
}

/**
 * 释放sds动态字符串类型的obj
 */ 
static void freeStringObject(lobj *o){
    sdsfree(o->ptr);
}

/**
 * 释放list链表类型的obj
 */ 
static void freeListObject(lobj *o){
    listRelease((list*)o->ptr);
}

/**
 * 释放set类型的obj
 */ 
static void freeSetObject(lobj *o){
    //什么也不做，此版本没有set这个类型
    o = o;
}

int main(int argc, char *argv[]){

    //初始化server配置
    initServerConfig();
    //初始化server
    initServer();
    if(argc == 2){  //制定了conf文件
        ResetServerSaveParams();    //清空saveparams字段
        loadServerConfig(argv[1]);
        ledisLog(LEDIS_NOTICE, "Configuration loaded");
    }else if(argc > 2){
        fprintf(stderr, "Usage: ./ledis-server [/path/to/ledis.conf]\n");
        exit(EXIT_FAILURE);
    }
    ledisLog(LEDIS_NOTICE, "Server started");
    //尝试恢复数据库dump.ldb文件

    //基于server的fd，创建fileEvent
    ledisLog(LEDIS_NOTICE, "The server is now ready to accept connections");
    aeMain(server.el);  //开始轮询，直到el的stop被置位
    aeDeleteEventLoop(server.el);
    exit(EXIT_SUCCESS);
}