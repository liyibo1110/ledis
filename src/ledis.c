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
    dict **dict;    //内存存储的指针数组
    long long dirty;    //自从上次save后经历的变更数
    list *clients;  //客户端链表

    char netarr[ANET_ERR_LEN];
    aeEventLoop *el;
    int verbosity;  //log的输出等级
    int cronloops;
    int maxidletime;

    int dbnum;
    list *objfreelist;  //复用lobj对象的池子链表
    int bgsaveinprogress;
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
static void decrRefCount(void *o);
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
static int sdsDictCompare(void *privdata, const void *key1, const void *key2){
    
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

/*========================= ledis对象相关实现 ===============================*/

/**
 * 创建一个对象，优先从复用池里取，避免malloc系统调用
 */ 
static lobj *createObject(int type, void *ptr){
    lobj *o;
    if(listLength(server.objfreelist)){
        listNode *head = listFirst(server.objfreelist);
        o = listNodeValue(head);
        //必须从复用链表里删除
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

}