#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#define _USE_POSIX199309
#include <signal.h>
#include <execinfo.h>
#include <ucontext.h>
#include <sys/wait.h>

#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <inttypes.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <limits.h>
#include <execinfo.h>
#include <stdbool.h>

#include "ledis.h"
#include "ae.h"
#include "sds.h"
#include "anet.h"
#include "dict.h"
#include "adlist.h"
#include "zmalloc.h"
#include "lzf.h"
#include "pqsort.h"

#include "config.h"

#define LEDIS_VERSION "0.900"

/*========== server配置相关 ==========*/
#define LEDIS_SERVERPORT    6379    //默认TCP端口
#define LEDIS_MAXIDLETIME   (60*5)  //默认客户端timeout时长
#define LEDIS_IOBUF_LEN  1024    //1k
#define LEDIS_LOADBUF_LEN   1024    //1k
#define LEDIS_STATIC_ARGS  4
#define LEDIS_DEFAULT_DBNUM 16  //默认db数目
#define LEDIS_CONFIGLINE_MAX    1024    //1k
#define LEDIS_OBJFREELIST_MAX 1000000 //obj池子最大数目
#define LEDIS_MAX_SYNC_TIME 60 //
#define LEDIS_EXPIRELOOKUPS_PER_CRON 100    //每秒尝试最多淘汰100个key
#define LEDIS_MAX_WRITE_PER_EVENT (1024*64)
#define LEDIS_REQUEST_MAX_SIZE (1024*1024*256)  //inline command最大256M

/*========== Hash table 相关==========*/
#define LEDIS_HT_MINFILL    10  //dict实际占用百分数，小于这个比率就会尝试再收缩（前提是扩张过）
//#define LEDIS_HT_MINSLOTS   16384   //最小容量，如果不扩长，也就不会继续收缩


/*========== 返回错误代码 ==========*/
#define LEDIS_OK    0
#define LEDIS_ERR   -1

/*========== 命令类型 ==========*/
#define LEDIS_CMD_BULK  1
#define LEDIS_CMD_INLINE    2
#define LEDIS_CMD_DENYOOM   4   //追加标记，有这个的command在超过maxmemory时返回错误   

/*========== 对象类型 ==========*/
#define LEDIS_STRING    0
#define LEDIS_LIST  1
#define LEDIS_SET   2
#define LEDIS_HASH  3

//只会用在持久化功能里
#define LEDIS_EXPIRETIME 253
#define LEDIS_SELECTDB  254
#define LEDIS_EOF   255

/*========== 持久化格式相关 ==========*/
/* 定义了4种存储格式，用前2位来识别，分别是6位/14位/32位/其他 */
#define LEDIS_LDB_6BITLEN 0
#define LEDIS_LDB_14BITLEN 1
#define LEDIS_LDB_32BITLEN 2
#define LEDIS_LDB_ENCVAL 3
#define LEDIS_LDB_LENERR UINT_MAX

/* 定义了4种其他类型格式，也用前2位来识别，分别是8位带符号INT/16位带符号INT/32位带符号INT/LZF压缩字符串 */
#define LEDIS_LDB_ENC_INT8 0
#define LEDIS_LDB_ENC_INT16 1
#define LEDIS_LDB_ENC_INT32 2
#define LEDIS_LDB_ENC_LZF 3

/*========== 客户端flags ==========*/
#define LEDIS_CLOSE 1 //客户端是普通端，用完直接关闭
#define LEDIS_SLAVE 2 //客户端是个从服务端
#define LEDIS_MASTER 4 //客户端是个主服务端
#define LEDIS_MONITOR 8 //客户端是个从monitor

/*========== 从的状态 ==========*/
#define LEDIS_REPL_NONE 0 //没有任何主从配置
#define LEDIS_REPL_CONNECT 1 //从的初始状态，还没有获取主的db文件
#define LEDIS_REPL_CONNECTED 2 //从已经获取到了主的db文件

//主从复制中，主的状态
#define LEDIS_REPL_WAIT_BGSAVE_START 3  //从在等bgsave开始运行
#define LEDIS_REPL_WAIT_BGSAVE_END 4    //从在等bgsave运行完毕
#define LEDIS_REPL_SEND_BULK 5  //从在发送bulk信息
#define LEDIS_REPL_ONLINE 6 //有待完善

/*========== list相关 ==========*/
#define LEDIS_HEAD  0   //链表遍历方向
#define LEDIS_TAIL  1

/*========== 排序相关 ==========*/
#define LEDIS_SORT_GET 0
#define LEDIS_SORT_DEL 1
#define LEDIS_SORT_INCR 2
#define LEDIS_SORT_DECR 3
#define LEDIS_SORT_ASC 4
#define LEDIS_SORT_DESC 5
#define LEDIS_SORTKEY_MAX 1024

/*========== 日志级别 ==========*/
#define LEDIS_DEBUG 0
#define LEDIS_NOTICE    1
#define LEDIS_WARNING   2

/*========== 消除编译器转换警告 ==========*/
#define LEDIS_NOTUSED(V) ((void) V)

/*========== 定义数据结构 ==========*/

//对象封装，可以是string/list/set
typedef struct ledisObject{
    void *ptr;
    int type;
    int refcount;
} lobj;

typedef struct ledisDb{
    dict *dict;
    dict *expires;  //额外记录了过期的key/val
    int id;
} ledisDb;

//客户端封装
typedef struct ledisClient{
    int fd;
    //dict *dict;
    ledisDb *db;    //是指针，并不是数组
    int dictid;
    sds querybuf;
    lobj **argv;    //不再是固定16个，而是动态了
    int argc;
    int bulklen;
    list *reply;
    int sentlen;
    time_t lastinteraction; //上一次交互的时间点
    int flags; //LEDIS_CLOSE或者LEDIS_SLAVE或者LEDIS_MONITOR
    int slaveseldb; //如果client是从服务端，则代表自己的dbid
    int authenticated;  //是否已验证
    int replstate;  //如果是从，用作存放主从的状态
    int repldbfd;   //主从复制中，db文件的fd
    long repldboff; //db文件的偏移量
    off_t repldbsize;   //db文件总大小
} ledisClient;

//封装save功能的参数，只有一个，所以不需要typedef
struct saveparam{  
    time_t seconds;
    int changes;
};

//服务端封装，只有一个，所以不需要typedef
struct ledisServer{
    int port;
    int fd;
    //dict **dict;    //内存存储的指针数组，和dbnum相关，即默认16个元素
    ledisDb *db;    //其实还是数组，并不是指针
    dict *sharingpool;
    unsigned int sharingpoolsize;
    long long dirty;    //自从上次save后经历的变更数
    list *clients;  //客户端链表
    list *slaves;   //从服务端链表
    list *monitors; //monitor客户端链表

    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;
    int cronloops;
    list *objfreelist;  //复用lobj对象的池子链表
    time_t lastsave;
    size_t usedmemory; //使用的堆内存总量，单位是Mbyte
    //只在stats使用
    time_t stat_starttime;  //server启动的时间
    long long stat_numcommands; //执行command的总数
    long long stat_numconnections;  //收到的连接数

    /*======配置相关======*/
    int verbosity;  //log的输出等级
    int glueoutputbuf;
    
    int maxidletime;
    int dbnum;
    int daemonize;
    char *pidfile;
    int bgsaveinprogress;   //是否正在bgsave，其实是个bool
    pid_t bgsavechildpid;

    struct saveparam *saveparams;
    int saveparamslen;
    char *logfile;
    char *bindaddr;
    char *dbfilename;
    char *requirepass;
    int shareobjects;

    /*======主从相关======*/
    int isslave;
    char *masterhost;
    int masterport;
    ledisClient *master;
    int replstate;
    
    unsigned int maxclients;
    unsigned int maxmemory;

    /*======排序相关======*/
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
};

//定义每个ledis命令的实际函数形态
typedef void ledisCommandProc(ledisClient *c);

//封装ledis命令的字段，只有一个数组，所以不需要typedef
struct ledisCommand{
    char *name;
    ledisCommandProc *proc;
    int arity;
    int flags;   //是块类命令，还是内联命令
};

struct ledisFunctionSym{
    char *name;
    unsigned long pointer;
};


//进一步封装obj结构，增加了排序相关的辅助字段
typedef struct _ledisSortObject{
    lobj *obj;
    union{
        double score;
        lobj *cmpobj;
    } u;
} ledisSortObject;

typedef struct _ledisSortOperation{
    int type;
    lobj *pattern;
} ledisSortOperation;

//定义会用到的lobj可复用对象
struct sharedObjectsStruct{
    lobj *crlf, *ok, *err, *emptybulk, *czero, *cone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *plus,
    *select0, *select1, *select2, *select3, *select4,
    *select5, *select6, *select7, *select8, *select9;
} shared;

/*====================================== 函数原型 ===============================*/

static void freeStringObject(lobj *o);
static void freeListObject(lobj *o);
static void freeSetObject(lobj *o);
static void decrRefCount(void *obj);
static lobj *createObject(int type, void *ptr);
static void freeClient(ledisClient *c);
static int ldbLoad(char *filename);
static void addReply(ledisClient *c, lobj *obj);
static void addReplySds(ledisClient *c, sds s);
static void incrRefCount(lobj *o);
static int ldbSaveBackground(char *filename);
static lobj *createStringObject(char *ptr, size_t len);
static void replicationFeedSlaves(list *slaves, struct ledisCommand *cmd, int dictid, lobj **argv, int argc);
static int syncWithMaster(void);    //和主同步

static lobj *tryObjectSharing(lobj *o);
static int removeExpire(ledisDb *db, lobj *key);
static int expireIfNeeded(ledisDb *db, lobj *key);
static int deleteIfVolatile(ledisDb *db, lobj *key);
static int deleteKey(ledisDb *db, lobj *key);
static time_t getExpire(ledisDb *db, lobj *key);
static int setExpire(ledisDb *db, lobj *key, time_t when);
static void updateSlavesWaitingBgsave(int bgsaveerr);   //貌似单词拼错了

static void freeMemoryIfNeeded(void);
static int processCommand(ledisClient *c);
static void setupSigSegvAction(void);
static void ldbRemoveTempFile(pid_t childpid);

//所有函数均只在本文件被调用
static void authCommand(ledisClient *c);
static void pingCommand(ledisClient *c);
static void echoCommand(ledisClient *c);
static void dbsizeCommand(ledisClient *c);
static void saveCommand(ledisClient *c);
static void bgsaveCommand(ledisClient *c);

static void setCommand(ledisClient *c);
static void setnxCommand(ledisClient *c);
static void getCommand(ledisClient *c);
static void keysCommand(ledisClient *c);
static void delCommand(ledisClient *c);
static void existsCommand(ledisClient *c);
static void incrCommand(ledisClient *c);
static void decrCommand(ledisClient *c);
static void incrbyCommand(ledisClient *c);
static void decrbyCommand(ledisClient *c);
static void selectCommand(ledisClient *c);
static void randomkeyCommand(ledisClient *c);
static void lastsaveCommand(ledisClient *c);
static void shutdownCommand(ledisClient *c);

static void renameCommand(ledisClient *c);
static void renamenxCommand(ledisClient *c);
static void moveCommand(ledisClient *c);

static void lpushCommand(ledisClient *c);
static void rpushCommand(ledisClient *c);
static void lpopCommand(ledisClient *c);
static void rpopCommand(ledisClient *c);
static void llenCommand(ledisClient *c);
static void lindexCommand(ledisClient *c);
static void lsetCommand(ledisClient *c);
static void lrangeCommand(ledisClient *c);
static void ltrimCommand(ledisClient *c);

static void typeCommand(ledisClient *c);
static void saddCommand(ledisClient *c);
static void sremCommand(ledisClient *c);
static void smoveCommand(ledisClient *c);
static void sismemberCommand(ledisClient *c);
static void scardCommand(ledisClient *c);
static void spopCommand(ledisClient *c);
static void sinterCommand(ledisClient *c);
static void sinterstoreCommand(ledisClient *c);
static void sunionCommand(ledisClient *c);
static void sunionstoreCommand(ledisClient *c);
static void sdiffCommand(ledisClient *c);
static void sdiffstoreCommand(ledisClient *c);

static void syncCommand(ledisClient *c);
static void flushdbCommand(ledisClient *c);
static void flushallCommand(ledisClient *c);
static void sortCommand(ledisClient *c);
static void lremCommand(ledisClient *c);
static void infoCommand(ledisClient *c);
static void mgetCommand(ledisClient *c);
static void monitorCommand(ledisClient *c);
static void expireCommand(ledisClient *c);

static void getSetCommand(ledisClient *c);
static void ttlCommand(ledisClient *c);
static void slaveofCommand(ledisClient *c);
static void debugCommand(ledisClient *c);

/*====================================== 全局变量 ===============================*/
static struct ledisServer server;
static struct ledisCommand cmdTable[] = {
    {"get",getCommand,2,LEDIS_CMD_INLINE},
    {"set",setCommand,3,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},
    {"setnx",setnxCommand,3,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},
    {"del",delCommand,-2,LEDIS_CMD_INLINE},
    {"exists",existsCommand,2,LEDIS_CMD_INLINE},
    {"incr",incrCommand,2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"decr",decrCommand,2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"mget",mgetCommand,-2,LEDIS_CMD_INLINE},
    {"lpush",lpushCommand,3,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},
    {"rpush",rpushCommand,3,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},
    {"lpop",lpopCommand,2,LEDIS_CMD_INLINE},
    {"rpop",rpopCommand,2,LEDIS_CMD_INLINE},
    {"llen",llenCommand,2,LEDIS_CMD_INLINE},
    {"lindex",lindexCommand,3,LEDIS_CMD_INLINE},
    {"lset",lsetCommand,4,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},
    {"lrange",lrangeCommand,4,LEDIS_CMD_INLINE},
    {"ltrim",ltrimCommand,4,LEDIS_CMD_INLINE},
    {"lrem",lremCommand,4,LEDIS_CMD_BULK},

    {"sadd",saddCommand,3,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},
    {"srem",sremCommand,3,LEDIS_CMD_BULK},
    {"smove",smoveCommand,4,LEDIS_CMD_BULK},
    {"sismember",sismemberCommand,3,LEDIS_CMD_BULK},
    {"scard",scardCommand,2,LEDIS_CMD_INLINE},
    {"spop",spopCommand,2,LEDIS_CMD_INLINE},
    {"sinter",sinterCommand,-2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"sinterstore",sinterstoreCommand,-3,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"sunion",sunionCommand,-2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"sunionstore",sunionstoreCommand,-3,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"sdiff",sdiffCommand,-2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"sdiffstore",sdiffstoreCommand,-3,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"smembers",sinterCommand,2,LEDIS_CMD_INLINE},

    {"incrby",incrbyCommand,2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"decrby",decrbyCommand,2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"getset",getSetCommand,3,LEDIS_CMD_BULK|LEDIS_CMD_DENYOOM},

    {"randomkey",randomkeyCommand,1,LEDIS_CMD_INLINE},
    {"select",selectCommand,2,LEDIS_CMD_INLINE},
    {"move",moveCommand,3,LEDIS_CMD_INLINE},
    {"rename",renameCommand,3,LEDIS_CMD_INLINE},
    {"renamenx",renamenxCommand,3,LEDIS_CMD_INLINE},
    {"expire",expireCommand,3,LEDIS_CMD_INLINE},
    {"keys",keysCommand,2,LEDIS_CMD_INLINE},
    {"dbsize",dbsizeCommand,1,LEDIS_CMD_INLINE},
    {"auth",authCommand,2,LEDIS_CMD_INLINE},
    {"ping",pingCommand,1,LEDIS_CMD_INLINE},
    {"echo",echoCommand,2,LEDIS_CMD_BULK},
    {"save",saveCommand,1,LEDIS_CMD_INLINE},
    {"bgsave",bgsaveCommand,1,LEDIS_CMD_INLINE},
    {"shutdown",shutdownCommand,1,LEDIS_CMD_INLINE},
    {"lastsave",lastsaveCommand,1,LEDIS_CMD_INLINE},
    {"type",typeCommand,2,LEDIS_CMD_INLINE},
    {"sync",syncCommand,1,LEDIS_CMD_INLINE},
    {"flushdb",flushdbCommand,1,LEDIS_CMD_INLINE},
    {"flushall",flushallCommand,1,LEDIS_CMD_INLINE},
    {"sort",sortCommand,-2,LEDIS_CMD_INLINE|LEDIS_CMD_DENYOOM},
    {"info",infoCommand,1,LEDIS_CMD_INLINE},
    {"monitor",monitorCommand,1,LEDIS_CMD_INLINE},
    {"ttl",ttlCommand,2,LEDIS_CMD_INLINE},
    {"slaveof",slaveofCommand,3,LEDIS_CMD_INLINE},
    {"debug",debugCommand,-2,LEDIS_CMD_INLINE},
    {NULL,NULL,0,0}
};

/*====================================== 工具函数 ===============================*/

/**
 * 匹配字符串，是否符合给定pattern的模式，就是个简化版正则的解析器
 * 返回1表示匹配，0则不匹配
 */ 
int stringmatchlen(const char *pattern, int patternLen,
            const char *string, int stringLen, int nocase){

    while(patternLen){
        switch(pattern[0]){
            case '*':{  //先处理通配符*
                while(pattern[1] == '*'){   //如果后面还是*，则自己算命中，直到不是*
                    pattern++;
                    patternLen--;
                }
                if(patternLen == 1) return 1;   //说明pattern全是一堆*
                while(stringLen){
                    //递归调用后面的，已经去掉了之前的*，内部只比后面的内容（后面可能还会有*）
                    if(stringmatchlen(pattern+1, patternLen-1, string, stringLen, nocase)){
                        return 1;  
                    }
                    //如果剩下的pattern不匹配，则从头开始减少string，例如mykey,ykey,key,ey,y这样的
                    string++;
                    stringLen--;
                }
                //string缩没了也没有递归匹配，就是匹配不到
                return 0; 
                break;
            }
            case '?':{  //处理单个通配符
                if(stringLen == 0)  return 0;   //string没内容
                //string直接放过
                string++;
                stringLen--;
                break;
            }
            case '[':{  //处理最复杂的或者关系（包括^）
                pattern++;
                patternLen--;   //跳过[，进来了就没意义了
                int not = pattern[0] == '^';
                if(not){    //存储了^标记，再次跳过
                    pattern++;  
                    patternLen--;
                }
                int match = 0;
                while(true){    //逐一处理[后面的部分
                    if(pattern[0] == '\\'){
                        pattern++;
                        patternLen--;   //还得跳
                        if(pattern[0] == string[0]){
                            match = 1;
                        }
                    }else if(pattern[0] == ']'){
                        break;
                    }else if(patternLen == 0){  //如果压根没有]闭合，match返回0，还要回退1格
                        pattern--;
                        patternLen++;
                        break;
                    }else if(pattern[1] == '-' && patternLen >= 3){ //还支持a-z这样的匹配
                        int start = pattern[0];
                        int end = pattern[2];
                        int c = string[0];
                        if(start > end){    //如果是z-a，则交换顺序
                            int t = start;
                            start = end;
                            end = t;
                        }
                        if(nocase){
                            start = tolower(start);
                            end = tolower(end);
                            c = tolower(c);
                        }
                        pattern += 2;
                        patternLen -= 2;
                        if(c >= start && c <= end){
                            match = 1;
                        }
                    }else{  //普通字符
                        if(nocase){
                            if(tolower((int)pattern[0]) == tolower((int)string[0])){
                                match = 1;
                            }
                        }else{
                            if(pattern[0] == string[0]){
                                match = 1;
                            }
                        }
                    }
                    pattern++;
                    patternLen--;
                }
                if(not){
                    match = !match; //取反
                }
                if(!match)  return 0;
                //到这里说明匹配到了
                string++;
                stringLen--;
                break;
            }
            case '\\':{ //处理转义符
                if(patternLen >= 2){    //后面还有，则跳过转义符，继续default，不要break
                    pattern++;
                    patternLen--;
                }
            }
            default:{   //普通字符了
                if(nocase){
                    if(tolower((int)pattern[0]) != tolower((int)string[0])){
                        return 0;
                    }
                }else{
                    if(pattern[0] != string[0]){
                        return 0;
                    }
                }
                //中了
                string++;
                stringLen--;
                break;
            }
        }
        pattern++;
        patternLen--;
        if(stringLen == 0){ //如果字符串都匹配到了，pattern还有剩余，吃了最后所有都是*的情况
            while(*pattern == '*'){ 
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    //都匹配完了，看最后结果
    if(patternLen == 0 && stringLen == 0){  //pattern和string都消耗完了说明就匹配到了
        return 1;
    }
    return 0;
}

static void ledisLog(int level, const char *fmt, ...){
    va_list ap;

    FILE *fp = server.logfile == NULL ? stdout : fopen(server.logfile, "a");
    if(!fp) return;
    
    va_start(ap, fmt);
    //尝试输出log
    if(level >= server.verbosity){
        char *c = ".-*";    //3个级别的前缀符号
        char buf[64];
        time_t now = time(NULL);
        strftime(buf, 64, "%d %b %H:%M:%S", gmtime(&now));  //特定格式化

        fprintf(fp, "%s %c", buf, c[level]);
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

static void dictLedisObjectDestructor(void *privdata, void *val){
    DICT_NOTUSED(privdata);
    decrRefCount(val);
}

/**
 * set类型里面的dict，key为lobj对象，type是sds
 */ 
static unsigned int dictSdsHash(const void *key){
    const lobj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

static int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2){
    const lobj *o1 = key1;
    const lobj *o2 = key2;
    return sdsDictKeyCompare(privdata, o1->ptr, o2->ptr);
}

dictType hashDictType = {
    dictSdsHash,    //hash函数
    NULL,                   //keyDup函数
    NULL,                   //valDup函数
    dictSdsKeyCompare,      //keyCompare函数
    dictLedisObjectDestructor,   //key清理函数
    dictLedisObjectDestructor,   //val清理函数
};

dictType setDictType = {
    dictSdsHash,    //hash函数
    NULL,                   //keyDup函数
    NULL,                   //valDup函数
    dictSdsKeyCompare,      //keyCompare函数
    dictLedisObjectDestructor,   //key清理函数（看不懂，怎么又sds了）
    NULL,   //val清理函数（压根就没有val）
};

/*========================= server相关实现 ===============================*/

/**
 * 关闭所有超时的客户端，在别的文件里调用，不能是static函数
 */ 
static void closeTimedoutClients(void){
    time_t now = time(NULL);
    listNode *ln;
    ledisClient *c;
    listRewind(server.clients);
    while((ln = listYield(server.clients)) != NULL){
        c = listNodeValue(ln);
        //检查上次交互的时间，slave客户端以及master端都没有timeout
        if(!(c->flags & LEDIS_SLAVE) && 
            !(c->flags & LEDIS_MASTER) && 
            now - c->lastinteraction > server.maxidletime){
            ledisLog(LEDIS_DEBUG, "Closing idle client");
            //关闭client
            freeClient(c);
        }
    }
}

/**
 * 再次提取成更小的函数，因为2个dict都要resize
 */ 
static int htNeedsResize(dict *dict){
    long long size = dictSlots(dict);
    long long used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE && 
            (used * 100 / size < LEDIS_HT_MINFILL));
}

/**
 * 尝试数据db和expires的resize
 */ 
static void tryResizeHashTables(void){
    //尝试收缩每个HT
    for(int i = 0; i < server.dbnum; i++){
        if(htNeedsResize(server.db[i].dict)){
            ledisLog(LEDIS_DEBUG, "The hash table %d is too sparse, resize it...", i);
            dictResize(server.db[i].dict);
            ledisLog(LEDIS_DEBUG, "Hash table %d resized.", i);
        }
        if(htNeedsResize(server.db[i].expires)){
            dictResize(server.db[i].expires);
        }
    }
}

/**
 * timeEvent的回调函数，此版本是每隔1秒执行一次，但不是精确的
 */ 
static int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData){
    
    //此版本所有入参都没用到
    LEDIS_NOTUSED(eventLoop);
    LEDIS_NOTUSED(id);
    LEDIS_NOTUSED(clientData);

    //重新赋值堆内存使用量
    server.usedmemory = zmalloc_used_memory();

    int loops = server.cronloops++; //当前循环次数
    //尝试收缩每个HT
    for(int i = 0; i < server.dbnum; i++){
        //获取总量和实际使用量
        long long size = dictSlots(server.db[i].dict);
        long long used = dictSize(server.db[i].dict);
        long long vkeys = dictSize(server.db[i].expires);
        //差不多每5秒尝试输出一下上述状态
        if(!(loops % 5) && (used || vkeys)){
            ledisLog(LEDIS_DEBUG, "DB %d: %lld keys (%lld volatile) in %lld slots HT.", i, used, vkeys, size);
            //dictPrintStats(server.dict)
        }
    }    

    //在bgsave中resize也可以，但是fork的子进程是有copy-on-write特性的
    //因此在子进程save过程中，主进程如果改变了数据结构，会影响子进程效率
    if(!server.bgsaveinprogress) tryResizeHashTables();

    //每5秒输出一次状态
    if(!(loops % 5)){
        ledisLog(LEDIS_DEBUG, "%d clients connected (%d slaves), %zu bytes in use", 
                listLength(server.clients)-listLength(server.slaves), 
                listLength(server.slaves),
                server.usedmemory,
                dictSize(server.sharingpool));  //参数对应不上，最后一个没用到
    }

    //差不多每10秒，尝试清理超时的client
    if(server.maxidletime && !(loops % 10)){
        closeTimedoutClients();
    }

    //尝试bgsave
    if(server.bgsaveinprogress){
        //如果正在bgsave，则阻塞
        int statloc;
        //非阻塞式wait
        if(wait4(-1, &statloc, WNOHANG, NULL)){
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = WIFSIGNALED(statloc);
            if(!bysignal && exitcode == 0){  //成功完成
                ledisLog(LEDIS_NOTICE, "Background saving terminated with success");
                server.dirty = 0;
                server.lastsave = time(NULL);
            }else if(!bysignal && exitcode != 0){   //exitcode不为0
                ledisLog(LEDIS_WARNING, "Background saving error");
            }else{  //exitcode为0,但bysignal不为0
                ledisLog(LEDIS_WARNING, "Background saving terminated by signal");
                ldbRemoveTempFile(server.bgsavechildpid);
            }
            server.bgsaveinprogress = 0;    //只有在这里还原标记
            server.bgsavechildpid = -1;
            //通知所有等待bgsave的从们
            updateSlavesWaitingBgsave(exitcode == 0 ? LEDIS_OK : LEDIS_ERR);
        }
    }else{
        //检测是否需要bgsave
        time_t now = time(NULL);
        for(int j = 0; j < server.saveparamslen; j++){
            struct saveparam *sp = server.saveparams+j;
            if(server.dirty >= sp->changes && now - server.lastsave > sp->seconds){
                ledisLog(LEDIS_NOTICE, "%d changes in %d seconds. Saving...",
                            sp->changes, sp->seconds);
                ldbSaveBackground(server.dbfilename);
                break;
            }
        }
    }

    //删除有限数目的expires字典中的数据
    for(int j = 0; j < server.dbnum; j++){
        ledisDb *db = server.db+j;  //取当前下标的db，因为是指针写法
        int num = dictSize(db->expires);
        if(num){    //expires的dict有值
            time_t now = time(NULL);
            if(num > LEDIS_EXPIRELOOKUPS_PER_CRON){
                num = LEDIS_EXPIRELOOKUPS_PER_CRON; //最多处理有限个
            }
            while(num--){
                dictEntry *de;
                if((de = dictGetRandomKey(db->expires)) == NULL) break; //删干净了直接退出
                time_t t = (time_t)dictGetEntryVal(de);
                if(now > t){    //还要判断里面的是否真过期了
                    deleteKey(db, dictGetEntryKey(de));
                }
            }
        }
    }

    //如果是从服务端，还要检查与主的同步，只会运行1次，同步成功以后状态会变成CONNECTED
    if(server.replstate == LEDIS_REPL_CONNECT){
        ledisLog(LEDIS_NOTICE, "Connecting to MASTER...");
        if(syncWithMaster() == LEDIS_OK){
            ledisLog(LEDIS_NOTICE, "MASTER <-> SLAVE sync succeeded");
        }
    }

    return 1000;
}

/**
 * 创建需要的常量obj们
 */ 
static void createSharedObjects(void){
    shared.crlf = createObject(LEDIS_STRING, sdsnew("\r\n"));
    shared.ok = createObject(LEDIS_STRING, sdsnew("+OK\r\n"));
    shared.err = createObject(LEDIS_STRING, sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(LEDIS_STRING, sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(LEDIS_STRING, sdsnew(":0\r\n"));
    shared.cone = createObject(LEDIS_STRING, sdsnew(":1\r\n"));
    shared.nullbulk = createObject(LEDIS_STRING, sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(LEDIS_STRING, sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(LEDIS_STRING, sdsnew("*0\r\n"));
    
    shared.pong = createObject(LEDIS_STRING, sdsnew("+PONG\r\n"));

    shared.wrongtypeerr = createObject(LEDIS_STRING, sdsnew("-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(LEDIS_STRING, sdsnew("-ERR no such key\r\n"));
    shared.syntaxerr = createObject(LEDIS_STRING, sdsnew("-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(LEDIS_STRING, sdsnew("-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(LEDIS_STRING, sdsnew("-ERR index out of range\r\n"));

    shared.space = createObject(LEDIS_STRING, sdsnew(" "));
    shared.colon = createObject(LEDIS_STRING, sdsnew(":"));
    shared.plus = createObject(LEDIS_STRING, sdsnew("+"));

    shared.select0 = createStringObject("select 0\r\n", 10);
    shared.select1 = createStringObject("select 1\r\n", 10);
    shared.select2 = createStringObject("select 2\r\n", 10);
    shared.select3 = createStringObject("select 3\r\n", 10);
    shared.select4 = createStringObject("select 4\r\n", 10);
    shared.select5 = createStringObject("select 5\r\n", 10);
    shared.select6 = createStringObject("select 6\r\n", 10);
    shared.select7 = createStringObject("select 7\r\n", 10);
    shared.select8 = createStringObject("select 8\r\n", 10);
    shared.select9 = createStringObject("select 9\r\n", 10);
}

/**
 * 给server的saveparams增加一项可能性
 */ 
static void appendServerSaveParams(time_t seconds, int changes){
    server.saveparams = zrealloc(server.saveparams, sizeof(struct saveparam)*(server.saveparamslen+1));
    if(server.saveparams == NULL)   oom("appendServerSaveParams");
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

/**
 * 重置server的saveparams全局结构
 */ 
static void ResetServerSaveParams(){
    zfree(server.saveparams);
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
    server.bindaddr = NULL;
    server.glueoutputbuf = 1;
    server.daemonize = 0;
    server.pidfile = "/var/run/ledis.pid";
    server.dbfilename = "dump.ldb";
    server.requirepass = NULL;
    server.shareobjects = 0;
    server.maxclients = 0;
    server.maxmemory = 0;
    ResetServerSaveParams();
    //给默认的配置
    appendServerSaveParams(60*60, 1);   //1小时后要达到1次变动即save
    appendServerSaveParams(300, 100);   //5分钟后要达到100次变动即save
    appendServerSaveParams(60, 10000);  //1分钟后要达到10000次变动即save
    //主从相关配置
    server.isslave = 0;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.replstate = LEDIS_REPL_NONE;
}

/**
 * 初始化server结构自身功能，只会在启动时调用
 */ 
static void initServer(){

    //忽略hup和pipe信号的默认行为（终止server进程）
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSigSegvAction();

    server.clients = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.objfreelist = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    //初始化db数组，只是分配了dbnum个地址空间，并没有为实际dict结构分配内存
    server.db = zmalloc(sizeof(ledisDb)*server.dbnum);
    server.sharingpool = dictCreate(&setDictType, NULL);
    server.sharingpoolsize = 1024;
    if(!server.db || !server.clients || !server.slaves || !server.monitors || !server.el || !server.objfreelist){
        oom("server initialization");
    }
    server.fd = anetTcpServer(server.neterr, server.port, server.bindaddr);
    if(server.fd == -1){
        ledisLog(LEDIS_WARNING, "Opening TCP port: %s", server.neterr);
        exit(EXIT_FAILURE);
    }
    //printf("server.fd=%d\n", server.fd);
    //继续给dict数组内部真实分配
    for(int i = 0; i < server.dbnum; i++){
        server.db[i].dict = dictCreate(&hashDictType, NULL);
        server.db[i].expires = dictCreate(&setDictType, NULL);  //expires的dict只有key没有val
        server.db[i].id = i;
    }
    server.cronloops = 0;
    server.bgsaveinprogress = 0;
    server.bgsavechildpid = -1;
    server.lastsave = time(NULL);
    server.dirty = 0;
    server.usedmemory = 0;
    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_starttime = time(NULL);
    aeCreateTimeEvent(server.el, 1000, serverCron, NULL, NULL);
}

/**
 * 清空server的所有dict
 */ 
static long long emptyDb(){

    long long removed = 0;
    for(int i = 0; i < server.dbnum; i++){
        removed += dictSize(server.db[i].dict);
        dictEmpty(server.db[i].dict);
        dictEmpty(server.db[i].expires);
    }
    return removed;
}

/**
 * 工具函数，将配置文件里的yes和no转化为数字
 * 既不是yes或no，返回-1
 */ 
static int yesnotoi(char *s){
    if(!strcasecmp(s, "yes")){
        return 1;
    }else if(!strcasecmp(s, "no")){
        return 0;
    }else{
        return -1;
    }
}

/**
 * 从配置文件中加载，此版本实现的比较低端，只在启动时被调用一次
 */ 
static void loadServerConfig(char *filename){
    //FILE *fp = fopen(filename, "r");
    FILE *fp;
    char buf[LEDIS_CONFIGLINE_MAX+1], *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if(filename[0] == '-' && filename[1] == '\0'){
        fp = stdin;
    }else{
        if((fp = fopen(filename, "r")) == NULL){
            ledisLog(LEDIS_WARNING, "Fatal error, can't open config file");
            exit(EXIT_FAILURE);
        }
    }

    if(!fp){
        ledisLog(LEDIS_WARNING, "Fatal error, can't open config file");
        exit(EXIT_FAILURE);
    }

    //开始按行读取，fgets额外会存储换行符\n
    while(fgets(buf, LEDIS_CONFIGLINE_MAX+1, fp) != NULL){
        sds *argv;  //配置项数组，成对儿出现
        int argc;   //配置项元素数目，比如2个（key和value），可以用来检测配置合法

        linenum++;
        line = sdsnew(buf);
        line = sdstrim(line, " \t\r\n");
        //跳过注释和空白行
        if(line[0] == '#' || line[0] == '\0'){
            sdsfree(line);
            continue;
        }
        //argv要free
        argv = sdssplitlen(line, sdslen(line), " ", 1, &argc);
        sdstolower(argv[0]);

        //开始实施这一行的配置
        if(!strcasecmp(argv[0], "timeout") && argc == 2){
            server.maxidletime = atoi(argv[1]);
            if(server.maxidletime < 0){
                err = "Invalid timeout value";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "port") && argc == 2){
            server.port = atoi(argv[1]);
            if(server.port < 1 || server.port > 65535){
                err = "Invalid port";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "bind") && argc == 2){
            server.bindaddr = zstrdup(argv[1]);
        }else if(!strcasecmp(argv[0], "save") && argc == 3){
            int seconds = atoi(argv[1]);
            int changes = atoi(argv[2]);
            if(seconds < 1 || changes < 0){
                err = "Invalid save parameters";
                goto loaderr;
            }
            appendServerSaveParams(seconds, changes);
        }else if(!strcasecmp(argv[0], "dir") && argc == 2){
            if(chdir(argv[1]) == -1){
                ledisLog(LEDIS_WARNING, "Can't chdir to '%s': %s", argv[1], strerror(errno));
                exit(EXIT_FAILURE);
            }
        }else if(!strcasecmp(argv[0], "loglevel") && argc == 2){
            if(!strcasecmp(argv[1], "debug")){
                server.verbosity = LEDIS_DEBUG;
            }else if(!strcasecmp(argv[1], "notice")){
                server.verbosity = LEDIS_NOTICE;
            }else if(!strcasecmp(argv[1], "warning")){
                server.verbosity = LEDIS_WARNING;
            }else{
                err = "Invalid log level. Must be one of debug, notice, warning";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "logfile") && argc == 2){
            FILE *logfp;
            server.logfile = zstrdup(argv[1]);   //需要复制字符串，因为argv是函数范围
            if(!strcasecmp(server.logfile, "stdout")){
                zfree(server.logfile);
                server.logfile = NULL;
            }
            if(server.logfile){
                //如果不是stdout，则需要在这里测试一下文件的可读写性
                logfp = fopen(server.logfile, "a");
                if(logfp == NULL){
                    //为啥不用sscanf
                    err = sdscatprintf(sdsempty(), 
                        "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(logfp);
            }
        }else if(!strcasecmp(argv[0], "databases") && argc == 2){
            //dbnum的值在这里可能变了，但是dict数组已经在之前已经分配16组空间了，疑似BUG
            //loadConfig操作已在initServer之前执行，所以上面提到的BUG解除
            server.dbnum = atoi(argv[1]);
            if(server.dbnum < 1){
                err = "Invalid number of databases";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "maxclients") && argc == 2){
            server.maxclients = atoi(argv[1]);
        }else if(!strcasecmp(argv[0], "maxmemory") && argc == 2){
            server.maxmemory = atoi(argv[1]);
        }else if(!strcasecmp(argv[0], "slaveof") && argc ==3){
            server.masterhost = sdsnew(argv[1]);
            server.masterport = atoi(argv[2]);
            server.replstate = LEDIS_REPL_CONNECT;  //说明是从
        }else if(!strcasecmp(argv[0], "glueoutputbuf") && argc == 2){
            if((server.glueoutputbuf = yesnotoi(argv[1])) == -1){
                err = "argument must be 'yes' or 'no'";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "shareobjects") && argc == 2){
            if((server.shareobjects = yesnotoi(argv[1])) == -1){
                err = "argument must be 'yes' or 'no'";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "shareobjectspoolsize") && argc == 2){
            server.sharingpoolsize = atoi(argv[1]);
            if(server.sharingpoolsize < 1){
                err = "invalid object sharing pool size";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "daemonize") && argc == 2){
            if((server.daemonize = yesnotoi(argv[1])) == -1){
                err = "argument must be 'yes' or 'no'";
                goto loaderr;
            }
        }else if(!strcasecmp(argv[0], "requirepass") && argc == 2){
            server.requirepass = zstrdup(argv[1]);
        }else if(!strcasecmp(argv[0], "pidfile") && argc == 2){
            server.pidfile = zstrdup(argv[1]);
        }else if(!strcasecmp(argv[0], "dbfilename") && argc == 2){
            server.dbfilename = zstrdup(argv[1]);
        }else{
            err = "Bad directive or wrong number of arguments";
            goto loaderr;
        }
        //追加清理argv
        for(int i = 0; i < argc; i++){
            sdsfree(argv[i]);
        }
        zfree(argv);
        sdsfree(line);
    }
    if(fp != stdin) fclose(fp);
    return;

loaderr:{
    fprintf(stderr, "\n***FATAL CONFIG FILE ERROR ***\n");
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(EXIT_FAILURE);
}
}

/**
 * 清理client的参数，只处理argc和argv字段
 */ 
static void freeClientArgv(ledisClient *c){
    for(int i = 0; i < c->argc; i++){
        decrRefCount(c->argv[i]);
    }
    c->argc = 0;
}

/**
 * 清理server.clients里面特定的元素
 */ 
static void freeClient(ledisClient *c){
    //从el中清理相关的fileEvent
    aeDeleteFileEvent(server.el, c->fd, AE_READABLE);
    aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
    
    //清理client各个字段
    sdsfree(c->querybuf);
    listRelease(c->reply);
    freeClientArgv(c);
    close(c->fd);
    //最后就可以删除cliens链表的结点了
    listNode *ln = listSearchKey(server.clients, c);
    assert(ln != NULL);
    listDelNode(server.clients, ln);
    if(c->flags & LEDIS_SLAVE){
        //如果对应主正在发送bulkDB，则fd也要关闭
        if(c->replstate == LEDIS_REPL_SEND_BULK && c->repldbfd != -1){
            close(c->repldbfd);
        }
        list *l = (c->flags & LEDIS_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l, c);
        assert(ln != NULL);
        listDelNode(l, ln);
    }
    //如果客户端是主服务端反向连接过来的
    if(c->flags & LEDIS_MASTER){
        server.master = NULL;
        server.replstate = LEDIS_REPL_CONNECT;
    }
    zfree(c->argv); //变成动态分配了，也得free
    zfree(c);
}

/**
 * 将client的reply链表统一输出
 */ 
static void glueReplyBuffersIfNeeded(ledisClient *c){

    int totlen = 0;
    listNode *ln;
    lobj *o;
    
    listRewind(c->reply);
    while((ln = listYield(c->reply))){
        o = ln->value;
        totlen += sdslen(o->ptr);
        if(totlen > 1024) return;   //写死了
    }
    //开始发送
    if(totlen > 0){
        char buf[1024];
        int copylen = 0;
        listRewind(c->reply);   //重复调用就可以重置迭代器了
        while((ln = listYield(c->reply))){
            o = ln->value;
            memcpy(buf+copylen, o->ptr, sdslen(o->ptr));
            copylen += sdslen(o->ptr);
            listDelNode(c->reply, ln);
        }
        o = createObject(LEDIS_STRING, sdsnewlen(buf, totlen));
        //因为一些BUG, 不能在这里直接发送，而且封装成单个的obj，再塞回c->reply中
        if(!listAddNodeTail(c->reply, o)) oom("listAddNodeTail");
        //addReplySds(c, sdsnewlen(buf, totlen));
    }
}

static void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask){

    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);

    ledisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    lobj *o;
   
    //先尝试glue所有输出，glue只做整合，本身不输出了
    if(server.glueoutputbuf && listLength(c->reply) > 1){
        glueReplyBuffersIfNeeded(c);
    }

    while(listLength(c->reply)){
        o = listNodeValue(listFirst(c->reply));
        objlen = sdslen(o->ptr);    //实际字符串长度
        if(objlen == 0){
            listDelNode(c->reply, listFirst(c->reply));
            continue;
        }

        if(c->flags & LEDIS_MASTER){    //如果是主服务端反向来的，不真返回只模拟，因为主库不需要这些消息
            nwritten = objlen - c->sentlen;
        }else{
            //开始写入client，分段写入，并没有用到anet.c里面的anetWrite函数
            nwritten = write(fd, ((char*)o->ptr) + c->sentlen, objlen - c->sentlen);
            //printf("nwritten=%d\n", nwritten);
            if(nwritten <= 0)   break;
        }
        c->sentlen += nwritten;
        totwritten += nwritten;
        //检查是否写完了，可能因为网络或者client问题，造成部分写入，则进入新的循环再写剩下的（sentlen保存了已写的字节数）
        if(c->sentlen == objlen){
            //说明写完一个reply了
            listDelNode(c->reply, listFirst(c->reply));
            c->sentlen = 0;
        }

        //最大64k
        if(totwritten > LEDIS_MAX_WRITE_PER_EVENT) break;
        //printf("reply over.\n");
    }
    if(nwritten == -1){
        if(errno == EAGAIN){
            nwritten = 0;
        }else{
            ledisLog(LEDIS_DEBUG, "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
    if(totwritten > 0)  c->lastinteraction = time(NULL);
    if(listLength(c->reply) == 0){
        c->sentlen = 0;
        aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
    }
}

/**
 * 根据传来的命令名称，找在静态命令结构里找特定的命令结构
 */ 
static struct ledisCommand *lookupCommand(char *name){
    int i = 0;
    while(cmdTable[i].name != NULL){
        if(!strcasecmp(name, cmdTable[i].name)){
            return &cmdTable[i];
        }
        i++;
    }
    return NULL;
}

/**
 * 重置client结构，清理argc,argv以及bulklen字段
 */ 
static void resetClient(ledisClient *c){
    freeClientArgv(c);
    c->bulklen = -1;
}

/**
 * 尝试运行实际命令，可以延迟性处理bulk命令，或者一次性处理inline命令
 * 返回1说明client还存活，0说明已失联
 */ 
static int processCommand(ledisClient *c){

    if(server.maxmemory) freeMemoryIfNeeded();

    if(!strcasecmp(c->argv[0]->ptr, "quit")){
        freeClient(c);
        return 0;
    }

    struct ledisCommand *cmd = lookupCommand(c->argv[0]->ptr);

    if(!cmd){
        addReplySds(c, sdsnew("-ERR unknown command\r\n"));
        resetClient(c);
        return 1;
    }else if((cmd->arity > 0 && cmd->arity != c->argc) || 
                (-cmd->arity > c->argc)){    //arity可以是负数了（代表参数必须大于arity），但也和argc有限制关系
        addReplySds(c, sdsnew("-ERR wrong number of arguments\r\n"));
        resetClient(c);
        return 1;
    }else if(server.maxmemory && cmd->flags & LEDIS_CMD_DENYOOM && zmalloc_used_memory() > server.maxmemory){
        addReplySds(c, sdsnew("-ERR command not allowed when used memory > 'maxmemory'\r\n"));
        resetClient(c);
        return 1;
    }else if(cmd->flags == LEDIS_CMD_BULK && c->bulklen == -1){
        int bulklen = atoi(c->argv[c->argc-1]->ptr); //获取后面bulk数据的长度
        decrRefCount(c->argv[c->argc-1]);    //转成int原来的就没用了
        if(bulklen < 0 || bulklen > 1024*1024*1024){
            c->argc--;
            addReplySds(c, sdsnew("-ERR invalid bulk write count\r\n"));
            resetClient(c);
            return 1;
        }
        //清理
        c->argc--;
        c->bulklen = bulklen + 2;   //bulk数据后面还有\r\n要计算，解析时会被跳过
        //检查querybuf里有没有bulk数据，这个不一定有，没有则要退回readQueryFromClient的again中
        if((signed)sdslen(c->querybuf) >= c->bulklen){
            //有则填充最后一个argv，用真正的bulk参数
            c->argv[c->argc] = createStringObject(c->querybuf, c->bulklen - 2);  //bulk实际数据不包括\r\n
            c->argc++;
            c->querybuf = sdsrange(c->querybuf, c->bulklen, -1);    //包括bulk数据和\r\n一起跳过
        }else{
            return 1;
        }
    }

    //尝试利用shareobjects池子
    if(server.shareobjects){
        for(int i = 1; i < c->argc; i++){
            c->argv[i] = tryObjectSharing(c->argv[i]);
        }
    }

    //检查是否通过了auth
    if(server.requirepass && !c->authenticated && cmd->proc != authCommand){
        addReplySds(c, sdsnew("-ERR operation not permitted\r\n"));
        resetClient(c);
        return 1;
    }

    //运行命令，可是inline的，也可以是bulk类型的
    long long dirty = server.dirty;
    cmd->proc(c);
    //如果配置了从，而且cmd发生了数据变动，则触发从同步
    if(server.dirty - dirty != 0 && listLength(server.slaves)){
        replicationFeedSlaves(server.slaves, cmd, c->db->id, c->argv, c->argc);
    }
    if(listLength(server.monitors)){
        replicationFeedSlaves(server.monitors, cmd, c->db->id, c->argv, c->argc);
    }
    server.stat_numcommands++;

    //如果是普通类型，用完一次就彻底清理，不需要等待内置timeout了，也不用reset了
    if(c->flags & LEDIS_CLOSE){
        freeClient(c);
        return 0;
    }
    resetClient(c);
    return 1;
}

/**
 * 在主库运行cmd之后尝试触发
 */ 
static void replicationFeedSlaves(list *slaves, struct ledisCommand *cmd, int dictid, lobj **argv, int argc){
    listNode *ln;
    lobj **outv;    //变成了动态
    lobj *static_outv[LEDIS_STATIC_ARGS*2+1];   //够大了
    int outc = 0;

    if(argc <= LEDIS_STATIC_ARGS){
        outv = static_outv;
    }else{
        outv = zmalloc(sizeof(lobj*)*(argc*2+1));
        if(!outv) oom("replicationFeedSlaves");
    }

    //将argv的obj数组，放到outv数组里面，用空格分开，注意每个元素（空格，换行符）都是一个单独的obj元素
    for(int i = 0; i < argc; i++){
        if(i != 0) outv[outc++] = shared.space;
        if((cmd->flags & LEDIS_CMD_BULK) && i == argc-1){   //特殊处理bulk类型
            lobj *lenobj = createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "%d\r\n", sdslen(argv[i]->ptr)));
            lenobj->refcount = 0;   //应该只是为了明确表示lenobj是局部变量
            outv[outc++] = lenobj;
        }
        outv[outc++] = argv[i];
    }
    outv[outc++] = shared.crlf;

    //统一增加1次计数，结尾再减1次，不明白具体原因
    for(int j = 0; j < outc; j++){
        incrRefCount(outv[j]);
    }
    listRewind(slaves);
    while((ln = listYield(slaves))){
        ledisClient *slave = ln->value; //不用宏了？
        
        //不发给状态为等待bgsave的从
        if(slave->replstate == LEDIS_REPL_WAIT_BGSAVE_START) continue;

        if(slave->slaveseldb != dictid){    //先尝试让从切到和主一致的库
            lobj *selectcmd;
            switch(dictid){
                //只是为了快
                case 0: selectcmd = shared.select0; break;
                case 1: selectcmd = shared.select1; break;
                case 2: selectcmd = shared.select2; break;
                case 3: selectcmd = shared.select3; break;
                case 4: selectcmd = shared.select4; break;
                case 5: selectcmd = shared.select5; break;
                case 6: selectcmd = shared.select6; break;
                case 7: selectcmd = shared.select7; break;
                case 8: selectcmd = shared.select8; break;
                case 9: selectcmd = shared.select9; break;
                default:{
                    selectcmd = createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "select %d\r\n", dictid));
                    selectcmd->refcount = 0;
                    break;
                }
                addReply(slave, selectcmd);
                slave->slaveseldb = dictid;
            }
        }
        //传输实际命令
        for(int j = 0; j < outc; j++){
            addReply(slave, outv[j]);
        }
    }
    for(int j = 0; j < outc; j++){
        decrRefCount(outv[j]);
    }
    if(outv != static_outv) zfree(outv);
}

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask){
    //此版本只需要使用fd和privdata
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);
    //printf("readQueryFromClient\n");
    ledisClient *c = (ledisClient *)privdata;
    char buf[LEDIS_IOBUF_LEN];

    int nread = read(fd, buf, LEDIS_IOBUF_LEN);
    //printf("nread=%d\n", nread);
    if(nread == -1){
        if(errno == EAGAIN){
            nread = 0;
        }else{
            ledisLog(LEDIS_DEBUG, "Reading from client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }else if(nread == 0){   //说明客户端关闭了
        ledisLog(LEDIS_DEBUG, "Client closed connection");
        freeClient(c);
        return;
    }
    if(nread){  //总之真读到了数据才继续
        c->querybuf = sdscatlen(c->querybuf, buf ,nread);   //是往里面追加，所以可能有多组命令
        //printf("%s", c->querybuf);
        c->lastinteraction = time(NULL);
    }else{
        return;
    }

    //开始处理query缓冲区的数据
again:
    if(c->bulklen == -1){   //第一次都会进入这样，不管是inline还是bulk类型
        char *p = strchr(c->querybuf, '\n');
        size_t querylen;
        if(p){
            sds query = c->querybuf;
            c->querybuf = sdsempty();   //直接清空，注意这里querybuf已经指向了全新的结构，可以对query进行free了
            querylen = 1+(p-(query));   //即例如set mykey 7\r\n的总长度
            if(sdslen(query) > querylen){
                //说明是bulk类型，后面还有实际数据，或者后面有新命令
                c->querybuf = sdscatlen(c->querybuf, query+querylen, sdslen(query)-querylen);
            }
            //处理querybuf得到的命令（不止1条）
            *p = '\0';  //query弄成set mykey 7\0\0
            if(*(p-1) == '\r'){
                *(p-1) = '\0';
            }
            //必须重新调整query
            sdsupdatelen(query);

            //终于可以解析了
            if(sdslen(query) == 0){
                sdsfree(query);
                return;
            }
            int argc;
            sds *argv = sdssplitlen(query, sdslen(query), " ", 1, &argc);
            if(argv == NULL) oom("sdssplitlen");
            sdsfree(query); //再见了，c->querybuf已经指向新的结构了
            
            //c->argv已经是动态的了
            if(c->argv) zfree(c->argv);
            c->argv = zmalloc(sizeof(lobj*)*argc);
            if(c->argv == NULL) oom("allocating argument list for client");


            for(int i = 0; i < argc; i++){
                if(sdslen(argv[i])){
                    //终于把命令弄进去了
                    c->argv[c->argc] = createObject(LEDIS_STRING, argv[i]);
                    c->argc++;
                }else{
                    sdsfree(argv[i]);   //不赋值，则必须立刻回收
                }
            }
            zfree(argv); //split调用者还要负责回收
            //开始执行客户端命令，如果还有bulk数据或者后面还有新命令，还会回来继续判断
            if(processCommand(c) && sdslen(c->querybuf))    goto again;
        }else if(sdslen(c->querybuf) >= LEDIS_REQUEST_MAX_SIZE){
            ledisLog(LEDIS_DEBUG, "Client protocol error");
            freeClient(c);
            return;
        }
    }else{  //如果bulk数据不是一起来的，在下一次才来，就会进入这里直接处理
        int qbl = sdslen(c->querybuf);  
        if(c->bulklen <= qbl){  //querybuf里面的实际数据长度，至少要大于上次bulklen才对
            c->argv[c->argc] = createStringObject(c->querybuf, c->bulklen - 2);  //填充bulk数据
            c->argc++;
            c->querybuf = sdsrange(c->querybuf, c->bulklen, -1);
            processCommand(c);
            return;
        }
    }
}

/**
 * 将客户端结构定位相应的dict表索引
 */ 
static int selectDb(ledisClient *c, int id){
    if(id < 0 || id >= server.dbnum){
        return LEDIS_ERR;
    }
    c->db = &server.db[id];
    return LEDIS_OK;
}

/**
 * 就是引用+1
 */ 
static void *dupClientReplyValue(void *o){
    incrRefCount((lobj*)o);
    return 0;   //为啥？
}

static ledisClient *createClient(int fd){
    ledisClient *c = zmalloc(sizeof(*c));

    anetNonBlock(NULL, fd); //开启非阻塞
    anetTcpNoDelay(NULL, fd);   //开启TCP_NODELAY
    if(!c)  return NULL;
    selectDb(c, 0); //将客户端对接0号表
    //初始化各个字段
    c->fd = fd;
    c->querybuf = sdsempty();
    c->argc = 0;
    c->argv = NULL; //变成了动态
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    c->authenticated = 0;
    c->replstate = LEDIS_REPL_NONE;
    if((c->reply = listCreate()) == NULL)   oom("listCreate");
    listSetFreeMethod(c->reply, decrRefCount);
    listSetDupMethod(c->reply, dupClientReplyValue);
    //将client的fd也加入到eventLoop中（存在2种fd，监听新请求/建立起来的客户端后续请求）
    if(aeCreateFileEvent(server.el, fd, AE_READABLE, readQueryFromClient, c, NULL) == AE_ERR){
        freeClient(c);
        return NULL;
    }
    //添加到server.clients的尾部
    if(!listAddNodeTail(server.clients, c)) oom("listAddNodeTail");
    return c;
}

static void addReply(ledisClient *c, lobj *obj){
    //client如果是从，则必须是ONLINE状态才能去回复，否则说明在为了从而进行bgsave，只会往c->reply追加暂存而不会真的返回
    if(listLength(c->reply) == 0 && 
        (c->replstate == LEDIS_REPL_NONE ||
         c->replstate == LEDIS_REPL_ONLINE) &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE, 
        sendReplyToClient, c, NULL) == AE_ERR){
        return;
    }
    if(!listAddNodeTail(c->reply, obj)) oom("listAddNodeTail");
    incrRefCount(obj);  //引用数+1,因为又跑到c->reply里面了
}

static void addReplySds(ledisClient *c, sds s){
    lobj *o = createObject(LEDIS_STRING, s);
    addReply(c, o);
    decrRefCount(o);    //引用数-1,应该还剩1,通过c->reply来指向
}

static void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask){

    //此版本只需要用fd参数
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);
    LEDIS_NOTUSED(privdata);

    char cip[1025];  //写死的，不太好
    ledisClient *c;
    int cport;
    //printf("sdf=%d\n", fd);
    int cfd = anetAccept(server.neterr, fd, cip, &cport);
    //printf("cdf=%d\n", cfd);
    if(cfd == AE_ERR){
        ledisLog(LEDIS_DEBUG, "Accepting client connection: %s", server.neterr);
        return;
    }
    //得到客户端fd了
    ledisLog(LEDIS_DEBUG, "Accepted %s:%d", cip, cport);
    //根据得到的fd创建client结构
    if((c = createClient(cfd)) == NULL){
        ledisLog(LEDIS_WARNING, "Error allocating resoures for the client");
        close(cfd); //状态此时不一定
        return;
    }

    //检测是否超出了maxclients限制
    if(server.maxclients && 
        listLength(server.clients) > server.maxclients){
        char *err = "-ERR max number of clients reached\r\n";
        //直接在这里用write回复
        (void) write(c->fd, err, strlen(err));
        freeClient(c);
        return;
    }

    server.stat_numconnections++;
    //printf("createClient is ok!\n");
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
        o = zmalloc(sizeof(*o));
    }
    if(!o)  oom("createObject");
    //初始化其他字段
    o->type = type;
    o->ptr = ptr;
    o->refcount = 1;
    return o;
}

/**
 * 创建一个sds类型的对象
 */ 
static lobj *createStringObject(char *ptr, size_t len){
    return createObject(LEDIS_STRING, sdsnewlen(ptr, len));
}

/**
 * 创建一个list类型的对象
 */ 
static lobj *createListObject(void){
    list *l = listCreate();
    if(!l)  oom("listCreate");
    listSetFreeMethod(l, decrRefCount); //重要一步，所有的obj都要绑定decrRefCount函数
    return createObject(LEDIS_LIST, l);
}

/**
 * 创建一个set类型的对象，内部是个只有key的dict
 */ 
static lobj *createSetObject(void){
    dict *d = dictCreate(&setDictType, NULL);
    if(!d) oom("dictCreate");
    return createObject(LEDIS_SET, d);
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
    dictRelease((dict*)o->ptr);
}

/**
 * 释放hash类型的obj
 */ 
static void freeHashObject(lobj *o){
    dictRelease((dict*)o->ptr);
}

/**
 * 给obj的ref引用计数加1
 */ 
static void incrRefCount(lobj *o){
    o->refcount++;
#ifdef DEBUG_REFCOUNT
    if(o->type == LEDIS_STRING){
        printf("Increment '%s' (%p), now is: %d\n", o->ptr, o, o->refcount);
    }
#endif
}

/**
 * 给obj的ref引用计数减1,如果变成0,则调用相应类型的free
 */ 
static void decrRefCount(void *obj){
    lobj *o = obj;
#ifdef DEBUG_REFCOUNT
    if(o->type == LEDIS_STRING){
        printf("Decrement '%s' (%p), now is: %d\n", o->ptr, o, o->refcount-1);
    }
#endif
    if(--(o->refcount) == 0){
        switch(o->type){
            case LEDIS_STRING : freeStringObject(o);    break;
            case LEDIS_LIST : freeListObject(o);    break;
            case LEDIS_SET : freeSetObject(o);    break;
            case LEDIS_HASH : freeHashObject(o);    break;
            default : assert(0 != 0);   break;
        }
        //引用为0,只是free里面的ptr数据，obj本身会加入到free列表首部，等待复用
        if(listLength(server.objfreelist) > LEDIS_OBJFREELIST_MAX || 
            !listAddNodeHead(server.objfreelist, o)){
            zfree(o);
        }
    }
}

/**
 * 尝试使用sharedobj池子共享obj，注意只能是SDS类型的obj
 */ 
static lobj *tryObjectSharing(lobj *o){

    unsigned long c;
    if(o == NULL || server.shareobjects == 0) return o;
    assert(o->type == LEDIS_STRING);
    dictEntry *de = dictFind(server.sharingpool, o);
    if(de){ //池子里有则复用，用里面的
        lobj *shared = dictGetEntryKey(de);
        //使用次数+1
        c = ((unsigned long)dictGetEntryVal(de))+1;
        dictGetEntryVal(de) = (void*)c;
        incrRefCount(shared);
        decrRefCount(o);
        return shared;  //直接返回
    }else{
        if(dictSize(server.sharingpool) >= server.sharingpoolsize){  //如果用完了，则尝试
            de = dictGetRandomKey(server.sharingpool);
            assert(de != NULL);
            //随机找出一个key，然后使用次数-1
            c = ((unsigned long)dictGetEntryVal(de))-1;
            dictGetEntryVal(de) = (void*)c;
            if(c == 0){ //尝试delete
                dictDelete(server.sharingpool, de->key);
            }
        }else{
            c = 0;
        }
    }
    if(c == 0){
        int retval = dictAdd(server.sharingpool, o, (void*)1);
        assert(retval == DICT_OK);
        incrRefCount(o);
    }
    return o;
}

/**
 * 便利函数，根据key的obj，直接获取val的obj
 */ 
static lobj *lookupKey(ledisDb *db, lobj *key){
    dictEntry *de = dictFind(db->dict, key);
    return de ? dictGetEntryVal(de) : NULL;
}

static lobj *lookupKeyRead(ledisDb *db, lobj *key){
    expireIfNeeded(db, key);
    return lookupKey(db, key);
}

static lobj *lookupKeyWrite(ledisDb *db, lobj *key){
    deleteIfVolatile(db, key);
    return lookupKey(db, key);
}

/**
 * 利用dictDelete，清理db的dict和expires
 */ 
static int deleteKey(ledisDb *db, lobj *key){
    //要对key进行保护，但是不明白
    incrRefCount(key);
    //先删除expires
    if(dictSize(db->expires)) dictDelete(db->expires, key);
    int retval = dictDelete(db->dict, key);
    decrRefCount(key);
    return retval == DICT_OK;
}

/*====================================== DB SAVE/LOAD相关 ===============================*/

/**
 * 写saveType（即存储数据的类型，占1个字节）
 */ 
static int ldbSaveType(FILE *fp, unsigned char type){
    if(fwrite(&type, 1, 1, fp) == 0) return -1;
    return 0;
}

/**
 * 写存储的时间（占固定4个字节，没法压缩）
 */ 
static int ldbSaveTime(FILE *fp, time_t t){
    //必须用定长的数据类型
    int32_t t32 = (int32_t)t;
    if(fwrite(&t32, 4, 1, fp) == 0) return -1;
    return 0;
}

/**
 * 存储数据的长度（1字节6位/2字节14位/4字节32位）
 */ 
static int ldbSaveLen(FILE *fp, uint32_t len){
    unsigned char buf[2];   //存储长度类型
    if(len < (1<<6)){   //1左移6位即1000000，即64
        //小于64，只用6位存储即可
        buf[0] = (len&0xFF)|(LEDIS_LDB_6BITLEN<<6); //只用1个字节即可，里面的计算其实是多余的，因为标识本身就是00
        //只写1个字节
        if(fwrite(buf, 1, 1, fp) == 0) return -1;
    }else if(len < (1<<14)){    //即小于16384，只用右面14位存储，标识位为01
        //len右移8位，相当于只留高6位，并存储在单字节的低6位空间，然后高2位追加标识位01
        buf[0] = ((len>>8)&0xFF)|(LEDIS_LDB_14BITLEN<<6);
        buf[1] = len&0xFF;  //len的低8位原样存储
        //写2个字节
        if(fwrite(buf, 2, 1, fp) == 0) return -1;
    }else{  //大于16384，直接用32位空间存储
        buf[0] = (LEDIS_LDB_32BITLEN<<6);   //将标识位10写入最高2位
        //直接写1个字节，buf[0]只用了高2位，后6位都是0
        if(fwrite(buf, 1, 1, fp) == 0) return -1;
        len = htonl(len);   //要转网络序，有啥用，还想文件跨平台啊？
        //直接写就完了，参数本来就是32位的
        if(fwrite(&len, 4, 1, fp) == 0) return -1;
    }
    return 0;
}

/**
 * 将s中的字符串，判断是不是纯数字（例如123或-1234）如果是，则根据位数转存到enc数组里（静态5个字节），位数不同占用空间不同
 * 返回值为enc填充了的字节数
 */ 
static int ldbTryIntegerEncoding(sds s, unsigned char *enc){
    
    char *endptr, buf[32];
    long long value = strtoll(s, &endptr, 10);
    if(endptr[0] != '\0') return 0; //s不是字符串
    snprintf(buf, 32, "%lld", value);   //将带符号的数字转回字符串

    if(strlen(buf) != sdslen(s) || memcmp(buf, s, sdslen(s))) return 0; //再次检查s和buf必须是同一个数值字符串

    if(value >= -(1<<7) && value <= (1<<7)-1){  //value属于[-128, 127]
        enc[0] = (LEDIS_LDB_ENCVAL<<6)|LEDIS_LDB_ENC_INT8;  //即11000000
        enc[1] = value&0xFF;    //即实际的value
        return 2;
    }else if(value >= -(1<<15) && value <= (1<<15)-1){  //value属于[-32768, 32767]
        enc[0] = (LEDIS_LDB_ENCVAL<<6)|LEDIS_LDB_ENC_INT16;  //即11000001
        enc[1] = value&0xFF;    //先存低位了又。。
        enc[2] = (value>>8)&0xFF;   //再存高位
        return 3;
    }else if(value >= -((long long)1<<31) && value <= ((long long)1<<31)-1){ 
        enc[0] = (LEDIS_LDB_ENCVAL<<6)|LEDIS_LDB_ENC_INT32;  //即11000010
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    }else{
        return 0;
    }
}

/**
 * 利用lzf算法压缩obj里的字符串（长度必须大于20才能进来），然后写入fp
 */ 
static int ldbSaveLzfStringObject(FILE *fp, lobj *obj){

    void *out;
    unsigned int outlen = sdslen(obj->ptr)-4; 
    if(outlen <= 0) return 0;
    if((out = zmalloc(outlen+1)) == NULL) return 0;
    unsigned int comprlen = lzf_compress(obj->ptr, sdslen(obj->ptr), out, outlen);
    if(comprlen == 0){  //压缩失败
        zfree(out);
        return 0;
    }
    //压缩成功
    unsigned char byte = (LEDIS_LDB_ENCVAL<<6)|LEDIS_LDB_ENC_LZF;   //为11000011
    if(fwrite(&byte, 1, 1, fp) == 0) goto writeerr;
    if(ldbSaveLen(fp, comprlen) == -1) goto writeerr;   //压缩后的长度
    if(ldbSaveLen(fp, sdslen(obj->ptr)) == -1) goto writeerr;   //实际的长度
    if(fwrite(out, comprlen, 1, fp) == 0) goto writeerr;    //压缩后的实际数据
    zfree(out);
    return comprlen;
writeerr:
    zfree(out);
    return -1;
}

/**
 * 存储一个字符串类型的obj
 * 返回0说明成功，-1说明有问题
 */ 
static int ldbSaveStringObject(FILE *fp, lobj *obj){

    size_t len = sdslen(obj->ptr);
    int enclen;
    if(len <= 11){  //如果字符串小于等于11位，则尝试转成整数存储（但肯定不一定是）
        unsigned char buf[5];
        if((enclen = ldbTryIntegerEncoding(obj->ptr, buf)) > 0){
            if(fwrite(buf, enclen, 1, fp) == 0) return -1;
            return 0;
        }
    }

    //不是整数，或者字符串长度过大
    if(1 && len > 20){   //长度大于20，才用LZF压缩算法，不然一样没效率
        int retval = ldbSaveLzfStringObject(fp, obj);
        if(retval == -1) return -1;
        if(retval > 0) return 0;    //成功了
    }

    //到这里，说明即不是合理的整数，长度又不是特别大，则普通存储
    if(ldbSaveLen(fp, len) == -1) return -1;
    if(len && fwrite(obj->ptr, len, 1, fp) == 0) return -1;
    return 0;
}

static int ldbSave(char *filename){

    dictIterator *di = NULL;
    dictEntry *de;
    char tmpfile[256];
    time_t now = time(NULL);
    //建立临时文件
    snprintf(tmpfile, 256, "temp-%d.ldb", (int)getpid());

    FILE *fp = fopen(tmpfile, "w");
    if(!fp){
        ledisLog(LEDIS_WARNING," Failed saving the DB: %s", strerror(errno));
        return LEDIS_ERR;
    }
    //写死固定的开头标识字符
    if(fwrite("LEDIS0001", 9, 1, fp) == 0) goto werr;
    
    for(int i = 0; i < server.dbnum; i++){
        ledisDb *db = server.db+i;
        dict *d = db->dict;
        if(dictSize(d) == 0) continue;
        di = dictGetIterator(d);
        if(!di){    //为啥不goto werr
            fclose(fp);
            return LEDIS_ERR;
        }

        //写当前DB的序号
        if(ldbSaveType(fp, LEDIS_SELECTDB) == -1) goto werr;
        //写序号本身，利用了整数压缩
        if(ldbSaveLen(fp, i) == -1) goto werr;

        //利用迭代器，遍历当前DB的每个entry
        while((de = dictNext(di)) != NULL){
            lobj *key = dictGetEntryKey(de);
            lobj *o = dictGetEntryVal(de);
            time_t expiretime = getExpire(db, key);
            
            if(expiretime != -1){   //有expiretime则也要存储
                //如果过期了，就不存了
                if(expiretime < now) continue;
                if(ldbSaveType(fp, LEDIS_EXPIRETIME) == -1) goto werr;  //写type
                if(ldbSaveTime(fp, expiretime) == -1) goto werr;    //写实际time
            }
            
            //存key和val
            if(ldbSaveType(fp, o->type) == -1) goto werr;   //存val的type，key不用存type，因为肯定是SDS
            if(ldbSaveStringObject(fp, key) == -1) goto werr;   //写入key
            //根据不同类型，写val信息
            if(o->type == LEDIS_STRING){
                if(ldbSaveStringObject(fp, o) == -1) goto werr;
            }else if(o->type == LEDIS_LIST){
                list *list = o->ptr;
                listNode *ln;
                listRewind(list);
                if(ldbSaveLen(fp, listLength(list)) == -1) goto werr;
                while((ln = listYield(list))){
                    lobj *eleobj = listNodeValue(ln);
                    if(ldbSaveStringObject(fp, eleobj) == -1) goto werr;
                }
            }else if(o->type == LEDIS_SET){
                dict *set = o->ptr;
                dictIterator *di = dictGetIterator(set);
                dictEntry *de;

                if(!set) oom("dictGetIterator");
                if(ldbSaveLen(fp, dictSize(set)) == -1) goto werr;
                while((de = dictNext(di)) != NULL){
                    lobj *eleobj = dictGetEntryKey(de);
                    if(ldbSaveStringObject(fp, eleobj) == -1) goto werr;
                }
                dictReleaseIterator(di);
            }else{
                //只能是上面3种
                assert(0 != 0);
            }
        }
        dictReleaseIterator(di);
    }
    
    //结尾标识
    if(ldbSaveType(fp, LEDIS_EOF) == -1) goto werr;
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    //临时文件生成完毕，要重命名为正式的文件名
    if(rename(tmpfile, filename) == -1){
        ledisLog(LEDIS_WARNING, "Error moving temp DB file on the final destination: %s"), strerror(errno);
        unlink(tmpfile);
        return LEDIS_ERR;
    }

    ledisLog(LEDIS_NOTICE, "DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    return LEDIS_OK;

werr:   //统一清理
    fclose(fp);
    unlink(tmpfile);    //也得删
    ledisLog(LEDIS_WARNING, "Error saving DB on disk: %s", strerror(errno));
    if(di)  dictReleaseIterator(di);
    return LEDIS_ERR;
}

static int ldbSaveBackground(char *filename){
    if(server.bgsaveinprogress) return LEDIS_ERR;
    pid_t childpid;
    if((childpid = fork()) == 0){
        //子进程
        close(server.fd);   //不需要这个衍生品
        if(ldbSave(filename) == LEDIS_OK){
            exit(EXIT_SUCCESS);
        }else{
            exit(EXIT_FAILURE);
        }
    }else{
        //主进程
        if(childpid == -1){ //说明fork出错了
            ledisLog(LEDIS_WARNING, "Can't save in background: fork: %s", strerror(errno));
            return LEDIS_ERR;
        }
        ledisLog(LEDIS_NOTICE, "Background saving started by pid %d", childpid);
        server.bgsaveinprogress = 1;    //在serverCron中才能得到子进程完毕的事件
        server.bgsavechildpid = childpid;
        return LEDIS_OK;
    }
    return LEDIS_OK;    //到不了这里
}

/**
 * 删除临时文件
 */ 
static void ldbRemoveTempFile(pid_t childpid){
    char tmpfile[256];
    snprintf(tmpfile, 256, "temp-%d.ldb", (int)childpid);
    unlink(tmpfile);
}

static int ldbLoadType(FILE *fp){
    unsigned char type;
    if(fread(&type, 1, 1, fp) == 0) return -1;
    return type;
}

static time_t ldbLoadTime(FILE *fp){
    int32_t t32;
    if(fread(&t32, 4, 1, fp) == 0) return -1;
    return (time_t)t32;
}

static uint32_t ldbLoadLen(FILE *fp, int ldbver, int *isencoded){
    unsigned char buf[2];
    uint32_t len;
    if(isencoded) *isencoded = 0;
    if(ldbver == 0){    //直接取定长32位
        if(fread(&len, 4, 1, fp) == 0) return LEDIS_LDB_LENERR;
        return ntohl(len);
    }else{
        if(fread(buf, 1, 1, fp) == 0) return LEDIS_LDB_LENERR;
        int type = (buf[0]&0xC0)>>6;    //0xC0为11000000
        if(type == LEDIS_LDB_6BITLEN){
            return buf[0]&0x3F; //0x3F为00111111
        }else if(type == LEDIS_LDB_ENCVAL){
            if(isencoded) *isencoded = 1;   //打标记
            return buf[0]&0x3F; //其实只要最后2位
        }else if(type == LEDIS_LDB_14BITLEN){
            if(fread(buf+1, 1, 1, fp) == 0) return LEDIS_LDB_LENERR;
            return ((buf[0]&0x3F)<<8)|buf[1];
        }else{  //32bit
            if(fread(&len, 4, 1, fp) == 0) return LEDIS_LDB_LENERR;
            return ntohl(len);
        }
    }
}

/**
 * 根据传来的字符串类型（8位/16位/32位）直接读取的转换成相应的整数
 */ 
static lobj *ldbLoadIntegerObject(FILE *fp, int enctype){
    unsigned char enc[4];
    long long val;
    
    if(enctype == LEDIS_LDB_ENC_INT8){
        if(fread(enc, 1, 1, fp) == 0) return NULL;
        val = (signed char)enc[0];
    }else if(enctype == LEDIS_LDB_ENC_INT16){
        if(fread(enc, 2, 1, fp) == 0) return NULL;
        uint16_t v = enc[0]|enc[1]<<8;
        val = (int16_t)v;
    }else if(enctype == LEDIS_LDB_ENC_INT32){
        if(fread(enc, 4, 1, fp) == 0) return NULL;
        uint32_t v = enc[0]|enc[1]<<8|enc[2]<<16|enc[3]<<24;
        val = (int32_t)v;
    }else{
        val = 0;    //随便给个值，避免编译时出现值未初始化的警告
        assert(0!=0);   //直接异常
    }
    return createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "%lld", val));
}

/**
 * 直接读取压缩过的字符串，并解压返回
 */ 
static lobj *ldbLoadLzfStringObject(FILE *fp, int ldbver){

    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;
    //压缩后的长度
    if((clen = ldbLoadLen(fp, ldbver, NULL)) == LEDIS_LDB_LENERR) return NULL;
    //原始长度
    if((len = ldbLoadLen(fp, ldbver, NULL)) == LEDIS_LDB_LENERR) return NULL;
    if((c = zmalloc(clen)) == NULL) goto err;   //存压缩后的字符串
    if((val = sdsnewlen(NULL, len)) == NULL) goto err;  //存解压缩后字符串
    if(fread(c, clen, 1, fp) == 0) goto err;
    if(lzf_decompress(c, clen, val, len) == 0) goto err;
    zfree(c);
    return createObject(LEDIS_STRING, val);

err:
    zfree(c);
    sdsfree(val);
    return NULL;
}

static lobj *ldbLoadStringObject(FILE *fp, int ldbver){
    int isencoded;
    uint32_t len = ldbLoadLen(fp, ldbver, &isencoded);
    if(isencoded){
        switch(len){
            case LEDIS_LDB_ENC_INT8:
            case LEDIS_LDB_ENC_INT16:
            case LEDIS_LDB_ENC_INT32:
                return tryObjectSharing(ldbLoadIntegerObject(fp, len)); //将整型干回字符串
            case LEDIS_LDB_ENC_LZF:
                return tryObjectSharing(ldbLoadLzfStringObject(fp, ldbver));
            default:
                assert(0!=0);
        }
    }

    //说明既不是整型字符串，也不是LZF压缩字符串，是普通的不加工类型
    if(len == LEDIS_LDB_LENERR) return NULL;
    sds val = sdsnewlen(NULL, len);
    if(len && fread(val, len, 1, fp) == 0){
        sdsfree(val);
        return NULL;
    }
    return tryObjectSharing(createObject(LEDIS_STRING, val));
}

static int ldbLoad(char *filename){

    lobj *keyobj = NULL;
    uint32_t dbid;
    
    int type, retval;
    dict *d = server.db[0].dict;
    ledisDb *db = server.db+0;
    char buf[1024];
    time_t expiretime = -1, now = time(NULL);

    FILE *fp = fopen(filename, "r");
    if(!fp) return LEDIS_ERR;
    //验证文件签名
    if(fread(buf, 9, 1, fp) == 0)   goto eoferr;
    buf[9] = '\0';
    if(memcmp(buf, "LEDIS", 5) != 0){   //只验证前5个字符串，后4个字符串是版本号
        fclose(fp);
        ledisLog(LEDIS_WARNING, "Wrong signature trying to load DB from file");
        return LEDIS_ERR;
    }
    //验证文件版本，要么0要么1
    int ldbver = atoi(buf+5);   //转换后4个字节
    if(ldbver > 1){
        fclose(fp);
        ledisLog(LEDIS_WARNING, "Can't handle LDB format version %d", ldbver);
        return LEDIS_ERR;
    }

    while(true){
        lobj *o;

        //获取type，根据不同的type做不同的操作
        if((type = ldbLoadType(fp)) == -1) goto eoferr;
        if(type == LEDIS_EXPIRETIME){
            if((expiretime = ldbLoadTime(fp)) == -1) goto eoferr;
            //expiretimeType后面肯定还是其它type
            if((type = ldbLoadType(fp)) == -1) goto eoferr;
        }
        if(type == LEDIS_EOF) break;    //eof说明读完了
        //处理选择数据库
        if(type == LEDIS_SELECTDB){
            if((dbid = ldbLoadLen(fp, ldbver, NULL)) == LEDIS_LDB_LENERR) goto eoferr;
            if(dbid >= (unsigned)server.dbnum){
                ledisLog(LEDIS_WARNING, "FATAL: Data file was created with a Ledis server compilied to handle more than %d databases. Exiting\n", server.dbnum);
                exit(EXIT_FAILURE);
            }
            db = server.db+dbid;
            d = db->dict;
            continue;
        }

        //尝试获取key
        if((keyobj = ldbLoadStringObject(fp, ldbver)) == NULL) goto eoferr;
        //处理val
        if(type == LEDIS_STRING){
            if((o = ldbLoadStringObject(fp, ldbver)) == NULL) goto eoferr;
        }else if(type == LEDIS_LIST || type == LEDIS_SET){
            uint32_t listlen;
            if((listlen = ldbLoadLen(fp, ldbver, NULL)) == LEDIS_LDB_LENERR) goto eoferr;
            o = (type == LEDIS_LIST) ? createListObject() : createSetObject();
            while(listlen--){
                lobj *ele;
                if((ele = ldbLoadStringObject(fp, ldbver)) == NULL) goto eoferr;
                //添加到list或set的dict中
                if(type == LEDIS_LIST){
                    if(!listAddNodeTail((list*)o->ptr, ele)) oom("listAddNodeTail");
                }else{  //否则是SET
                    if(dictAdd((dict*)o->ptr, ele, NULL) == DICT_ERR) oom("dictAdd");
                }
            }
        }else{
            assert(0 != 0);
        }

        //lobj生成了，还需要弄到dict里
        retval = dictAdd(d, keyobj, o);
        if(retval == DICT_ERR){
            ledisLog(LEDIS_WARNING, "Loading DB, duplicated key (%s) found! Unrecoverable error, exiting now.", keyobj->ptr);
            exit(EXIT_FAILURE);
        }

        //恢复expire设置
        if(expiretime != -1){
            setExpire(db, keyobj, expiretime); 
            //已经超时了，就直接删掉 
            if(expiretime < now) deleteKey(db, keyobj); //冗余操作，先放后删？
            expiretime = -1;
        }
        //清理
        keyobj = NULL;
        o = NULL;
    }

    fclose(fp);
    return LEDIS_OK;

eoferr:
    //并没有调用fclose(fp)？？？
    if(keyobj) decrRefCount(keyobj);
    ledisLog(LEDIS_WARNING, "Short read or OOM loading DB. unrecoverable error, exiting now.");
    exit(EXIT_FAILURE); //load失败就直接退出
    return LEDIS_ERR;   //到不了，函数必须要返回1个值而已
}

/*====================================== 各种命令实现 ===============================*/

static void authCommand(ledisClient *c){
    if(!server.requirepass || !strcmp(c->argv[1]->ptr, server.requirepass)){
        c->authenticated = 1;   //已登录
        addReply(c, shared.ok);
    }else{
        c->authenticated = 0;   //未登录
        addReply(c, shared.err);
    }
}

static void pingCommand(ledisClient *c){
    addReply(c, shared.pong);
}

static void echoCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", (int)sdslen(c->argv[1]->ptr)));
    addReply(c, c->argv[1]);
    addReply(c, shared.crlf);
    //已经被reply内部指向，要变成裸指针，防止被free
    c->argv[1] = NULL;
}

/*====================================== 字符串相关 ===============================*/

static void setGenericCommand(ledisClient *c, int nx){
   
    int retval = dictAdd(c->db->dict, c->argv[1], c->argv[2]);
    if(retval == DICT_ERR){
        if(!nx){    //如果是setCommand，直接覆盖原来的val
            dictReplace(c->db->dict, c->argv[1], c->argv[2]);
            incrRefCount(c->argv[2]); 
        }else{  //如果是setnxCommand，则撤销val
            addReply(c, shared.czero);
            return;
        }
    }else{
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    }
    server.dirty++;
    removeExpire(c->db, c->argv[1]);    //撤销原key上的expiretime
    addReply(c, nx ? shared.cone : shared.ok);
}

static void setCommand(ledisClient *c){
    setGenericCommand(c, 0);
}

static void setnxCommand(ledisClient *c){
    setGenericCommand(c, 1);
}

static void getCommand(ledisClient *c){
    lobj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.nullbulk);
    }else{
        if(o->type != LEDIS_STRING){
            addReply(c, shared.wrongtypeerr);
        }else{
            //正常情况
            addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", (int)sdslen(o->ptr)));
            addReply(c, o);
            addReply(c, shared.crlf);
        }
    }
}

static void getSetCommand(ledisClient *c){
    getCommand(c);  //直接返回旧val
    if(dictAdd(c->db->dict, c->argv[1], c->argv[2]) == DICT_ERR){
        //改val不需要操作key的引用数
        dictReplace(c->db->dict, c->argv[1], c->argv[2]);
    }else{
        //如果是add的，则key要引用+1
        incrRefCount(c->argv[1]);
    }
    incrRefCount(c->argv[2]);
    server.dirty++;
    //expires数据直接清理，因为val变了
    removeExpire(c->db, c->argv[1]);
}

static void mgetCommand(ledisClient *c){
    //至少2个参数，每个key一定会返回值
    addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", c->argc-1));
    for(int i = 1; i < c->argc; i++){
        lobj *o = lookupKeyRead(c->db, c->argv[i]);
        if(o == NULL){
            addReply(c, shared.nullbulk);
        }else{
            //只支持SDS类型的val
            if(o->type != LEDIS_STRING){
                addReply(c, shared.nullbulk);
            }else{  
                addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(o->ptr)));
                addReply(c, o);
                addReply(c, shared.crlf);
            }
        }
    }
}

static void incrDecrCommand(ledisClient *c, long long incr){
    
    long long value;
    lobj *o = lookupKey(c->db, c->argv[1]);
    if(o == NULL){
        value = 0;  //没有key则将val为0
    }else{
        if(o->type == LEDIS_STRING){
            //如果原来的val是字符串，强制转成ll
            char *endp;
            value = strtoll(o->ptr, &endp, 10);
        }else{
            value = 0;  //val不是sds，强制改成0，而不是返回错误之类的
        }
    }

    value += incr;//自增或者自减
    lobj *obj = createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "%lld", value));
    int retval = dictAdd(c->db->dict, c->argv[1], o);
    if(retval == DICT_ERR){
        //如果存在key，则直接替换
        dictReplace(c->db->dict, c->argv[1], obj);
        removeExpire(c->db, c->argv[1]);
    }else{
        incrRefCount(c->argv[1]);
    }
    server.dirty++; //无论如何都会改变
    addReply(c, shared.colon);
    addReply(c, obj);
    addReply(c, shared.crlf);
}

static void incrCommand(ledisClient *c){
    incrDecrCommand(c, 1);
}

static void decrCommand(ledisClient *c){
    incrDecrCommand(c, -1);
}

static void incrbyCommand(ledisClient *c){
    long long incr = strtoll(c->argv[2]->ptr, NULL, 10);
    incrDecrCommand(c, incr);
}

static void decrbyCommand(ledisClient *c){
    long long incr = strtoll(c->argv[2]->ptr, NULL, 10);
    incrDecrCommand(c, -incr);
}

/*====================================== 其他相关 ===============================*/

static void delCommand(ledisClient *c){
    
    int deleted = 0;
    for(int i = 1; i < c->argc; i++){
        if(deleteKey(c->db, c->argv[i])){
            server.dirty++;
            deleted++;
        }
    }
    switch(deleted){
        case 0:{
            addReply(c, shared.czero); break;
        }
        case 1:{
            addReply(c, shared.cone); break;
        }
        default:{
            addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", deleted)); break;
        }
    }

}

static void existsCommand(ledisClient *c){
    
    addReply(c, lookupKeyRead(c->db, c->argv[1]) ? shared.cone : shared.czero);
}

static void selectCommand(ledisClient *c){
    int id = atoi(c->argv[1]->ptr);
    if(selectDb(c, id) == LEDIS_OK){
        addReply(c, shared.ok);
        addReply(c, shared.crlf);
    }else{
        addReplySds(c, sdsnew("-ERR invalid DB index\r\n"));
    }
}

static void randomkeyCommand(ledisClient *c){
    dictEntry *de;
    
    while(true){
        de = dictGetRandomKey(c->db->dict);
        //过期了就会尝试再继续找，直到遇到不过期的key
        if(!de || expireIfNeeded(c->db, dictGetEntryKey(de)) == 0) break;
    }
        
    if(de){
        addReply(c, shared.plus);
        addReply(c, dictGetEntryKey(de));
        addReply(c, shared.crlf);
    }else{
        addReply(c, shared.plus);
        addReply(c, shared.crlf);
    }
}

static void keysCommand(ledisClient *c){
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern);
    int numkeys = 0, keyslen = 0;
    lobj *lenobj = createObject(LEDIS_STRING, NULL);

    dictIterator *di = dictGetIterator(c->db->dict);
    if(!di) oom("dictGetIterator");

    addReply(c, lenobj);    //最后还会改里面的值，先占位
    decrRefCount(lenobj);

    //遍历dict，寻找匹配的key
    dictEntry *de;
    while((de = dictNext(di)) != NULL){
        lobj *keyobj = dictGetEntryKey(de);
        sds key = keyobj->ptr;
        if((pattern[0] == '*' && pattern[1] == '\0') || 
            stringmatchlen(pattern, plen, key, sdslen(key), 0)){
            if(expireIfNeeded(c->db, keyobj) == 0){ //还得不过期
                //匹配则加入结果字符串中，并用空格分隔
                if(numkeys != 0) addReply(c, shared.space);
                addReply(c, keyobj);
                numkeys++;
                keyslen += sdslen(key);
            }
        }
    }
    dictReleaseIterator(di);
    //为啥数目和长度要加一起
    lenobj->ptr = sdscatprintf(sdsempty(), "$%lu\r\n", keyslen+(numkeys ? (numkeys-1) : 0));
    addReply(c, shared.crlf);
}

static void dbsizeCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%:lu\r\n", dictSize(c->db->dict)));
}

static void lastsaveCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%:lu\r\n", server.lastsave));
}

static void typeCommand(ledisClient *c){
    
    char *type;
    lobj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL){
        type = "+none";
    }else{
        switch(o->type){
            case LEDIS_STRING: type = "+string"; break;
            case LEDIS_LIST: type = "+list"; break;
            case LEDIS_SET: type = "+set"; break;
            default: type = "unknown"; break;
        }
    }
    addReplySds(c, sdsnew(type));
    addReply(c, shared.crlf);
}

static void saveCommand(ledisClient *c){

    if(server.bgsaveinprogress){
        addReplySds(c, sdsnew("-ERR background save in progress\r\n"));
        return;
    }
    if(ldbSave(server.dbfilename) == LEDIS_OK){
        addReply(c, shared.ok);
    }else{
        addReply(c, shared.err);
    }
}

static void bgsaveCommand(ledisClient *c){

    if(server.bgsaveinprogress){
        addReplySds(c, sdsnew("-ERR background save already in progress\r\n"));
        return;
    }
    if(ldbSaveBackground(server.dbfilename) == LEDIS_OK){
        addReply(c, shared.ok);
    }else{
        addReply(c, shared.err);
    }
}

static void shutdownCommand(ledisClient *c){
    
    //如果正在bgsave，强制干掉这个子进程
    if(server.bgsaveinprogress){
        ledisLog(LEDIS_WARNING, "There is a live saving child. Killing it!");
        kill(server.bgsavechildpid, SIGKILL);
        ldbRemoveTempFile(server.bgsavechildpid);
    }
    
    ledisLog(LEDIS_WARNING, "User requested shutdown, saving DB...");
    if(ldbSave(server.dbfilename) == LEDIS_OK){
        //删除pid文件
        if(server.daemonize){
            unlink(server.pidfile);
        }
        ledisLog(LEDIS_WARNING, "%zu bytes used at exit", zmalloc_used_memory());
        ledisLog(LEDIS_NOTICE, "server exit now, bye bye...");
        exit(EXIT_SUCCESS);
    }else{
        ledisLog(LEDIS_WARNING, "Error trying to save the DB, can't exit");
        addReplySds(c, sdsnew("-ERR can't quit, problems saving the DB\r\n"));
    }
}

static void renameGenericCommand(ledisClient *c, int nx){
    
    //新旧key不能一样
    if(sdscmp(c->argv[1]->ptr, c->argv[2]->ptr) == 0){
        addReply(c, shared.sameobjecterr);
        return;
    }

    lobj *o = lookupKeyWrite(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.nokeyerr);
        return;
    }
    incrRefCount(o);    //被弄到新的key里（引用加1），原来的val后面会被delete（引用减1）
    deleteIfVolatile(c->db, c->argv[2]);
    //尝试add
    if(dictAdd(c->db->dict, c->argv[2], o) == DICT_ERR){
        if(nx){
            //存在key则放弃
            decrRefCount(o);
            addReply(c, shared.czero);
            return;
        }else{
            dictReplace(c->db->dict, c->argv[2], o);
        }
    }else{
        incrRefCount(c->argv[2]);
    }
    deleteKey(c->db, c->argv[1]);
    server.dirty++;
    addReply(c, nx ? shared.cone : shared.ok);
}

static void renameCommand(ledisClient *c){
    renameGenericCommand(c, 0);
}
static void renamenxCommand(ledisClient *c){
    renameGenericCommand(c, 1);
}

static void moveCommand(ledisClient *c){
    ledisDb *src = c->db;    //备份指向
    int srcid = c->db->id;
    if(selectDb(c, atoi(c->argv[2]->ptr)) == LEDIS_ERR){
        addReply(c, shared.outofrangeerr);
        return;
    }
    ledisDb *dst = c->db;    //新的DB指向
    selectDb(c, srcid); //指回去

    if(src == dst){ //不能一样
        addReply(c, shared.sameobjecterr);
        return;
    }

    lobj *o = lookupKeyWrite(c->db, c->argv[1]);    //那不就有可能没了么？
    if(!o){
        addReply(c, shared.czero);
        return;
    }

    //将key尝试放到新的DB里面
    deleteIfVolatile(dst, c->argv[1]);
    if(dictAdd(dst->dict, c->argv[1], o) == DICT_ERR){
        addReply(c, shared.czero);
        return;
    }
    incrRefCount(c->argv[1]);
    incrRefCount(o);

    //清理src的节点，但是只是移除结构并不能free，只是指向改变了而已
    deleteKey(src, c->argv[1]);
    server.dirty++;
    addReply(c, shared.cone);
}

/*====================================== LIST相关 ===============================*/

static void pushGenericCommand(ledisClient *c, int where){
    
    lobj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    list *list;
    if(lobj == NULL){
        lobj = createListObject();
        list = lobj->ptr;
        if(where == LEDIS_HEAD){
            if(!listAddNodeHead(list, c->argv[2])) oom("listAddNodeHead");
        }else{
            if(!listAddNodeTail(list, c->argv[2])) oom("listAddNodeTail");
        }
        dictAdd(c->db->dict, c->argv[1], lobj);
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    }else{
        //检查类型，必须是list
        if(lobj->type != LEDIS_LIST){
            addReply(c, shared.wrongtypeerr);
            return;
        }
        list = lobj->ptr;
        if(where == LEDIS_HEAD){
            if(!listAddNodeHead(list, c->argv[2])) oom("listAddNodeHead");
        }else{
            if(!listAddNodeTail(list, c->argv[2])) oom("listAddNodeTail");
        }
        incrRefCount(c->argv[2]);
    }
    server.dirty++;
    addReply(c, shared.ok);
}

static void lpushCommand(ledisClient *c){
    pushGenericCommand(c, LEDIS_HEAD);
}

static void rpushCommand(ledisClient *c){
    pushGenericCommand(c, LEDIS_TAIL);
}

static void llenCommand(ledisClient *c){
    lobj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.czero);
        return;
    }else{
        if(o->type == LEDIS_LIST){
            addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", listLength((list*)o->ptr)));
        }else{
            addReply(c, shared.wrongtypeerr);
            return;
        }
    }
}

static void lindexCommand(ledisClient *c){
    lobj *o = lookupKeyRead(c->db, c->argv[1]);
    int index = atoi(c->argv[2]->ptr);
    if(o == NULL){
        addReply(c, shared.nullbulk);
        return;
    }else{
        if(o->type == LEDIS_LIST){
            list *list = o->ptr;
            listNode *node = listIndex(list, index);
            if(node == NULL){
                addReply(c, shared.nullbulk);
                return;
            }else{
                lobj *ele = listNodeValue(node);
                //返回BULK类型
                addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", (int)sdslen(ele->ptr)));
                addReply(c, ele);
                addReply(c, shared.crlf);
                return;
            }
        }else{
            addReply(c, shared.nullbulk);
            return;
        }
    }
}

static void lsetCommand(ledisClient *c){
    
    int index = atoi(c->argv[2]->ptr);
    lobj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.nokeyerr);
        return;
    }else{
        if(o->type != LEDIS_LIST){
            addReply(c, shared.wrongtypeerr);
            return;
        }else{
            //是list就可以尝试set了
            list *list = o->ptr;
            listNode *ln = listIndex(list, index);
            if(ln == NULL){
                addReply(c, shared.outofrangeerr);
                return;
            }else{
                lobj *ele = listNodeValue(ln);
                decrRefCount(ele);  //删除原来的
                listNodeValue(ln) = c->argv[3];
                incrRefCount(c->argv[3]);
                addReply(c, shared.ok);
                server.dirty++;
                return;
            }
        }
    }
}

static void popGenericCommand(ledisClient *c, int where){
    
    lobj *o = lookupKeyWrite(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.nullbulk);
    }else{
       if(o->type != LEDIS_LIST){
           addReply(c, shared.wrongtypeerr);
       }else{
           list *list = o->ptr;
           listNode *node;
           if(where == LEDIS_HEAD){
               node = listFirst(list);
           }else{
               node = listLast(list);
           }
           if(node == NULL){
               addReply(c, shared.nullbulk);
           }else{
               lobj *ele = listNodeValue(node);
               addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", (int)sdslen(ele->ptr)));
               addReply(c, ele);
               addReply(c, shared.crlf);
               //因为是pop操作，所以要删掉
               listDelNode(list, node);
               server.dirty++;  //结构一定发生了变化
           }
       }
    }
}

static void lpopCommand(ledisClient *c){
    popGenericCommand(c, LEDIS_HEAD);
}

static void rpopCommand(ledisClient *c){
    popGenericCommand(c, LEDIS_TAIL);
}

static void lrangeCommand(ledisClient *c){
    
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);

    lobj *o = lookupKeyRead(c->db, c->argv[1]);

    if(o == NULL){
        addReply(c, shared.nullmultibulk);
        return;
    }else{
        if(o->type == LEDIS_LIST){
            list *list = o->ptr;
            int llen = listLength(list);

            //校正入参start和end
            if(start < 0) start = llen + start;
            if(end < 0) end = llen + end;
            if(start < 0) start = 0;    //start为大负数，依然会小于0
            if(end < 0) end = 0;    //end为大负数，依然会小于0

            if(start > end || start >= llen){
                addReply(c, shared.emptymultibulk);
                return;
            }
            if(end >= llen) end = llen - 1;
            int rangelen = (end - start) + 1;   //即最终截取多少个元素

            listNode *node = listIndex(list, start);
            //返回格式为多行BULK，即第一个数字是元素总个数
            addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", rangelen));
            lobj *ele;
            for(int i = 0; i < rangelen; i++){
                ele = listNodeValue(node);
                //返回BULK类型
                addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", (int)sdslen(ele->ptr)));
                addReply(c, ele);
                addReply(c, shared.crlf);
                node = node->next;
            }
            return;
        }else{
            addReply(c, shared.wrongtypeerr);
            return;
        }
    }
}

static void ltrimCommand(ledisClient *c){
  
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    lobj *o = lookupKeyWrite(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.nokeyerr);
        return;
    }else{
        if(o->type == LEDIS_LIST){
            list *list = o->ptr;
            int llen = listLength(list);
            int ltrim, rtrim;
            //校正入参start和end
            if(start < 0) start = llen + start;
            if(end < 0) end = llen + end;
            if(start < 0) start = 0;    //start为大负数，依然会小于0
            if(end < 0) end = 0;    //end为大负数，依然会小于0

            if(start > end || start >= llen){
                ltrim = llen;
                rtrim = 0;
            }else{
                if(end >= llen) end = llen - 1;
                ltrim = start;
                rtrim = llen - end - 1;
            }

            listNode *node;
            for(int i = 0; i < ltrim; i++){
                node = listFirst(list);
                listDelNode(list, node);
            }
            for(int j = 0; j < rtrim; j++){
                node = listLast(list);
                listDelNode(list, node);
            }
            server.dirty++; //只算一次变动
            addReply(c, shared.ok);
            return;
        }else{
            addReply(c, shared.wrongtypeerr);
            return;
        }
    }
}

static void lremCommand(ledisClient *c){
    
    lobj *o = lookupKeyWrite(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.czero);
        return;
    }else{
        if(o->type != LEDIS_LIST){
            addReply(c, shared.wrongtypeerr);
            return;
        }else{
            list *list = o->ptr;
            int removed = 0;
            int toremove = atoi(c->argv[2]->ptr);
            int fromtail = 0;
            if(toremove < 0){   //如果count参数为负，先变回正数，然后从尾部遍历
                toremove = -toremove;
                fromtail = 1;
            }
            //首元素准备遍历
            listNode *ln = fromtail ? list->tail : list->head;
            listNode *next; //删除用
            while(ln){
                lobj *ele = listNodeValue(ln);
                next = fromtail ? ln->prev : ln->next;
                if(sdscmp(ele->ptr, c->argv[3]->ptr) == 0){
                    listDelNode(list, ln);
                    server.dirty++;
                    removed++;
                    //如果count不为0,则删到count个元素就停止
                    if(toremove && removed == toremove) break;
                }
                ln = next;
            }
            addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", removed));
        }
    }
}

/*====================================== SET相关 ===============================*/

static void saddCommand(ledisClient *c){
    
    lobj *set = lookupKeyWrite(c->db, c->argv[1]);
    if(set == NULL){
        //没有这个key则新增，val为SET类型的obj
        set = createSetObject();
        dictAdd(c->db->dict, c->argv[1], set);  //find过了所以肯定成功
        incrRefCount(c->argv[1]);
    }else{
        //已经有key了，则追加
        if(set->type != LEDIS_SET){
            addReply(c, shared.wrongtypeerr);
            return;
        }
    }
    //只是处理好了set本身，还要处理set里面dict的key们
    if(dictAdd(set->ptr, c->argv[2], NULL) == DICT_OK){
        server.dirty++;
        addReply(c, shared.cone);
        return;
    }else{
        addReply(c, shared.czero);
        return;
    }
}

static void sremCommand(ledisClient *c){
    lobj *set = lookupKeyWrite(c->db, c->argv[1]);
    if(set == NULL){
        addReply(c, shared.czero);
        return;
    }else{
        if(set->type != LEDIS_SET){
            addReply(c, shared.wrongtypeerr);
            return;
        }else{
            //尝试删除里面dict的key（也就是单个元素）
            if(dictDelete(set->ptr, c->argv[2]) == DICT_OK){
                server.dirty++;
                //dict里数目变少了，就要尝试收缩
                if(htNeedsResize(set->ptr)) dictResize(set->ptr);
                addReply(c, shared.cone);
            }else{
                addReply(c, shared.czero);
            }
            return;
        }
    }
}

static void smoveCommand(ledisClient *c){
    
    lobj *srcset = lookupKeyWrite(c->db, c->argv[1]);
    lobj *dstset = lookupKeyWrite(c->db, c->argv[2]);
    if(srcset == NULL || srcset->type != LEDIS_SET){
        //如果src的val为NULL，则返回0，如果类型不对则返回类型错误
        addReply(c, srcset ? shared.wrongtypeerr : shared.czero);
        return;
    }
    if(dstset && dstset->type != LEDIS_SET){
        //如果dst的val不存在，也返回类型错误的信息
        addReply(c, shared.wrongtypeerr);
        return;
    }
    //删除原来set中的dict元素
    if(dictDelete(srcset->ptr, c->argv[3]) == DICT_ERR){
        addReply(c, shared.czero);  //并没有参数3指定的key
        return;
    }
    server.dirty++;
    if(!dstset){    //目标dict不存在，则先建立
        dstset = createSetObject();
        dictAdd(c->db->dict, c->argv[2], dstset);
        incrRefCount(c->argv[2]);
    }
    if(dictAdd(dstset->ptr, c->argv[3], NULL) == DICT_OK){
        incrRefCount(c->argv[3]);
    }
    addReply(c, shared.cone);
}

static void sismemberCommand(ledisClient *c){
    lobj *set = lookupKeyRead(c->db, c->argv[1]);
    if(set == NULL){
        addReply(c, shared.czero);
        return;
    }else{
        if(set->type != LEDIS_SET){
            addReply(c, shared.wrongtypeerr);
            return;
        }else{
            //查找
            if(dictFind(set->ptr, c->argv[2])){
                addReply(c, shared.cone);
            }else{
                addReply(c, shared.czero);
            }
            return;
        }
    }
}

static void scardCommand(ledisClient *c){
    
    lobj *o = lookupKeyRead(c->db, c->argv[1]);
    if(o == NULL){
        addReply(c, shared.czero);
        return;
    }else{
        if(o->type != LEDIS_SET){
            addReply(c, shared.wrongtypeerr);
            return;
        }
        dict *s = o->ptr;
        addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", dictSize(s)));
        return;
    }
}

static void spopCommand(ledisClient *c){

    dictEntry *de;
    lobj *set = lookupKeyWrite(c->db, c->argv[1]);
    if(set == NULL){
        addReply(c, shared.nullbulk);
    }else{
        if(set->type != LEDIS_SET){
            addReply(c, shared.wrongtypeerr);
            return;
        }
        de = dictGetRandomKey(set->ptr);
        if(de == NULL){
            addReply(c, shared.nullbulk);
        }else{
            lobj *ele = dictGetEntryKey(de);
            addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", sdslen(ele->ptr)));
            addReply(c, ele);
            addReply(c, shared.crlf);
            //返回了就可以删了
            dictDelete(set->ptr, ele);
            if(htNeedsResize(set->ptr)) dictResize(set->ptr);
            server.dirty++;
        }
    }
}

static int qsortCompareSetsByCardinality(const void *s1, const void *s2){
    //注意s1和s2必须是指针，所以要多一层
    dict **d1 = (void *)s1;
    dict **d2 = (void *)s2;
    return dictSize(*d1) - dictSize(*d2);
}

/**
 * 取不同SET的交集，因此c->argc至少要为3
 * 此command还兼容smember命令，即只传一个SET的KEY，让取交集的for直接跳过，从此造成交集等于自身SET的情况，并依次返回
 */ 
static void sinterGenericCommand(ledisClient *c, lobj **setskeys, int setsnum, lobj *dstkey){
    
    lobj *lenobj = NULL, *dstset = NULL;
    dict **dv = zmalloc(sizeof(dict*)*setsnum);
    if(!dv) oom("sinterGenericCommand");
    //尝试处理参数传来的每个SET
    for(int i = 0; i < setsnum; i++){
        
        lobj *setobj = dstkey ? lookupKeyWrite(c->db, setskeys[i]) :
                                lookupKeyRead(c->db, setskeys[i]);
        if(setobj == NULL){ //每个key参数必须要有效
            zfree(dv);
            if(dstkey){
                deleteKey(c->db, dstkey);
                addReply(c, shared.ok);
            }else{
                addReply(c, shared.nullmultibulk);
            }
            return;
        }
        if(setobj->type != LEDIS_SET){
            zfree(dv);
            addReply(c, shared.wrongtypeerr);
            return;
        }
        dv[i] = setobj->ptr;
    }

    //开始处理dict数组中的每个dict了
    qsort(dv, setsnum, sizeof(dict*), qsortCompareSetsByCardinality); //将dv里面的dict按总量排序
    
    if(!dstkey){    //说明不是sinterstore命令，要返回结果
        //返回类型为multi-bulk，但是现在还不知道SET最终交集的数目，所以先输出一个空字符串，在最后会修改实际值
        lenobj = createObject(LEDIS_STRING, NULL);
        addReply(c, lenobj);
        decrRefCount(lenobj);
    }else{  //是sinterstore，则要存储，这里先用空的SET占位，后面再写入交集对象
        dstset = createSetObject();
    }
    
    //开始迭代最小的SET，测试每一个在其他SET里面是否存在即可，有一个不在就不算
    dictIterator *di = dictGetIterator(dv[0]);
    if(!di) oom("dictGetIterator");

    dictEntry *de;
    int cardinality = 0;
    while((de = dictNext(di)) != NULL){
        lobj *ele;
        int j;
        //从dv的第2个元素开始比
        for(j = 1; j < setsnum; j++){
            //没找到直接退出for
            if(dictFind(dv[j], dictGetEntryKey(de)) == NULL) break;
        }
        if(j != setsnum) continue;    //如果不是最后一个SET，说明for是被break出来的，不是后面SET都有这个元素，换下一个元素尝试
        ele = dictGetEntryKey(de);
        if(!dstkey){    //如果不是sinterstore，则需要返回
            addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", sdslen(ele->ptr)));
            addReply(c, ele);
            addReply(c, shared.crlf);
            cardinality++;
        }else{  //如果是sinterstore，则直接写进之前建好的SET里
            dictAdd(dstset->ptr, ele, NULL);
            incrRefCount(ele);
            server.dirty++;
        }
    }
    dictReleaseIterator(di);
    //在这里才进行新key的存储
    if(dstkey){
        deleteKey(c->db, dstkey);   //直接删掉原来的key
        dictAdd(c->db->dict, dstkey, dstset);
        incrRefCount(dstkey);   //必须要+1，因为dstkey是参数
    }

    if(!dstkey){
        //因为是单线程的，所以向客户端write的操作要等到下一轮ae迭代了
        lenobj->ptr = sdscatprintf(sdsempty(), "*%d\r\n", cardinality);  
    }else{  //如果是sinterstore，则返回新集合的大小
        addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", 
                        dictSize((dict*)dstset->ptr)));
        server.dirty++;
    }
    zfree(dv);
}

static void sinterCommand(ledisClient *c){
    sinterGenericCommand(c, c->argv+1, c->argc-1, NULL);
}
static void sinterstoreCommand(ledisClient *c){
    sinterGenericCommand(c, c->argv+2, c->argc-2, c->argv[1]);
}

#define LEDIS_OP_UNION 0
#define LEDIS_OP_DIFF 1

/**
 * 支持超级多的命令变种（sunion/sunionstore/sdiff/sdiffstore）
 */ 
static void sunionDiffGenericCommand(ledisClient *c, lobj **setskeys, int setsnum, lobj *dstkey, int op){

    dictIterator *di;
    dictEntry *de;
    int cardinality = 0;    //结果集合的大小
    dict **dv = zmalloc(sizeof(dict*)*setsnum); //各个set的容器
    if(!dv) oom("sunionDiffGenericCommand");
    for(int i = 0; i < setsnum; i++){
        lobj *setobj = dstkey ? 
                    lookupKeyWrite(c->db, setskeys[i]) : 
                    lookupKeyRead(c->db, setskeys[i]);
        if(!setobj){
            dv[i] = NULL;
            continue;
        }
        //入参对应的数据类型不能错
        if(setobj->type != LEDIS_SET){
            zfree(dv);
            addReply(c, shared.wrongtypeerr);
            return;
        }
        dv[i] = setobj->ptr;
    }
    lobj *dstset = createSetObject();
    for(int j = 0; j < setsnum; j++){
        //如果diff，第一个SET不能为空（因为sdiff是求第1个SET和后面其他SET的差集的）
        if(op == LEDIS_OP_DIFF && j == 0 && !dv[j]) break;
        if(!dv[j]) continue;

        di = dictGetIterator(dv[j]);
        if(!di) oom("dictGetIterator");
        while((de = dictNext(di)) != NULL){
            lobj *ele = dictGetEntryKey(de);
            //不管是union和diff，第1个SET开始都是完整的，遍历从第2个SET开始才有操作区别
            if(op == LEDIS_OP_UNION || j == 0){
                if(dictAdd(dstset->ptr, ele, NULL) == DICT_OK){
                    incrRefCount(ele);
                    cardinality++;
                }
            }else if(op == LEDIS_OP_DIFF){  //从第2个SET才有可能进来
                if(dictDelete(dstset->ptr, ele) == DICT_OK){
                    cardinality--;
                }
            }
        }
        dictReleaseIterator(di);
        //对于diff，如果一轮操作结果集合已经没有元素了，后面也就不需要操作了，肯定也是空
        if(op == LEDIS_OP_DIFF && cardinality == 0) break;
    }

    if(!dstkey){
        addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", cardinality));
        di = dictGetIterator(dstset->ptr);
        if(!di) oom("dictGetIterator");
        while((de = dictNext(di)) != NULL){
            lobj *ele = dictGetEntryKey(de);
            addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", sdslen(ele->ptr)));
            addReply(c, ele);
            addReply(c, shared.crlf);
        }
        dictReleaseIterator(di);
    }else{
        deleteKey(c->db, dstkey);   //先清空已有的
        dictAdd(c->db->dict, dstkey, dstset);
        incrRefCount(dstkey);   //dstset为什么不引用+1？？？在后面还会有decrRefCount
    }

    //清理
    if(!dstkey){
        decrRefCount(dstset);
    }else{
        addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", 
                    dictSize((dict*)dstset->ptr)));
        server.dirty++;
    }
    zfree(dv);
}

static void sunionCommand(ledisClient *c){
    sunionDiffGenericCommand(c, c->argv+1, c->argc-1, NULL, LEDIS_OP_UNION);
}

static void sunionstoreCommand(ledisClient *c){
    sunionDiffGenericCommand(c, c->argv+2, c->argc-2, c->argv[1], LEDIS_OP_UNION);
}

static void sdiffCommand(ledisClient *c){
    sunionDiffGenericCommand(c, c->argv+1, c->argc-1, NULL, LEDIS_OP_DIFF);
}

static void sdiffstoreCommand(ledisClient *c){
    sunionDiffGenericCommand(c, c->argv+2, c->argc-2, c->argv[1], LEDIS_OP_DIFF);
}

static void flushdbCommand(ledisClient *c){
    server.dirty += dictSize(c->db->dict);
    //清空当前的DB
    dictEmpty(c->db->dict);
    dictEmpty(c->db->expires);
    addReply(c, shared.ok);
    //不会再save了，dirty巨变很可能直接触发bgsave
}

static void flushallCommand(ledisClient *c){
    server.dirty += emptyDb();
    addReply(c, shared.ok);
    //强制save
    ldbSave(server.dbfilename);
    server.dirty++; //啥意思？
}

static ledisSortOperation *createSortOperation(int type, lobj *pattern){
    ledisSortOperation *so = zmalloc(sizeof(*so));
    if(!so) oom("createSortOperation");
    so->type = type;
    so->pattern = pattern;
    return so;
}

/**
 * 根据给定的pattern（必须带替代符*）和特定片段subst一起当作key，查找并返回对应的val
 */ 
static lobj *lookupKeyByPattern(ledisDb *db, lobj *pattern, lobj *subst){
    
    //内部匿名struct
    struct{
        long len;
        long free;
        char buf[LEDIS_SORTKEY_MAX+1];
    } keyname;
    
    sds spat = pattern->ptr;
    sds ssub = subst->ptr;
    if(sdslen(spat)+sdslen(ssub)-1 > LEDIS_SORTKEY_MAX) return NULL;
    //必须要有*
    char *p = strchr(spat, '*');
    if(!p) return NULL;

    int prefixlen = p-spat; //pattern中*前面的长度，不包括*本身
    int sublen = sdslen(ssub);
    int postfixlen = sdslen(spat) - (prefixlen+1);  //pattern中*后面的长度，不包括*本身

    //构建key的原型，其中subst就是必须要匹配的，没有通配
    memcpy(keyname.buf, spat, prefixlen);
    memcpy(keyname.buf+prefixlen, ssub, sublen);
    memcpy(keyname.buf+prefixlen+sublen, p+1, postfixlen);
    keyname.buf[prefixlen+sublen+postfixlen] = '\0';
    keyname.len = prefixlen+sublen+postfixlen;

    lobj keyobj;    //不是指针，直接静态赋值
    keyobj.refcount = 1;
    keyobj.type = LEDIS_STRING;
    keyobj.ptr = ((char*)&keyname)+(sizeof(long)*2);    //直接跳字段获取buf

    return lookupKeyRead(db, &keyobj);
}

/**
 * 比较2个sortObject，规则由server的sort不同而不同
 */ 
static int sortCompare(const void *s1, const void *s2){

    const ledisSortObject *so1 = s1;
    const ledisSortObject *so2 = s2;
    int cmp;

    if(!server.sort_alpha){ //按对象内部的score字段排序
        if(so1->u.score > so2->u.score){
            cmp = 1;
        }else if(so1->u.score < so2->u.score){
            cmp = -1;
        }else{
            cmp = 0;
        }
    }else{  //按字母排序
        if(server.sort_bypattern){
            if(!so1->u.cmpobj || !so2->u.cmpobj){
                if(so1->u.cmpobj == so2->u.cmpobj){ //cmpobj都是NULL
                    cmp = 0;
                }else if(so1->u.cmpobj == NULL){    //so1的cmpobj是NULL
                    cmp = -1;
                }else{  //so2的cmpobj是NULL
                    cmp = 1;
                }
            }else{  //如果里面都有cmpobj，则用这个比
                //strcoll不同于strcmp，可以识别UTF-8之类的本地设置特定编码
                cmp = strcoll(so1->u.cmpobj->ptr, so2->u.cmpobj->ptr);  
            }
        }else{
            cmp = strcoll(so1->obj->ptr, so2->obj->ptr);
        }
    }
    return server.sort_desc ? -cmp : cmp;
}

/**
 * 超级复杂的一个命令，子功能特别多，为了速度还降低了一些可读性
 */ 
static void sortCommand(ledisClient *c){

    int outputlen = 0;
    int desc = 0, alpha = 0;    //默认值
    int limit_start = 0, limit_count = -1;  //默认值
    int dontsort = 0;   //不排序标记
    int getop = 0;  //GET参数计数器，因为只有它可以有多个
    lobj *sortby = NULL;
    lobj *sortval = lookupKeyRead(c->db, c->argv[1]);
    if(sortval == NULL){
        addReply(c, shared.nokeyerr);
        return;
    }
    //key对应的val必须是SET或LIST这类集合，只有集合才有排序的需要
    if(sortval->type != LEDIS_SET && sortval->type != LEDIS_LIST){
        addReply(c, shared.wrongtypeerr);
        return;
    }

    list *operations = listCreate();
    listSetFreeMethod(operations, zfree);
    int j = 2;

    //增加1次引用，因为可能会对原val进行修改或者删除的操作
    incrRefCount(sortval);

    //sort命令看着很像SQL的语法，在这里初步解析各参数，从参数3开始
    while(j < c->argc){
        int leftargs = c->argc-j-1; //剩下的参数个数
        if(!strcasecmp(c->argv[j]->ptr, "asc")){
            desc = 0;   //正序输出
        }else if(!strcasecmp(c->argv[j]->ptr, "desc")){
            desc = 1;   //倒序输出
        }else if(!strcasecmp(c->argv[j]->ptr, "alpha")){
            alpha = 1;  //按照字符排序，并不是默认的数值排序
        }else if(!strcasecmp(c->argv[j]->ptr, "limit") && leftargs >= 2){   //有limit则必须还有offset和count
            limit_start = atoi(c->argv[j+1]->ptr);
            limit_count = atoi(c->argv[j+2]->ptr);
            j+=2;   //limit一共是3个参数，这里要先跳过2个
        }else if(!strcasecmp(c->argv[j]->ptr, "by") && leftargs >= 1){  //有by则必须还有pattern
            sortby = c->argv[j+1];  //就是obj类型
            if(strchr(c->argv[j+1]->ptr, '*') == NULL)  dontsort = 1;
            j++;    //额外跳过1个
        }else if(!strcasecmp(c->argv[j]->ptr, "get") && leftargs >= 1){
            listAddNodeTail(operations, createSortOperation(LEDIS_SORT_GET, c->argv[j+1]));
            getop++;
            j++;
        }else if(!strcasecmp(c->argv[j]->ptr, "del") && leftargs >= 1){
            listAddNodeTail(operations, createSortOperation(LEDIS_SORT_DEL, c->argv[j+1]));
            j++;
        }else if(!strcasecmp(c->argv[j]->ptr, "incr") && leftargs >= 1){
            listAddNodeTail(operations, createSortOperation(LEDIS_SORT_INCR, c->argv[j+1]));
            j++;
        }else if(!strcasecmp(c->argv[j]->ptr, "decr") && leftargs >= 1){
            listAddNodeTail(operations, createSortOperation(LEDIS_SORT_DECR, c->argv[j+1]));
            j++;
        }else{
            decrRefCount(sortval);
            listRelease(operations);
            addReply(c, shared.syntaxerr);
            return;
        }
        j++;
    }

    //获取要排序集合的长度
    int vectorlen = (sortval->type == LEDIS_LIST) ?
                    listLength((list*)sortval->ptr) :
                    dictSize((dict*)sortval->ptr);
    
    //初始化要排序的中间存储容器
    ledisSortObject *vector = zmalloc(sizeof(ledisSortObject)*vectorlen);
    if(!vector) oom("allocating objects vector for SORT");
    j = 0;
    if(sortval->type == LEDIS_LIST){
        list *list = sortval->ptr;
        listNode *ln;
        listRewind(list);
        while((ln = listYield(list))){
            lobj *ele = ln->value;
            vector[j].obj = ele;    //并不会增加ele的引用
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
    }else{
        dict *set = sortval->ptr;
        dictIterator *di = dictGetIterator(set);
        if(!di) oom("dictGetIterator");
        dictEntry *setele;
        while((setele = dictNext(di)) != NULL){
            vector[j].obj = dictGetEntryVal(setele);    //并不会增加obj的引用
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        dictReleaseIterator(di);
    }
    assert(j == vectorlen);

    if(dontsort == 0){  //如果没有BY参数，或者有但pattern没有占位符*
        for(j = 0; j < vectorlen; j++){
            if(sortby){ //有BY但是pattern没有占位符*
                //根据pattern寻找pattern动态键的val
                lobj *byval = lookupKeyByPattern(c->db, sortby, vector[j].obj);
                if(!byval || byval->type != LEDIS_STRING) continue;
                if(alpha){  //按字母排
                    vector[j].u.cmpobj = byval;
                    incrRefCount(byval);
                }else{
                    //其他键的值当作排序参照值
                    vector[j].u.score = strtod(byval->ptr, NULL);
                    //不需要考虑alpha，如果是，则自己拿自己排就可以了，不需要画蛇添足再放到cmpobj里
                }
            }else{  //没有BY参数
                if(!alpha){ //只能处理没有开启alpha字母排序的情况，默认将val当作浮点型排序
                    vector[j].u.score = strtod(vector[j].obj->ptr, NULL);
                }
            }
        }
    }

    //处理limit中,offset和count
    int start = (limit_start < 0) ? 0 : limit_start;    //默认是0,即第一个元素，其实就是左边界下标
    int end = (limit_count < 0) ? vectorlen-1 : start+limit_count-1;    //默认为总量-1，将count转换成右边界下标
    if(start >= vectorlen){ //start过大
        start = vectorlen-1;
        end = vectorlen-2;
    }
    if(end >= vectorlen) end = vectorlen-1; //end过大

    if(dontsort == 0){  //如果没有BY参数，或者有但pattern没有占位符*
        server.sort_desc = desc;
        server.sort_alpha = alpha;
        server.sort_bypattern = sortby ? 1 : 0;
        //如果传了BY，同时还传了有效的LIMIT，则改用pgsort来排序
        if(sortby && (start != 0 || end != vectorlen-1)){
            pqsort(vector, vectorlen, sizeof(ledisSortObject), sortCompare, start, end);
        }else{
            qsort(vector, vectorlen, sizeof(ledisSortObject), sortCompare);
        }
    }

    //直接算出最终输出的数目
    outputlen = getop ? getop*(end-start+1) : end-start+1;
    addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", outputlen));
    //输出实际数据
    for(j = start; j <= end; j++){
        listNode *ln;
        //输出常规内容
        if(!getop){
            addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", sdslen(vector[j].obj->ptr)));
            addReply(c, vector[j].obj);
            addReply(c, shared.crlf);
        }
        //输出运算参数对应的内容
        listRewind(operations);
        while((ln = listYield(operations))){
            ledisSortOperation *sop = ln->value;
            lobj *val = lookupKeyByPattern(c->db, sop->pattern, vector[j].obj);
            if(sop->type == LEDIS_SORT_GET){
                if(!val || val->type != LEDIS_STRING){
                    addReply(c, shared.nullbulk);
                }else{
                    addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", sdslen(val->ptr)));
                    addReply(c, val);
                    addReply(c, shared.crlf);
                }
            }else if(sop->type == LEDIS_SORT_DEL){
                //目前没这个功能
            }
        }
    }

    //完事了，要清理
    decrRefCount(sortval);
    listRelease(operations);
    for(j = 0; j < vectorlen; j++){
        decrRefCount(vector[j].obj);
        if(sortby && alpha && vector[j].u.cmpobj){
            decrRefCount(vector[j].u.cmpobj);
        }
    }
    zfree(vector);
}

static void infoCommand(ledisClient *c){
    
    time_t uptime = time(NULL)-server.stat_starttime;
    sds info = sdscatprintf(sdsempty(), 
                "ledis_version:%s\r\n"
                "uptime_in_seconds:%d\r\n"
                "uptime_in_days:%d\r\n",
                "connected_clients:%d\r\n"
                "connected_slaves:%d\r\n"
                "used_memory:%zu\r\n"
                "changes_since_last_save%lld\r\n"
                "bgsave_in_progress:%d\r\n"
                "last_save_time:%d\r\n"
                "total_connections_received:%lld\r\n"
                "total_commands_processed:%lld\r\n"
                
                LEDIS_VERSION,
                uptime,
                uptime/(3600*24),
                listLength(server.clients)-listLength(server.slaves),
                listLength(server.slaves),
                server.usedmemory,
                server.dirty,
                server.bgsaveinprogress,
                server.lastsave,
                server.stat_numconnections,
                server.stat_numcommands,
                server.masterhost == NULL ? "master" :"slave"
                );

    if(server.masterhost){
        info = sdscatprintf(info, 
                "master_host:%s\r\n"
                "master_port:%d\r\n"
                "master_link_status:%s\r\n"
                "master_last_io_seconds_age:%d\r\n",
                server.masterhost,
                server.masterport,
                (server.replstate == LEDIS_REPL_CONNECTED) ? 
                    "up" : "down",
                (int)(time(NULL)-server.master->lastinteraction));
    }

    for(int i = 0; i < server.dbnum; i++){
        long long keys = dictSize(server.db[i].dict);
        long long vkeys = dictSize(server.db[i].expires);
        if(keys || vkeys){
            info = sdscatprintf(info, "db%d: keys=%lld, expires=%lld\r\n",
                    i, keys, vkeys);
        }
    }

    addReplySds(c, sdscatprintf(sdsempty(), "$%d\r\n", sdslen(info)));
    addReplySds(c, info);
    addReply(c, shared.crlf);
}

/**
 * 将对应的客户端转换成monitor类型
 */ 
static void monitorCommand(ledisClient *c){
    
    //只有主才能处理monitor命令
    if(c->flags & LEDIS_SLAVE) return;
    c->flags |= (LEDIS_SLAVE|LEDIS_MONITOR);
    c->slaveseldb = 0;
    if(!listAddNodeTail(server.monitors, c)) oom("listAddNodeTail");
    addReply(c, shared.ok);
}

/*====================================== expires相关 ===============================*/

static int removeExpire(ledisDb *db, lobj *key){
    if(dictDelete(db->expires, key) == DICT_OK){
        return 1;
    }else{
        return 0;
    }
}

static int setExpire(ledisDb *db, lobj *key, time_t when){
    if(dictAdd(db->expires, key, (void*)when) == DICT_ERR){
        return 0;
    }else{
        incrRefCount(key);
        return 1;
    }
}

static time_t getExpire(ledisDb *db, lobj *key){
    dictEntry *de;
    //没找到则直接返回-1
    if(dictSize(db->expires) == 0 || (de = dictFind(db->expires, key)) == NULL){
        return -1;
    } 
    return (time_t)dictGetEntryVal(de);
}

/**
 * 从expires找出时间值，过期了则删除（dict和expires都要删），否则返回0
 */ 
static int expireIfNeeded(ledisDb *db, lobj *key){
    dictEntry *de;
    //没找到则直接返回-1
    if(dictSize(db->expires) == 0 || (de = dictFind(db->expires, key)) == NULL){
        return 0;
    } 

    time_t when = (time_t)dictGetEntryVal(de);
    if(time(NULL) <= when) return 0;
    //过期了就要删除
    dictDelete(db->expires, key);
    //为啥dirty不增加，下面的函数就要加
    return dictDelete(db->dict, key) == DICT_OK;
}

/**
 * 只有在expires有key直接删除（dict和expires都要删），否则返回0
 * 相等于是expireIfNeeded的严格版
 */ 
static int deleteIfVolatile(ledisDb *db, lobj *key){
    dictEntry *de;
    //没找到则直接返回-1
    if(dictSize(db->expires) == 0 || (de = dictFind(db->expires, key)) == NULL){
        return 0;
    } 

    //只要有就删除，根本不管时间值是多少
    server.dirty++;
    dictDelete(db->expires, key);
    return dictDelete(db->dict, key) == DICT_OK;
}

/**
 * 将一个已有key变成易变key，如果已操作过这个key，也不会更新时间
 */ 
static void expireCommand(ledisClient *c){

    int seconds = atoi(c->argv[2]->ptr);
    dictEntry *de = dictFind(c->db->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.czero);
        return;
    }
    if(seconds <= 0){   //为啥不先检查这个？
        addReply(c, shared.czero);
        return;
    }else{
        time_t when = time(NULL)+seconds;
        if(setExpire(c->db, c->argv[1], when)){
            addReply(c, shared.cone);
        }else{
            addReply(c, shared.czero);
        }
        return;
    }
}

static void ttlCommand(ledisClient *c){
    int ttl = -1;
    time_t expire = getExpire(c->db, c->argv[1]);
    if(expire != -1){
        ttl = (int)(expire-time(NULL));
        if(ttl < 0) ttl = -1;
    }
    addReplySds(c, sdscatprintf(sdsempty(), ":%d\r\n", ttl));
}

/*====================================== 主从相关 ===============================*/

/**
 * 本质还是调用write向fd写入数据，但要以间歇的方式，并且考虑数据分批
 */ 
static int syncWrite(int fd, char *ptr, ssize_t size, int timeout){

    ssize_t nwritten, ret = size;
    time_t start = time(NULL);
    timeout++;  //加上aeWait的1秒

    while(size){    //直到所有size都发送了
        //确保就绪才会写，写死timeout为1秒
        if(aeWait(fd, AE_WRITABLE, 1000) & AE_WRITABLE){
            nwritten = write(fd, ptr, size);
            if(nwritten == -1) return -1;   //出错了
            ptr += nwritten;
            size -= nwritten;
        }
        if((time(NULL)-start) > timeout){
            errno = ETIMEDOUT;
            return -1;
        }
    }
    return ret; //肯定是size
}

/**
 * 本质还是调用read从fd读入数据，但要以间歇的方式，并且考虑数据分批
 */
static int syncRead(int fd, char *ptr, ssize_t size, int timeout){

    ssize_t nread, totread = 0;
    time_t start = time(NULL);
    timeout++;  //加上aeWait的1秒

    while(size){    //直到所有size都发送了
        //确保就绪才会写，写死timeout为1秒
        if(aeWait(fd, AE_READABLE, 1000) & AE_READABLE){
            nread = read(fd, ptr, size);
            if(nread == -1) return -1;   //出错了
            ptr += nread;
            size -= nread;
            totread += nread;
        }
        if((time(NULL)-start) > timeout){
            errno = ETIMEDOUT;
            return -1;
        }
    }
    return totread; //肯定是size
}

/**
 * 内部使用syncRead，但每次只读一行（遇到\r\n或\n结尾就算完）
 */ 
static int syncReadLine(int fd, char *ptr, ssize_t size, int timeout){
    ssize_t nread = 0;
    //一个一个字符读
    while(size){
        char c;
        if(syncRead(fd, &c, 1, timeout) == -1) return -1;
        if(c == '\n'){  //如果是换行符，则封口ptr然后返回字符数
            *ptr = '\0';
            if(nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        }else{
            *ptr++ = c;
            *ptr = '\0';    //保险
            nread++;
        }
    }
    return nread;
}

static void syncCommand(ledisClient *c){

    //如果客户端已经标记为从，则不会再次处理（也就是说只有从初始化时候才可以调用一次）
    if(c->flags & LEDIS_SLAVE) return;
    //客户端不能有别的回复数据
    if(listLength(c->reply) != 0){
        addReplySds(c, sdsnew("-ERR SYNC is invalid with pending input\r\n"));
        return;
    }

    ledisLog(LEDIS_NOTICE, "slave ask for syncronization");
    //如果主正在bgsave，则要特殊处理
    if(server.bgsaveinprogress){
        ledisClient *slave;
        listNode *ln;
        listRewind(server.slaves);
        //检测其他的从，有没有状态为WAIT_BGSAVE_END的
        while((ln = listYield(server.slaves))){
            slave = ln->value;
            if(slave->replstate == LEDIS_REPL_WAIT_BGSAVE_END) break;
        }
        if(ln){ //有从的同步状态处于WAIT_BGSAVE_END
            listRelease(c->reply);
            c->reply = listDup(slave->reply);
            if(!c->reply) oom("listDup copying slave reply list");
            c->replstate = LEDIS_REPL_WAIT_BGSAVE_END;
            ledisLog(LEDIS_NOTICE, "Waiting for end of BGSAVE for SYNC");
        }else{
            c->replstate = LEDIS_REPL_WAIT_BGSAVE_START;
            ledisLog(LEDIS_NOTICE, "Waiting for next BGSAVE for SYNC");
        }
    }else{
        ledisLog(LEDIS_NOTICE, "Starting BGSAVE for SYNC");
        if(ldbSaveBackground(server.dbfilename) != LEDIS_OK){
            ledisLog(LEDIS_NOTICE, "Replication failed, can't BGSAVE");
            addReplySds(c, sdsnew("-ERR Unable to perform background save\r\n"));
            return;
        }
        c->replstate = LEDIS_REPL_WAIT_BGSAVE_END;
    }
    c->repldbfd = -1;
    c->flags |= LEDIS_SLAVE;
    c->slaveseldb = 0;
    if(!listAddNodeTail(server.slaves, c)) oom("listAddNodeTail");
    return;
}

static void sendBulkToSlave(aeEventLoop *el, int fd, void *privdata, int mask){
    
    ledisClient *slave = privdata;
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);
    char buf[LEDIS_IOBUF_LEN];
    ssize_t nwritten;

    if(slave->repldboff == 0){  //从还没有任何数据
        sds bulkcount = sdscatprintf(sdsempty(), "$%lld\r\n", 
                        (unsigned long long)slave->repldbsize);
        if(write(fd, bulkcount, sdslen(bulkcount)) != (signed)sdslen(bulkcount)){
            //失败了
            sdsfree(bulkcount);
            freeClient(slave);
            return;
        }
        sdsfree(bulkcount); //write成功，就不需要了
    }
    lseek(slave->repldbfd, slave->repldboff, SEEK_SET);
    ssize_t buflen = read(slave->repldbfd, buf, LEDIS_IOBUF_LEN);
    if(buflen <= 0){
        ledisLog(LEDIS_WARNING, "Read error sending DB to slave: %s", 
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if((nwritten = write(fd, buf, buflen)) == -1){
        ledisLog(LEDIS_DEBUG, "Write error sending DB to slave: %s", strerror(errno));
        freeClient(slave);
        return;
    }
    slave->repldboff += nwritten;
    //每轮只会read/write一次，如果完事了再清理event，同时运行新的event
    if(slave->repldboff == slave->repldbsize){
        close(slave->repldbfd);
        slave->repldbfd = -1;
        aeDeleteFileEvent(server.el, slave->fd, AE_WRITABLE);
        slave->replstate = LEDIS_REPL_ONLINE;   //置为db同步完毕
        if(aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, 
            sendReplyToClient, slave, NULL) == AE_ERR){
            freeClient(slave);
            return;
        }
        addReplySds(slave, sdsempty());
        ledisLog(LEDIS_NOTICE, "Synchronization with slave succeeded");
    }
}

/**
 * 由serverCron中检测如果bgsave完成时才会调用
 */ 
static void updateSlavesWaitingBgsave(int bgsaveerr){

    listNode *ln;
    int startbgsave = 0;
    listRewind(server.slaves);
    while((ln = listYield(server.slaves))){
        ledisClient *slave = ln->value;
        if(slave->replstate == LEDIS_REPL_WAIT_BGSAVE_START){
            //刚进行了bgsave，可以改变状态了
            startbgsave = 1;
            slave->replstate = LEDIS_REPL_WAIT_BGSAVE_END;
        }else if(slave->replstate == LEDIS_REPL_WAIT_BGSAVE_END){
            struct ledis_stat buf;
            if(bgsaveerr != LEDIS_OK){  //说明bgsave失败了
                freeClient(slave);
                ledisLog(LEDIS_WARNING, "SYNC failed. BGSAVE child returned an error");
                continue;
            }
            //说明bgsave正常返回
            if((slave->repldbfd = open(server.dbfilename, O_RDONLY)) == -1 || 
                ledis_fstat(slave->repldbfd, &buf) == -1){
                freeClient(slave);
                ledisLog(LEDIS_WARNING, "SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                continue;
            }
            slave->repldboff = 0;
            slave->repldbsize = buf.st_size;
            slave->replstate = LEDIS_REPL_SEND_BULK;
            aeDeleteFileEvent(server.el, slave->fd, AE_WRITABLE);
            //开始间歇性的启动DB文件复制给从的函数
            if(aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, 
                sendBulkToSlave, slave, NULL) == AE_ERR){
                freeClient(slave);
                continue;
            }
        }
    }
    //如果没有bgsave在执行，则开始执行，如果发生错误，则清理相应的从客户端
    if(startbgsave){
        if(ldbSaveBackground(server.dbfilename) != LEDIS_OK){
            listRewind(server.slaves);
            ledisLog(LEDIS_WARNING, "SYNC failed. BGSAVE failed");
            while((ln = listYield(server.slaves))){
                ledisClient *slave = ln->value;
                if(slave->replstate == LEDIS_REPL_WAIT_BGSAVE_START){
                    freeClient(slave);
                }
            }
        }
    }
}

/**
 * 和主同步，只有从会调用（在serverCron函数里周期性调用）
 */ 
static int syncWithMaster(void){
    
    char buf[1024], tmpfile[256];
    int fd = anetTcpConnect(NULL, server.masterhost, server.masterport);
    if(fd == -1){
        ledisLog(LEDIS_WARNING, "Unable to connect to MASTER: %s", strerror(errno));
        return LEDIS_ERR;
    }
    //发起sync命令
    if(syncWrite(fd, "SYNC \r\n", 7, 5) == -1){
        close(fd);
        ledisLog(LEDIS_WARNING, "I/O error writing to MASTER: %s", strerror(errno));
        return LEDIS_ERR;
    }
    //获取相应数据
    if(syncReadLine(fd, buf, 1024, 3600) == -1){
        close(fd);
        ledisLog(LEDIS_WARNING, "I/O error reading bulk count from MASTER: %s", 
                    strerror(errno));
        return LEDIS_ERR;
    }
    //先获取到的是dump文件大小，+1是为了跳过后来追加的$字符
    int dumpsize = atoi(buf+1);
    ledisLog(LEDIS_NOTICE, "Receiving %d bytes data dump from MASTER", dumpsize);
    //生成ldb文件临时文件名
    snprintf(tmpfile, 256, "temp-%d.%ld.ldb", (int)time(NULL), (long int)random());
    int dfd = open(tmpfile, O_CREAT|O_WRONLY, 0644);
    if(dfd == -1){
        close(fd);
        ledisLog(LEDIS_WARNING, "Opening the temp file needed for MASTER <-> SLAVE synchronization: %s", 
                    strerror(errno));
        return LEDIS_ERR;
    }
    //生成ldb文件
    while(dumpsize){
        int nread = read(fd, buf, (dumpsize < 1024 ? dumpsize : 1024));
        if(nread == -1){
            ledisLog(LEDIS_WARNING, "I/O error trying to sync with MASTER: %s", 
                        strerror(errno));
            close(fd);
            close(dfd);
            return LEDIS_ERR;
        }
        int nwritten = write(dfd, buf, nread);
        if(nwritten == -1){
            ledisLog(LEDIS_WARNING, "Write error writing to the DB dump file needed for MASTER <-> SLAVE synchronization: %s", 
                        strerror(errno));
            close(fd);
            close(dfd);
            return LEDIS_ERR;
        }
        dumpsize -= nread;
    }
    close(dfd);
    //生成临时ldb文件完毕
    if(rename(tmpfile, server.dbfilename) == -1){
        ledisLog(LEDIS_WARNING, "Failed trying to rename the temp DB into dump.ldb in MASTER <-> SLAVE synchronization: %s", 
                    strerror(errno));
        unlink(tmpfile);
        close(fd);
        return LEDIS_ERR;
    }
    emptyDb();
    if(ldbLoad(server.dbfilename) != LEDIS_OK){
        ledisLog(LEDIS_WARNING, "Failed trying to load the MASTER synchronization DB from disk");
        close(fd);
        return LEDIS_ERR;
    }
    server.master = createClient(fd);
    server.master->flags |= LEDIS_MASTER;
    server.replstate = LEDIS_REPL_CONNECTED;
    return LEDIS_OK;
}

static void slaveofCommand(ledisClient *c){
    //参数为no和one，说明要取消从，变成普通的主
    if(!strcasecmp(c->argv[1]->ptr, "no") && 
        !strcasecmp(c->argv[2]->ptr, "one")){
        if(server.masterhost){  //已经配了主，则先清除
            sdsfree(server.masterhost);
            server.masterhost = NULL;
            if(server.master) freeClient(server.master);
            server.replstate = LEDIS_REPL_NONE;
            ledisLog(LEDIS_NOTICE, "MASTER MODE enabled (user request)");
        }
    }else{  //否则将主配置成参数指定的ip和port
        sdsfree(server.masterhost);
        server.masterhost = sdsdup(c->argv[1]->ptr);
        server.masterport = atoi(c->argv[2]->ptr);
        if(server.master) freeClient(server.master);
        server.replstate = LEDIS_REPL_CONNECT;
        ledisLog(LEDIS_NOTICE, "SLAVE OF %s:%d enabled (user request)", 
                    server.masterhost, server.masterport);
    }
    addReply(c, shared.ok);
}

/*====================================== 最大内存相关 ===============================*/
/**
 * 内存满了，尝试删一个对象（感觉不实用，只是个试水版）
 */ 
static void freeMemoryIfNeeded(void){
    while(server.maxmemory && zmalloc_used_memory() > server.maxmemory){
        //先尝试删除objfreelist里面的对象
        if(listLength(server.objfreelist)){
            listNode *head = listFirst(server.objfreelist);
            lobj *o = listNodeValue(head);
            listDelNode(server.objfreelist, head);
            zfree(o);
        }else{  //第二方案是删除设置的expire的key/val，原则是删expire马上要到期的
            int freed = 0;
            for(int i = 0; i < server.dbnum; i++){
                int minttl = -1;
                lobj *minkey = NULL;
                dictEntry *de;
                if(dictSize(server.db[i].expires)){
                    freed = 1;  //标记是否删除了expires的数据
                    //只尝试找3个key，不会找出绝对最小的ttl
                    for(int j = 0; j < 3; j++){
                        de = dictGetRandomKey(server.db[i].expires);
                        time_t t = (time_t)dictGetEntryKey(de);
                        if(minttl == -1 || t < minttl){
                            minkey = dictGetEntryKey(de);
                            minttl = t;
                        }
                    }
                    deleteKey(server.db+i, minkey);
                }
            }
            //如果俩方案都行不通，直接退出
            if(!freed) return; 
        }
    }
}

/*====================================== DEBUG相关 ===============================*/

static void debugCommand(ledisClient *c){
    if(!strcasecmp(c->argv[1]->ptr, "segfault")){
        *((char*)-1) = 'x'; //故意产生一个segfault错误
    }else if(!strcasecmp(c->argv[1]->ptr, "object") && c->argc == 3){
        dictEntry *de = dictFind(c->db->dict, c->argv[2]);
        if(!de){
            addReply(c, shared.nokeyerr);
            return;
        }
        lobj *key = dictGetEntryKey(de);
        lobj *val = dictGetEntryVal(de);
        addReplySds(c, sdscatprintf(sdsempty(), 
                    "+KEY at:%p refcount:%d, value at:%p refcount:%d\r\n",
                    key, key->refcount, val, val->refcount));
    }else{
        addReplySds(c, sdsnew("-ERR Syntax error, try DEBUG [SEGFAULT|OBJECT <key>]\r\n"));
    }
}

#ifdef HAVE_BACKTRACE
static struct ledisFunctionSym symsTable[] = {
{"freeStringObject", (unsigned long)freeStringObject},
{"freeListObject", (unsigned long)freeListObject},
{"freeSetObject", (unsigned long)freeSetObject},
{"decrRefCount", (unsigned long)decrRefCount},
{"createObject", (unsigned long)createObject},
{"freeClient", (unsigned long)freeClient},
{"rdbLoad", (unsigned long)ldbLoad},
{"addReply", (unsigned long)addReply},
{"addReplySds", (unsigned long)addReplySds},
{"incrRefCount", (unsigned long)incrRefCount},
{"rdbSaveBackground", (unsigned long)ldbSaveBackground},
{"createStringObject", (unsigned long)createStringObject},
{"replicationFeedSlaves", (unsigned long)replicationFeedSlaves},
{"syncWithMaster", (unsigned long)syncWithMaster},
{"tryObjectSharing", (unsigned long)tryObjectSharing},
{"removeExpire", (unsigned long)removeExpire},
{"expireIfNeeded", (unsigned long)expireIfNeeded},
{"deleteIfVolatile", (unsigned long)deleteIfVolatile},
{"deleteKey", (unsigned long)deleteKey},
{"getExpire", (unsigned long)getExpire},
{"setExpire", (unsigned long)setExpire},
{"updateSlavesWaitingBgsave", (unsigned long)updateSlavesWaitingBgsave},
{"freeMemoryIfNeeded", (unsigned long)freeMemoryIfNeeded},
{"authCommand", (unsigned long)authCommand},
{"pingCommand", (unsigned long)pingCommand},
{"echoCommand", (unsigned long)echoCommand},
{"setCommand", (unsigned long)setCommand},
{"setnxCommand", (unsigned long)setnxCommand},
{"getCommand", (unsigned long)getCommand},
{"delCommand", (unsigned long)delCommand},
{"existsCommand", (unsigned long)existsCommand},
{"incrCommand", (unsigned long)incrCommand},
{"decrCommand", (unsigned long)decrCommand},
{"incrbyCommand", (unsigned long)incrbyCommand},
{"decrbyCommand", (unsigned long)decrbyCommand},
{"selectCommand", (unsigned long)selectCommand},
{"randomkeyCommand", (unsigned long)randomkeyCommand},
{"keysCommand", (unsigned long)keysCommand},
{"dbsizeCommand", (unsigned long)dbsizeCommand},
{"lastsaveCommand", (unsigned long)lastsaveCommand},
{"saveCommand", (unsigned long)saveCommand},
{"bgsaveCommand", (unsigned long)bgsaveCommand},
{"shutdownCommand", (unsigned long)shutdownCommand},
{"moveCommand", (unsigned long)moveCommand},
{"renameCommand", (unsigned long)renameCommand},
{"renamenxCommand", (unsigned long)renamenxCommand},
{"lpushCommand", (unsigned long)lpushCommand},
{"rpushCommand", (unsigned long)rpushCommand},
{"lpopCommand", (unsigned long)lpopCommand},
{"rpopCommand", (unsigned long)rpopCommand},
{"llenCommand", (unsigned long)llenCommand},
{"lindexCommand", (unsigned long)lindexCommand},
{"lrangeCommand", (unsigned long)lrangeCommand},
{"ltrimCommand", (unsigned long)ltrimCommand},
{"typeCommand", (unsigned long)typeCommand},
{"lsetCommand", (unsigned long)lsetCommand},
{"saddCommand", (unsigned long)saddCommand},
{"sremCommand", (unsigned long)sremCommand},
{"smoveCommand", (unsigned long)smoveCommand},
{"sismemberCommand", (unsigned long)sismemberCommand},
{"scardCommand", (unsigned long)scardCommand},
{"spopCommand", (unsigned long)spopCommand},
{"sinterCommand", (unsigned long)sinterCommand},
{"sinterstoreCommand", (unsigned long)sinterstoreCommand},
{"sunionCommand", (unsigned long)sunionCommand},
{"sunionstoreCommand", (unsigned long)sunionstoreCommand},
{"sdiffCommand", (unsigned long)sdiffCommand},
{"sdiffstoreCommand", (unsigned long)sdiffstoreCommand},
{"syncCommand", (unsigned long)syncCommand},
{"flushdbCommand", (unsigned long)flushdbCommand},
{"flushallCommand", (unsigned long)flushallCommand},
{"sortCommand", (unsigned long)sortCommand},
{"lremCommand", (unsigned long)lremCommand},
{"infoCommand", (unsigned long)infoCommand},
{"mgetCommand", (unsigned long)mgetCommand},
{"monitorCommand", (unsigned long)monitorCommand},
{"expireCommand", (unsigned long)expireCommand},
{"getSetCommand", (unsigned long)getSetCommand},
{"ttlCommand", (unsigned long)ttlCommand},
{"slaveofCommand", (unsigned long)slaveofCommand},
{"debugCommand", (unsigned long)debugCommand},
{"processCommand", (unsigned long)processCommand},
{"setupSigSegvAction", (unsigned long)setupSigSegvAction},
{"readQueryFromClient", (unsigned long)readQueryFromClient},
{"rdbRemoveTempFile", (unsigned long)ldbRemoveTempFile},
{NULL,0}
};

/* This function try to convert a pointer into a function name. It's used in
 * oreder to provide a backtrace under segmentation fault that's able to
 * display functions declared as static (otherwise the backtrace is useless). */
static char *findFuncName(void *pointer, unsigned long *offset){
    int i, ret = -1;
    unsigned long off, minoff = 0;

    /* Try to match against the Symbol with the smallest offset */
    for (i=0; symsTable[i].pointer; i++) {
        unsigned long lp = (unsigned long) pointer;

        if (lp != (unsigned long)-1 && lp >= symsTable[i].pointer) {
            off=lp-symsTable[i].pointer;
            if (ret < 0 || off < minoff) {
                minoff=off;
                ret=i;
            }
        }
    }
    if (ret == -1) return NULL;
    *offset = minoff;
    return symsTable[ret].name;
}

static void *getMcontextEip(ucontext_t *uc) {
#if defined(__FreeBSD__)
    return (void*) uc->uc_mcontext.mc_eip;
#elif defined(__dietlibc__)
    return (void*) uc->uc_mcontext.eip;
#elif defined(__APPLE__)
    return (void*) uc->uc_mcontext->__ss.__eip;
#else /* Linux */
    return (void*) uc->uc_mcontext.gregs[REG_EIP];
#endif
}

static void segvHandler(int sig, siginfo_t *info, void *secret) {
    void *trace[100];
    char **messages = NULL;
    int i, trace_size = 0;
    unsigned long offset=0;
    time_t uptime = time(NULL)-server.stat_starttime;
    ucontext_t *uc = (ucontext_t*) secret;
    LEDIS_NOTUSED(info);

    ledisLog(LEDIS_WARNING, "======= Ooops! Redis %s got signal: -%d- =======", LEDIS_VERSION, sig);
    ledisLog(LEDIS_WARNING, "%s", sdscatprintf(sdsempty(),
        "redis_version:%s; "
        "uptime_in_seconds:%d; "
        "connected_clients:%d; "
        "connected_slaves:%d; "
        "used_memory:%zu; "
        "changes_since_last_save:%lld; "
        "bgsave_in_progress:%d; "
        "last_save_time:%d; "
        "total_connections_received:%lld; "
        "total_commands_processed:%lld; "
        "role:%s;"
        ,LEDIS_VERSION,
        uptime,
        listLength(server.clients)-listLength(server.slaves),
        listLength(server.slaves),
        server.usedmemory,
        server.dirty,
        server.bgsaveinprogress,
        server.lastsave,
        server.stat_numconnections,
        server.stat_numcommands,
        server.masterhost == NULL ? "master" : "slave"
    ));
    
    trace_size = backtrace(trace, 100);
    /* overwrite sigaction with caller's address */
    trace[1] = getMcontextEip(uc);
    messages = backtrace_symbols(trace, trace_size);

    for (i=1; i<trace_size; ++i) {
        char *fn = findFuncName(trace[i], &offset), *p;

        p = strchr(messages[i],'+');
        if (!fn || (p && ((unsigned long)strtol(p+1,NULL,10)) < offset)) {
            ledisLog(LEDIS_WARNING,"%s", messages[i]);
        } else {
            ledisLog(LEDIS_WARNING,"%d ledis-server.out %p %s + %d", i, trace[i], fn, (unsigned int)offset);
        }
    }
    free(messages);
    exit(0);
}

static void setupSigSegvAction(void) {
    struct sigaction act;

    sigemptyset (&act.sa_mask);
    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
     * is used. Otherwise, sa_handler is used */
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = segvHandler;
    sigaction (SIGSEGV, &act, NULL);
    sigaction (SIGBUS, &act, NULL);
    sigaction (SIGFPE, &act, NULL);
    sigaction (SIGILL, &act, NULL);
    sigaction (SIGBUS, &act, NULL);
    return;
}
#else /* HAVE_BACKTRACE */
static void setupSigSegvAction(void) {
}

#endif /* HAVE_BACKTRACE */

/*====================================== 主函数 ===============================*/

#ifdef __linux__
int linuxOvercommitMemoryValue(void){
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory", "r");
    char buf[64];

    if(!fp) return -1;
    if(fgets(buf, 64, fp) == NULL){
        fclose(fp);
        return -1;
    }
    fclose(fp);
    return atoi(buf);
}

void linuxOvercommitMemoryWarning(void){
    if(linuxOvercommitMemoryValue() == 0){
        ledisLog(LEDIS_WARNING, "WARNING overcommit_memory is set to 0! Background save may fail under low condition memory. To fix this issue add 'echo 1 > /proc/sys/vm/overcommit_memory' in your init scripts.");
    }
}
#endif

/**
 * 将当前进程弄成daemon，用的是"古典"方法
 */ 
static void daemonize(void){
    int fd;
    FILE *fp;
    if(fork() != 0) exit(EXIT_SUCCESS); //退出主进程
    setsid();   //创建一个新会话

    if((fd = open("/dev/null", O_RDWR, 0)) != -1){
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if(fd > STDERR_FILENO)  close(fd);
    }
    //写prd文件
    fp = fopen(server.pidfile, "w");
    if(fp){
        fprintf(fp, "%d\n", getpid());
        fclose(fp);
    }
}

int main(int argc, char *argv[]){

#ifdef __linux__
    linuxOvercommitMemoryWarning();
#endif

    LEDIS_NOTUSED(argc);
    LEDIS_NOTUSED(argv);
    //初始化server配置
    initServerConfig();
    if(argc == 2){  //制定了conf文件
        ResetServerSaveParams();    //清空saveparams字段
        loadServerConfig(argv[1]);
    }else if(argc > 2){
        fprintf(stderr, "Usage: ./ledis-server [/path/to/ledis.conf]\n");
        exit(EXIT_FAILURE);
    }else{
        ledisLog(LEDIS_WARNING, "Warning: no config file specified, using the default config. In order to specify a config file use 'ledis-server.out /path/to/ledis.conf'");
    }
    //初始化server
    initServer();
    if(server.daemonize) daemonize();
    ledisLog(LEDIS_NOTICE, "Server started, LEDIS version " LEDIS_VERSION);
    //尝试恢复数据库dump.ldb文件
    if(ldbLoad(server.dbfilename) == LEDIS_OK){
        ledisLog(LEDIS_NOTICE, "DB loaded from disk");
    }
    //假定恢复db用了5s
    //sleep(5);

    //基于server的fd，创建fileEvent
    if(aeCreateFileEvent(server.el, server.fd, AE_READABLE,
            acceptHandler, NULL, NULL) == AE_ERR){
        oom("creating file event");
    }
    ledisLog(LEDIS_NOTICE, "The server is now ready to accept connections on port %d", server.port);
    aeMain(server.el);  //开始轮询，直到el的stop被置位
    aeDeleteEventLoop(server.el);
    exit(EXIT_SUCCESS);
}