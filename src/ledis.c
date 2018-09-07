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
#include <sys/stat.h>
#include <fcntl.h>

#include <arpa/inet.h>
#include <netdb.h>

#include <signal.h>
#include <sys/wait.h>

#include <assert.h>
#include <errno.h>

#include "ae.h"
#include "sds.h"
#include "anet.h"
#include "dict.h"
#include "adlist.h"

/*========== server配置相关 ==========*/
#define LEDIS_SERVERPORT    6379    //默认TCP端口
#define LEDIS_MAXIDLETIME   (60*5)  //默认客户端timeout时长
#define LEDIS_QUERYBUF_LEN  1024    //1k
#define LEDIS_LOADBUF_LEN   1024    //1k
#define LEDIS_MAX_ARGS  16
#define LEDIS_DEFAULT_DBNUM 16  //默认db数目
#define LEDIS_CONFIGLINE_MAX    1024    //1k
#define LEDIS_OBJFREELIST_MAX 10000 //obj池子最大数目
#define LEDIS_MAX_SYNC_TIME 60 //

/*========== Hash table 相关==========*/
#define LEDIS_HT_MINFILL    10  //dict实际占用百分数，小于这个比率就会尝试再收缩（前提是扩张过）
#define LEDIS_HT_MINSLOTS   16384   //最小容量，如果不扩长，也就不会继续收缩


/*========== 返回错误代码 ==========*/
#define LEDIS_OK    0
#define LEDIS_ERR   -1

/*========== 命令类型 ==========*/
#define LEDIS_CMD_BULK  1
#define LEDIS_CMD_INLINE    2

/*========== 对象类型 ==========*/
#define LEDIS_STRING    0
#define LEDIS_LIST  1
#define LEDIS_SET   2
#define LEDIS_HASH  3
#define LEDIS_SELECTDB  254
#define LEDIS_EOF   255

/*========== 客户端flags ==========*/
#define LEDIS_CLOSE 1 //客户端是普通端，用完直接关闭
#define LEDIS_SLAVE 2 //客户端是个从服务端
#define LEDIS_MASTER 4 //客户端是个主服务端（应该用不到）

/*========== 服务端主从状态 ==========*/
#define LEDIS_REPL_NONE 0 //没有活动的主从
#define LEDIS_REPL_CONNECT 1 //必须要连主
#define LEDIS_REPL_CONNECTED 2 //已连上主

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

//对象封装，可以是string/list/set
typedef struct ledisObject{
    int type;
    void *ptr;
    int refcount;
} lobj;

//客户端封装
typedef struct ledisClient{
    int fd;
    dict *dict;
    int dictid;
    sds querybuf;
    lobj *argv[LEDIS_MAX_ARGS];
    int argc;
    int bulklen;
    list *reply;
    int sentlen;
    time_t lastinteraction; //上一次交互的时间点
    int flags; //LEDIS_CLOSE或者LEDIS_SLAVE
    int slaveseldb; //如果client是从服务端，则代表自己的dbid
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
    dict **dict;    //内存存储的指针数组，和dbnum相关，即默认16个元素
    long long dirty;    //自从上次save后经历的变更数
    list *clients;  //客户端链表
    list *slaves;   //从服务端链表

    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;
    int verbosity;  //log的输出等级
    int glueoutputbuf;
    int cronloops;
    int maxidletime;

    int dbnum;
    list *objfreelist;  //复用lobj对象的池子链表
    int bgsaveinprogress;   //是否正在bgsave，其实是个bool
    time_t lastsave;
    struct saveparam *saveparams;
    
    int saveparamslen;
    char *logfile;
    char *bindaddr;

    //主从相关
    int isslave;
    char *masterhost;
    int masterport;
    ledisClient *master;
    int replstate;
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

//定义会用到的lobj可复用对象
struct sharedObjectsStruct{
    lobj *crlf, *ok, *err, *zerobulk, *nil, *zero, *one, *pong, *space,
    *minus1, *minus2, *minus3, *minus4,
    *wrongtypeerr, *nokeyerr, *wrongtypeerrbulk, *nokeyerrbulk,
    *select0, *select1, *select2, *select3, *select4,
    *select5, *select6, *select7, *select8, *select9;
} shared;

/*====================================== 函数原型 ===============================*/

static lobj *createObject(int type, void *ptr);
static lobj *createListObject(void);
static void incrRefCount(lobj *o);
static void decrRefCount(void *obj);
static void freeStringObject(lobj *o);
static void freeListObject(lobj *o);
static void freeSetObject(lobj *o);
static void freeHashObject(lobj *o);

static void freeClient(ledisClient *c);
static void addReply(ledisClient *c, lobj *obj);
static void addReplySds(ledisClient *c, sds s);

static int saveDbBackground(char *filename);
static lobj *createStringObject(char *ptr, size_t len);
static void replicationFeedSlaves(struct ledisCommand *cmd, int dictid, lobj **argv, int argc);
static int syncWithMaster(void);    //和主同步

//所有函数均只在本文件被调用
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
static void randomKeyCommand(ledisClient *c);
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
static void sismemberCommand(ledisClient *c);
static void scardCommand(ledisClient *c);
static void sinterCommand(ledisClient *c);

static void syncCommand(ledisClient *c);

/*====================================== 全局变量 ===============================*/
static struct ledisServer server;
static struct ledisCommand cmdTable[] = {
    {"set",setCommand,3,LEDIS_CMD_BULK},
    {"setnx",setnxCommand,3,LEDIS_CMD_BULK},
    {"get",getCommand,2,LEDIS_CMD_INLINE},
    {"del",delCommand,2,LEDIS_CMD_INLINE},
    {"exists",existsCommand,2,LEDIS_CMD_INLINE},
    {"incr",incrCommand,2,LEDIS_CMD_INLINE},
    {"decr",decrCommand,2,LEDIS_CMD_INLINE},
    {"lpush",lpushCommand,3,LEDIS_CMD_BULK},
    {"rpush",rpushCommand,3,LEDIS_CMD_BULK},
    {"lpop",lpopCommand,2,LEDIS_CMD_INLINE},
    {"rpop",rpopCommand,2,LEDIS_CMD_INLINE},
    {"llen",llenCommand,2,LEDIS_CMD_INLINE},
    {"lindex",lindexCommand,3,LEDIS_CMD_INLINE},
    {"lset",lsetCommand,4,LEDIS_CMD_BULK},
    {"lrange",lrangeCommand,4,LEDIS_CMD_INLINE},
    {"ltrim",ltrimCommand,4,LEDIS_CMD_INLINE},
    
    {"sadd",saddCommand,3,LEDIS_CMD_BULK},
    {"srem",sremCommand,3,LEDIS_CMD_BULK},
    {"sismember",sismemberCommand,3,LEDIS_CMD_BULK},
    {"scard",scardCommand,2,LEDIS_CMD_INLINE},
    {"sinter",sinterCommand,-2,LEDIS_CMD_INLINE},
    {"smembers",sinterCommand,2,LEDIS_CMD_INLINE},

    {"incrby",incrbyCommand,2,LEDIS_CMD_INLINE},
    {"decrby",decrbyCommand,2,LEDIS_CMD_INLINE},

    {"randomkey",randomKeyCommand,1,LEDIS_CMD_INLINE},
    {"select",selectCommand,2,LEDIS_CMD_INLINE},
    {"move",moveCommand,3,LEDIS_CMD_INLINE},
    {"rename",renameCommand,3,LEDIS_CMD_INLINE},
    {"renamenx",renamenxCommand,3,LEDIS_CMD_INLINE},
    {"keys",keysCommand,2,LEDIS_CMD_INLINE},
    {"dbsize",dbsizeCommand,1,LEDIS_CMD_INLINE},
    {"ping",pingCommand,1,LEDIS_CMD_INLINE},
    {"echo",echoCommand,2,LEDIS_CMD_BULK},
    {"save",saveCommand,1,LEDIS_CMD_INLINE},
    {"bgsave",bgsaveCommand,1,LEDIS_CMD_INLINE},
    {"shutdown",shutdownCommand,1,LEDIS_CMD_INLINE},
    {"lastsave",lastsaveCommand,1,LEDIS_CMD_INLINE},
    {"type",typeCommand,2,LEDIS_CMD_INLINE},
    {"sync",syncCommand,1,LEDIS_CMD_INLINE},
    {"",NULL,0,0}
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
void closeTimedoutClients(void){
    time_t now = time(NULL);
    listIter *li = listGetIterator(server.clients, AL_START_HEAD);
    if(!li) return;
    listNode *ln;
    ledisClient *c;
    while((ln = listNextElement(li)) != NULL){
        c = listNodeValue(ln);
        //检查上次交互的时间，slave客户端没有timeout
        if(!(c->flags & LEDIS_SLAVE) && now - c->lastinteraction > server.maxidletime){
            ledisLog(LEDIS_DEBUG, "Closing idle client");
            //关闭client
            freeClient(c);
        }
    }
    listReleaseIterator(li);
}

/**
 * timeEvent的回调函数，此版本是每隔1秒执行一次，但不是精确的
 */ 
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData){
    
    //此版本所有入参都没用到
    LEDIS_NOTUSED(eventLoop);
    LEDIS_NOTUSED(id);
    LEDIS_NOTUSED(clientData);

    int loops = server.cronloops++; //当前循环次数
    int size, used;
    //尝试收缩每个HT
    for(int i = 0; i < server.dbnum; i++){
        //获取总量和实际使用量
        size = dictGetHashTableSize(server.dict[i]);
        used = dictGetHashTableUsed(server.dict[i]);
        //差不多每5秒尝试输出一下上述状态
        if(!(loops % 5) && used > 0){
            ledisLog(LEDIS_DEBUG, "DB %d: %d keys in %d slots HT.", i, used, size);
        }
        
        if(size && used && size > LEDIS_HT_MINSLOTS && 
            (used * 100 / size < LEDIS_HT_MINFILL)){
            ledisLog(LEDIS_NOTICE, "The hash table %d is too sparse, resize it...", i);
            dictResize(server.dict[i]);
            ledisLog(LEDIS_NOTICE, "Hash table %d resized.", i);
        }
    }    

    //差不多每10秒，尝试清理超时的client
    if(!(loops % 5)){
        ledisLog(LEDIS_DEBUG, "%d clients connected (%d slaves)", listLength(server.clients), listLength(server.slaves));
    }

    if(!(loops % 10)){
        closeTimedoutClients();
    }

    //尝试bgsave，暂不实现
    if(server.bgsaveinprogress){
        //如果正在bgsave，则阻塞
        int statloc;
        //非阻塞式wait
        if(wait4(-1, &statloc, WNOHANG, NULL)){
            int exitcode = WEXITSTATUS(statloc);
            if(exitcode == 0){  //成功完成
                ledisLog(LEDIS_NOTICE, "Background saving terminated with success");
                server.dirty = 0;
                server.lastsave = time(NULL);
            }else{
                ledisLog(LEDIS_WARNING, "Background saving error");
            }
            server.bgsaveinprogress = 0;    //只有在这里还原标记
        }
    }else{
        //检测是否需要bgsave
        time_t now = time(NULL);
        for(int j = 0; j < server.saveparamslen; j++){
            struct saveparam *sp = server.saveparams+j;
            if(server.dirty >= sp->changes && now - server.lastsave > sp->seconds){
                ledisLog(LEDIS_NOTICE, "%d changes in %d seconds. Saving...",
                            sp->changes, sp->seconds);
                saveDbBackground("dump.ldb");
                break;
            }
        }
    }

    //如果是从服务端，还要检查与主的同步
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
    shared.zerobulk = createObject(LEDIS_STRING, sdsnew("0\r\n\r\n"));
    shared.nil = createObject(LEDIS_STRING, sdsnew("nil\r\n"));
    shared.zero = createObject(LEDIS_STRING, sdsnew("0\r\n"));
    shared.one = createObject(LEDIS_STRING, sdsnew("1\r\n"));
    shared.pong = createObject(LEDIS_STRING, sdsnew("+PONG\r\n"));

    //找不到key
    shared.minus1 = createObject(LEDIS_STRING, sdsnew("-1\r\n"));
    //key的type不对
    shared.minus2 = createObject(LEDIS_STRING, sdsnew("-2\r\n"));
    //src和dest是一样的
    shared.minus3 = createObject(LEDIS_STRING, sdsnew("-3\r\n"));
    //超出范围
    shared.minus4 = createObject(LEDIS_STRING, sdsnew("-4\r\n"));

    shared.wrongtypeerr = createObject(LEDIS_STRING, sdsnew("-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.wrongtypeerrbulk = createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "%d\r\n%s", -sdslen(shared.wrongtypeerr->ptr)+2, shared.wrongtypeerr->ptr));
    shared.nokeyerr = createObject(LEDIS_STRING, sdsnew("-ERR no such key\r\n"));
    shared.nokeyerrbulk = createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "%d\r\n%s", -sdslen(shared.nokeyerr->ptr)+2, shared.nokeyerr->ptr));

    shared.space = createObject(LEDIS_STRING, sdsnew(" "));
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
    server.bindaddr = NULL;
    server.glueoutputbuf = 1;

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

    server.clients = listCreate();
    server.slaves = listCreate();
    server.objfreelist = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    //初始化dict数组，只是分配了dbnum个地址空间，并没有为实际dict结构分配内存
    server.dict = malloc(sizeof(dict*)*server.dbnum);
    if(!server.dict || !server.clients || !server.slaves || !server.el || !server.objfreelist){
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
        server.dict[i] = dictCreate(&hashDictType, NULL);
        if(!server.dict[i]){
            oom("dictCreate");
        }
    }
    server.cronloops = 0;
    server.bgsaveinprogress = 0;
    server.lastsave = time(NULL);
    server.dirty = 0;
    aeCreateTimeEvent(server.el, 1000, serverCron, NULL, NULL);
}

/**
 * 清空server的所有dict
 */ 
static void emptyDb(){
    for(int i = 0; i < server.dbnum; i++){
        dictEmpty(server.dict[i]);
    }
}

/**
 * 从配置文件中加载，此版本实现的比较低端，只在启动时被调用一次
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
        if(!strcmp(argv[0], "timeout") && argc == 2){
            server.maxidletime = atoi(argv[1]);
            if(server.maxidletime < 1){
                err = "Invalid timeout value";
                goto loaderr;
            }
        }else if(!strcmp(argv[0], "port") && argc == 2){
            server.port = atoi(argv[1]);
            if(server.port < 1 || server.port > 65535){
                err = "Invalid port";
                goto loaderr;
            }
        }else if(!strcmp(argv[0], "bindaddr") && argc == 2){
            server.bindaddr = strdup(argv[1]);
        }else if(!strcmp(argv[0], "save") && argc == 3){
            int seconds = atoi(argv[1]);
            int changes = atoi(argv[2]);
            if(seconds < 1 || changes < 0){
                err = "Invalid save parameters";
                goto loaderr;
            }
            appendServerSaveParams(seconds, changes);
        }else if(!strcmp(argv[0], "dir") && argc == 2){
            if(chdir(argv[1]) == -1){
                ledisLog(LEDIS_WARNING, "Can't chdir to '%s': %s", argv[1], strerror(errno));
                exit(EXIT_FAILURE);
            }
        }else if(!strcmp(argv[0], "loglevel") && argc == 2){
            if(!strcmp(argv[1], "debug")){
                server.verbosity = LEDIS_DEBUG;
            }else if(!strcmp(argv[1], "notice")){
                server.verbosity = LEDIS_NOTICE;
            }else if(!strcmp(argv[1], "warning")){
                server.verbosity = LEDIS_WARNING;
            }else{
                err = "Invalid log level. Must be one of debug, notice, warning";
                goto loaderr;
            }
        }else if(!strcmp(argv[0], "logfile") && argc == 2){
            server.logfile = strdup(argv[1]);   //需要复制字符串，因为argv是函数范围
            if(!strcmp(server.logfile, "stdout")){
                free(server.logfile);
                server.logfile = NULL;
            }
            if(server.logfile){
                //如果不是stdout，则需要在这里测试一下文件的可读写性
                FILE *fp = fopen(server.logfile, "a");
                if(fp == NULL){
                    //为啥不用sscanf
                    err = sdscatprintf(sdsempty(), 
                        "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(fp);
            }
        }else if(!strcmp(argv[0], "databases") && argc ==2){
            //dbnum的值在这里可能变了，但是dict数组已经在之前已经分配16组空间了，疑似BUG
            server.dbnum = atoi(argv[1]);
            if(server.dbnum < 1){
                err = "Invalid number of databases";
                goto loaderr;
            }
        }else if(!strcmp(argv[0], "slaveof") && argc ==3){
            server.masterhost = sdsnew(argv[1]);
            server.masterport = atoi(argv[2]);
            server.replstate = LEDIS_REPL_CONNECT;  //说明是从
        }else if(!strcmp(argv[0], "glueoutputbuf") && argc == 2){
            sdstolower(argv[1]);
            if(!strcmp(argv[1], "yes")){
                server.glueoutputbuf = 1;
            }else if(!strcmp(argv[1], "no")){
                server.glueoutputbuf = 0;
            }else{
                err = "argument must be 'yes' or 'no'";
                goto loaderr;
            }
        }
        
        else{
            err = "Bad directive or wrong number of arguments";
            goto loaderr;
        }
        //追加清理argv
        for(int i = 0; i < argc; i++){
            sdsfree(argv[i]);
        }
        sdsfree(line);
    }
    fclose(fp);
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
        ln = listSearchKey(server.slaves, c);
        assert(ln != NULL);
        listDelNode(server.slaves, ln);
    }
    //如果客户端是主服务端反向连接过来的
    if(c->flags & LEDIS_MASTER){
        server.master = NULL;
        server.replstate = LEDIS_REPL_CONNECT;
    }
    free(c);
}

/**
 * 将client的reply链表统一输出
 */ 
static void glueReplyBuffersIfNeeded(ledisClient *c){

    listNode *ln = c->reply->head;
    lobj *o;
    int totlen = 0;
    while(ln){
        o = ln->value;
        totlen += sdslen(o->ptr);
        ln = ln->next;
        if(totlen > 1024) return;   //写死了
    }
    //开始发送
    if(totlen > 0){
        char buf[1024];
        int copylen = 0;
        listNode *next;
        ln = c->reply->head;
        while(ln){
            next = ln->next;
            o = ln->value;
            memcpy(buf+copylen, o->ptr, sdslen(o->ptr));
            copylen += sdslen(o->ptr);
            listDelNode(c->reply, ln);
            ln = next;
        }
        addReplySds(c, sdsnewlen(buf, totlen));
    }
}

static void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask){

    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);

    ledisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    lobj *o;
   
    //先尝试glue整体输出
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
            nwritten = write(fd, o->ptr + c->sentlen, objlen - c->sentlen);
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
        if(!strcmp(name, cmdTable[i].name)){
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

    sdstolower(c->argv[0]->ptr);

    if(!strcmp(c->argv[0]->ptr, "quit")){
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
    //运行命令，可是inline的，也可以是bulk类型的
    long long dirty = server.dirty;
    cmd->proc(c);
    //如果配置了从，而且cmd发生了数据变动，则触发从同步
    if(server.dirty - dirty != 0 && listLength(server.slaves)){
        replicationFeedSlaves(cmd, c->dictid, c->argv, c->argc);
    }
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
static void replicationFeedSlaves(struct ledisCommand *cmd, int dictid, lobj **argv, int argc){
    listNode *ln = server.slaves->head;
    lobj *outv[LEDIS_MAX_ARGS*4];
    int outc = 0;
    //将argv的obj数组，放到outv数组里面，用空格分开，注意每个元素（空格，换行符）都是一个单独的obj元素
    for(int i = 0; i < argc; i++){
        if(i != 0) outv[outc++] = shared.space;
        if((cmd->flags & LEDIS_CMD_BULK) && i == argc-1){   //特殊处理bulk类型
            lobj *lenobj = createObject(LEDIS_STRING, sdscatprintf(sdsempty(), "%d\r\n", sdslen(argv[i]->ptr)));
            lenobj->refcount = 0;   //要干啥
            outv[outc++] = lenobj;
        }
        outv[outc++] = argv[i];
    }
    outv[outc++] = shared.crlf;

    while(ln){
        ledisClient *slave = ln->value; //不用宏了？
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
        ln = ln->next;
    }
}

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask){
    //此版本只需要使用fd和privdata
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);
    //printf("readQueryFromClient\n");
    ledisClient *c = (ledisClient *)privdata;
    char buf[LEDIS_QUERYBUF_LEN];

    int nread = read(fd, buf, LEDIS_QUERYBUF_LEN);
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
            sdsfree(query); //再见了，c->querybuf已经指向新的结构了
            if(argv == NULL)    oom("sdssplitlen");
            for(int i = 0; i < argc && i < LEDIS_MAX_ARGS; i++){
                if(sdslen(argv[i])){
                    //终于把命令弄进去了
                    c->argv[c->argc] = createObject(LEDIS_STRING, argv[i]);
                    c->argc++;
                }else{
                    sdsfree(argv[i]);   //不赋值，则必须立刻回收
                }
            }
            free(argv); //split调用者还要负责回收
            //开始执行客户端命令，如果还有bulk数据或者后面还有新命令，还会回来继续判断
            if(processCommand(c) && sdslen(c->querybuf))    goto again;
        }else if(sdslen(c->querybuf) >= 1024){
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
    c->dict = server.dict[id];
    c->dictid = id;
    return LEDIS_OK;
}

static ledisClient *createClient(int fd){
    ledisClient *c = malloc(sizeof(*c));

    anetNonBlock(NULL, fd); //开启非阻塞
    anetTcpNoDelay(NULL, fd);   //开启TCP_NODELAY
    if(!c)  return NULL;
    selectDb(c, 0); //将客户端对接0号表
    //初始化各个字段
    c->fd = fd;
    c->querybuf = sdsempty();
    c->argc = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    if((c->reply = listCreate()) == NULL)   oom("listCreate");
    listSetFreeMethod(c->reply, decrRefCount);
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
    if(listLength(c->reply) == 0 && 
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
    if(createClient(cfd) == NULL){
        ledisLog(LEDIS_WARNING, "Error allocating resoures for the client");
        close(cfd); //状态此时不一定
        return;
    }
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

#if 0
static lobj *createHashObject(void){
    dict *d = dictCreate(&hashDictType, NULL);
    if(!d) oom("dictCreate");
    return createObject(LEDIS_SET, d);
}
#endif

/**
 * 创建一个set类型的对象，内部是个只有key的dict
 */ 
static lobj *createSetObject(void){
    dict *d = dictCreate(&setDictType, NULL);
    if(!d) oom("dictCreate");
    return createObject(LEDIS_SET, d);
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
            case LEDIS_HASH : freeHashObject(o);    break;
            default : assert(0 != 0);   break;
        }
        //引用为0,只是free里面的ptr数据，obj本身会加入到free列表首部，等待复用
        if(listLength(server.objfreelist) > LEDIS_OBJFREELIST_MAX || 
            !listAddNodeHead(server.objfreelist, o)){
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
 * 释放hash类型的obj
 */ 
static void freeHashObject(lobj *o){
    dictRelease((dict*)o->ptr);
}

/**
 * 释放set类型的obj
 */ 
static void freeSetObject(lobj *o){
    dictRelease((dict*)o->ptr);
}

/*====================================== DB SAVE/LOAD相关 ===============================*/

static int saveDb(char *filename){

    dictIterator *di = NULL;
    char tmpfile[256];
    //建立临时文件
    snprintf(tmpfile, 256, "temp-%d.%ld.rdb", (int)time(NULL), (long)random());

    FILE *fp = fopen(tmpfile, "w");
    if(!fp){
        ledisLog(LEDIS_WARNING," Failed saving the DB: %s", strerror(errno));
        return LEDIS_ERR;
    }
    //写死固定的开头标识字符
    if(fwrite("LEDIS0000", 9, 1, fp) == 0) goto werr;
    dictEntry *de;
    //需要用变量位数确定的类型，不能用int这类平台异同的类型
    uint8_t type;  
    uint32_t len;
    for(int i = 0; i < server.dbnum; i++){
        dict *d = server.dict[i];
        if(dictGetHashTableUsed(d) == 0) continue;
        di = dictGetIterator(d);
        if(!di){    //为啥不goto werr
            fclose(fp);
            return LEDIS_ERR;
        }

        //写当前DB的元信息，DB文件不一定在本机，所以多字节整型需要统一转大字序列
        type = LEDIS_SELECTDB;  //只有1个字节
        len = htonl(i); //4个字节必须要转
        if(fwrite(&type, 1, 1, fp) == 0) goto werr;
        if(fwrite(&len, 4, 1, fp) == 0) goto werr;

        //利用迭代器，遍历当前DB的每个entry
        while((de = dictNext(di)) != NULL){
            lobj *key = dictGetEntryKey(de);
            lobj *o = dictGetEntryVal(de);
            type = o->type;
            len = htonl(sdslen(key->ptr));
            //写入key信息
            if(fwrite(&type, 1, 1, fp) == 0) goto werr; //对应val的类型
            if(fwrite(&len, 4, 1, fp) == 0) goto werr;
            if(fwrite(key->ptr, sdslen(key->ptr), 1, fp) == 0) goto werr;
            //根据不同类型，写val信息
            if(type == LEDIS_STRING){
                sds sval = o->ptr;
                len = htonl(sdslen(sval));
                if(fwrite(&len, 4, 1, fp) == 0) goto werr;
                if(sdslen(sval) && fwrite(sval, sdslen(sval), 1, fp) == 0) goto werr;
            }else if(type == LEDIS_LIST){
                list *list = o->ptr;
                
                listNode *ln = list->head;
                len = htonl(listLength(list));  //先写list的长度
                if(fwrite(&len, 4, 1, fp) == 0) goto werr;
                while(ln){
                    lobj *eleobj = listNodeValue(ln);
                    len = htonl(sdslen(eleobj->ptr));
                    if(fwrite(&len, 4, 1, fp) == 0) goto werr;
                    if(sdslen(eleobj->ptr) && fwrite(eleobj->ptr, sdslen(eleobj->ptr), 1, fp) == 0) goto werr;
                    ln = ln->next;
                }
            }else if(type == LEDIS_SET){
                dict *set = o->ptr;
                dictIterator *di = dictGetIterator(set);
                dictEntry *de;

                if(!set) oom("dictGetIterator");
                len = htonl(dictGetHashTableUsed(set));
                if(fwrite(&len, 4, 1, fp) == 0) goto werr;
                while((de = dictNext(di)) != NULL){
                    lobj *eleobj = dictGetEntryKey(de);
                    len = htonl(sdslen(eleobj->ptr));
                    if(fwrite(&len, 4, 1, fp) == 0) goto werr;
                    if(sdslen(eleobj->ptr) && fwrite(eleobj->ptr, sdslen(eleobj->ptr), 1, fp) == 0) goto werr;
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
    type = LEDIS_EOF;
    if(fwrite(&type, 1, 1, fp) == 0) goto werr;
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

static int saveDbBackground(char *filename){
    if(server.bgsaveinprogress) return LEDIS_ERR;
    pid_t childpid;
    if((childpid = fork()) == 0){
        //子进程
        close(server.fd);   //不需要这个衍生品
        if(saveDb(filename) == LEDIS_OK){
            exit(EXIT_SUCCESS);
        }else{
            exit(EXIT_FAILURE);
        }
    }else{
        //主进程
        ledisLog(LEDIS_NOTICE, "Background saving started by pid %d", childpid);
        server.bgsaveinprogress = 1;    //在serverCron中才能得到子进程完毕的事件
        return LEDIS_OK;
    }
    return LEDIS_OK;    //到不了这里
}

static int loadDb(char *filename){

    char buf[LEDIS_LOADBUF_LEN];    //中转站
    char vbuf[LEDIS_LOADBUF_LEN];   //中转站
    char *key = NULL, *val = NULL;
    uint8_t type;
    uint32_t klen, vlen, dbid;
    int retval;
    dict *d = server.dict[0];

    FILE *fp = fopen(filename, "r");
    if(!fp) return LEDIS_ERR;
    //验证文件签名
    if(fread(buf, 9, 1, fp) == 0)   goto eoferr;
    if(memcmp(buf, "LEDIS0000", 9) != 0){
        fclose(fp);
        ledisLog(LEDIS_WARNING, "Wrong signature trying to load DB from file");
        return LEDIS_ERR;
    }

    while(true){
        lobj *o;

        //获取type，根据不同的type做不同的操作
        if(fread(&type, 1, 1, fp) == 0) goto eoferr;
        if(type == LEDIS_EOF) break;    //eof说明读完了
        if(type == LEDIS_SELECTDB){
            if(fread(&dbid, 4, 1, fp) == 0) goto eoferr;
            dbid = ntohl(dbid); //必须转
            if(dbid >= (unsigned)server.dbnum){
                ledisLog(LEDIS_WARNING, "FATAL: Data file was created with a Ledis server compilied to handle more than %d databases. Exiting\n", server.dbnum);
                exit(EXIT_FAILURE);
            }
            d = server.dict[dbid];
            continue;
        }

        //以下为正式数据type，后面肯定是key
        if(fread(&klen, 4, 1, fp) == 0) goto eoferr;
        klen = ntohl(klen);
        if(klen <= LEDIS_LOADBUF_LEN){
            key = buf;
        }else{
            key = malloc(klen);
            if(!key) oom("Loading DB from file");
        }
        if(fread(key, klen, 1, fp) == 0) goto eoferr;
        //处理val
        if(type == LEDIS_STRING){
            if(fread(&vlen, 4, 1, fp) == 0) goto eoferr;
            vlen = ntohl(vlen);
            if(vlen <= LEDIS_LOADBUF_LEN){
                val = vbuf;
            }else{
                val = malloc(vlen);
                if(!val) oom("Loading DB from file");
            }
            if(vlen && fread(val, vlen, 1, fp) == 0) goto eoferr;
            o = createObject(LEDIS_STRING, sdsnewlen(val, vlen));   //vlen可以为0，会构造没有buf的sds结构
        }else if(type == LEDIS_LIST || type == LEDIS_SET){
            uint32_t listlen;
            if(fread(&listlen, 4, 1, fp) == 0) goto eoferr;
            listlen = ntohl(listlen);
            o = (type == LEDIS_LIST) ? createListObject() : createSetObject();
            while(listlen--){
                lobj *ele;

                if(fread(&vlen, 4, 1, fp) == 0) goto eoferr;
                vlen = ntohl(vlen);
                if(vlen <= LEDIS_LOADBUF_LEN){
                    val = vbuf;
                }else{
                    val = malloc(vlen);
                    if(!val) oom("Loading DB from file");
                }
                
                if(vlen && fread(val, vlen, 1, fp) == 0) goto eoferr;   //vlen可以为0，会构造没有buf的sds结构
                ele = createObject(LEDIS_STRING, sdsnewlen(val, vlen));
                //添加到list或set的dict中
                if(type == LEDIS_LIST){
                    if(!listAddNodeTail((list*)o->ptr, ele)) oom("listAddNodeTail");
                }else{  //否则是SET
                    if(dictAdd((dict*)o->ptr, ele, NULL) == DICT_ERR) oom("dictAdd");
                }
                
                //要清理val
                if(val != vbuf) free(val);
                val = NULL;
            }
        }else{
            assert(0 != 0);
        }

        //lobj生成了，还需要弄到dict里
        retval = dictAdd(d, createStringObject(key, klen), o);
        if(retval == DICT_ERR){
            ledisLog(LEDIS_WARNING, "Loading DB, duplicated key found! Unrecoverable error, exiting now.");
            exit(EXIT_FAILURE);
        }

        //清理
        if(key != buf) free(key);
        if(val != vbuf) free(val);
        key = NULL;
        val = NULL;
    }

    fclose(fp);
    return LEDIS_OK;

eoferr:
    //并没有调用fclose(fp)？？？
    if(key != buf) free(key);
    if(val != vbuf) free(val);
    ledisLog(LEDIS_WARNING, "Short read loading DB. unrecoverable error, exiting now.");
    exit(EXIT_FAILURE); //load失败就直接退出
    return LEDIS_ERR;   //到不了，函数必须要返回1个值而已
}

/*====================================== 各种命令实现 ===============================*/

static void pingCommand(ledisClient *c){
    addReply(c, shared.pong);
}

static void echoCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(c->argv[1]->ptr)));
    addReply(c, c->argv[1]);
    addReply(c, shared.crlf);
    //已经被reply内部指向，要变成裸指针，防止被free
    c->argv[1] = NULL;
}

/*====================================== 字符串相关 ===============================*/

static void setGenericCommand(ledisClient *c, int nx){
   
    int retval = dictAdd(c->dict, c->argv[1], c->argv[2]);
    if(retval == DICT_ERR){
        if(!nx){    //如果是setCommand，直接覆盖原来的val
            dictReplace(c->dict, c->argv[1], c->argv[2]);
            incrRefCount(c->argv[2]); 
        }else{  //如果是setnxCommand，则撤销val
            addReply(c, shared.zero);
            return;
        }
    }else{
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    }
    server.dirty++;
    addReply(c, nx ? shared.one : shared.ok);
}

static void setCommand(ledisClient *c){
    setGenericCommand(c, 0);
}

static void setnxCommand(ledisClient *c){
    setGenericCommand(c, 1);
}

static void getCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.nil);
    }else{
        lobj *o = dictGetEntryVal(de);
        if(o->type != LEDIS_STRING){
            addReply(c, shared.wrongtypeerrbulk);
        }else{
            //正常情况
            addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(o->ptr)));
            addReply(c, o);
            addReply(c, shared.crlf);
        }
    }
}

static void incrDecrCommand(ledisClient *c, int incr){
    
    long long value;
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        value = 0;  //没有key则将val为0
    }else{
        lobj *o = dictGetEntryVal(de);
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
    //放冰箱里
    if(dictAdd(c->dict, c->argv[1], obj) == DICT_OK){
        incrRefCount(c->argv[1]);
    }else{
        //如果存在key，则直接替换
        dictReplace(c->dict, c->argv[1], obj);
    }
    server.dirty++; //无论如何都会改变
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
    int incr = atoi(c->argv[2]->ptr);
    incrDecrCommand(c, incr);
}

static void decrbyCommand(ledisClient *c){
    int incr = atoi(c->argv[2]->ptr);
    incrDecrCommand(c, -incr);
}

/*====================================== 其他相关 ===============================*/

static void delCommand(ledisClient *c){
    
    if(dictDelete(c->dict, c->argv[1]) == DICT_OK){
        server.dirty++;
        addReply(c, shared.ok);
    }else{
        addReply(c, shared.zero);
    }
}

static void existsCommand(ledisClient *c){
    
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
    }else{
        addReply(c, shared.one);
    }
}

static void selectCommand(ledisClient *c){
    int id = atoi(c->argv[1]->ptr);
    if(selectDb(c, id) == LEDIS_OK){
        addReply(c, shared.ok);
        addReply(c, shared.crlf);
    }else{
        addReplySds(c, sdsnew("-ERR invalid DB index\r\n"));    //原版直接返回C字符串，可能有问题，只有sds才能使用sds相关函数
    }
}

static void randomKeyCommand(ledisClient *c){
    dictEntry *de = dictGetRandomKey(c->dict);
    if(de){
        addReply(c, dictGetEntryKey(de));
        addReply(c, shared.crlf);
    }else{
        addReply(c, shared.crlf);
    }
}

static void keysCommand(ledisClient *c){
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern);
    int numkeys = 0, keyslen = 0;
    lobj *lenobj = createObject(LEDIS_STRING, NULL);

    dictIterator *di = dictGetIterator(c->dict);
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
            //匹配则加入结果字符串中，并用空格分隔
            if(numkeys != 0) addReply(c, shared.space);
            addReply(c, keyobj);
            numkeys++;
            keyslen += sdslen(key);
        }
    }
    dictReleaseIterator(di);
    //为啥数目和长度要加一起
    lenobj->ptr = sdscatprintf(sdsempty(), "%lu\r\n", keyslen+(numkeys ? (numkeys-1) : 0));
    addReply(c, shared.crlf);
}

static void dbsizeCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%lu\r\n", dictGetHashTableUsed(c->dict)));
}

static void lastsaveCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%lu\r\n", server.lastsave));
}

static void typeCommand(ledisClient *c){
    
    char *type;
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        type = "none";
    }else{
        lobj *o = dictGetEntryVal(de);
        switch(o->type){
            case LEDIS_STRING: type = "string"; break;
            case LEDIS_LIST: type = "list"; break;
            case LEDIS_SET: type = "set"; break;
            default: type = "unknown"; break;
        }
    }
    addReplySds(c, sdsnew(type));
    addReply(c, shared.crlf);
}

static void saveCommand(ledisClient *c){
    if(saveDb("dump.ldb") == LEDIS_OK){
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
    if(saveDbBackground("dump.ldb") == LEDIS_OK){
        addReply(c, shared.ok);
    }else{
        addReply(c, shared.err);
    }
}

static void shutdownCommand(ledisClient *c){
    ledisLog(LEDIS_WARNING, "User requested shutdown, saving DB...");
    if(saveDb("dump.ldb") == LEDIS_OK){
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
        if(nx){
            addReply(c, shared.minus3);
        }else{
            addReplySds(c, sdsnew("ERR src and dest are the same\r\n"));
        }
        return;
    }

    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        if(nx){
            addReply(c, shared.minus1);
        }else{
            addReply(c, shared.nokeyerr);
        }
        return;
    }
    //取出val
    lobj *o = dictGetEntryVal(de);
    incrRefCount(o);    //被弄到新的key里（引用加1），原来的val后面会被delete（引用减1）
    //尝试add
    if(dictAdd(c->dict, c->argv[2], o) == DICT_ERR){
        if(nx){
            //存在key则放弃
            decrRefCount(o);
            addReply(c, shared.zero);
            return;
        }else{
            dictReplace(c->dict, c->argv[2], o);
        }
    }else{
        incrRefCount(c->argv[2]);
    }
    dictDelete(c->dict, c->argv[1]);
    server.dirty++;
    addReply(c, nx ? shared.one : shared.ok);
}

static void renameCommand(ledisClient *c){
    renameGenericCommand(c, 0);
}
static void renamenxCommand(ledisClient *c){
    renameGenericCommand(c, 1);
}

static void moveCommand(ledisClient *c){
    dict *src = c->dict;    //备份指向
    int srcid = c->dictid;
    if(selectDb(c, atoi(c->argv[2]->ptr)) == LEDIS_ERR){
        addReply(c, shared.minus4);
        return;
    }
    dict *dst = c->dict;    //新的DB指向
    c->dict = src;  //指回去
    c->dictid = srcid;
    if(src == dst){ //不能一样
        addReply(c, shared.minus3);
        return;
    }

    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
        return;
    }

    //将key尝试放到新的DB里面
    lobj *key = dictGetEntryKey(de); //得是指针
    lobj *o = dictGetEntryVal(de);
    if(dictAdd(dst, key, o) == DICT_ERR){
        addReply(c, shared.zero);
        return;
    }
    incrRefCount(key);
    incrRefCount(o);

    //清理src的节点，但是只是移除结构并不能free，只是指向改变了而已
    dictDelete(src, c->argv[1]);
    server.dirty++;
    addReply(c, shared.one);
}

/*====================================== LIST相关 ===============================*/

static void pushGenericCommand(ledisClient *c, int where){
    
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    lobj *lobj;
    list *list;
    if(de == NULL){
        lobj = createListObject();
        list = lobj->ptr;
        if(where == LEDIS_HEAD){
            if(!listAddNodeHead(list, c->argv[2])) oom("listAddNodeHead");
        }else{
            if(!listAddNodeTail(list, c->argv[2])) oom("listAddNodeTail");
        }
        dictAdd(c->dict, c->argv[1], lobj);
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    }else{
        lobj = dictGetEntryVal(de);
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
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
        return;
    }else{
        lobj *o = dictGetEntryVal(de);
        if(o->type == LEDIS_LIST){
            addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", listLength((list*)o->ptr)));
        }else{
            addReply(c, shared.minus2);
            return;
        }
    }
}

static void lindexCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    int index = atoi(c->argv[2]->ptr);
    if(de == NULL){
        addReply(c, shared.nil);
        return;
    }else{
        lobj *o = dictGetEntryVal(de);
        if(o->type == LEDIS_LIST){
            list *list = o->ptr;
            listNode *node = listIndex(list, index);
            if(node == NULL){
                addReply(c, shared.nil);
                return;
            }else{
                lobj *ele = listNodeValue(node);
                //返回BULK类型
                addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(ele->ptr)));
                addReply(c, ele);
                addReply(c, shared.crlf);
                return;
            }
        }else{
            addReply(c, shared.wrongtypeerrbulk);
            return;
        }
    }
}

static void lsetCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    int index = atoi(c->argv[2]->ptr);
    if(de == NULL){
        addReply(c, shared.nokeyerr);
        return;
    }else{
        lobj *o = dictGetEntryVal(de);
        if(o->type != LEDIS_LIST){
            addReply(c, shared.wrongtypeerr);
            return;
        }else{
            //是list就可以尝试set了
            list *list = o->ptr;
            listNode *ln = listIndex(list, index);
            if(ln == NULL){
                addReplySds(c, sdsnew("-ERR index out of range\r\n"));
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
    
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.nil);
    }else{
       lobj *o = dictGetEntryVal(de);
       if(o->type != LEDIS_LIST){
           addReply(c, shared.wrongtypeerrbulk);
       }else{
           list *list = o->ptr;
           listNode *node;
           if(where == LEDIS_HEAD){
               node = listFirst(list);
           }else{
               node = listLast(list);
           }
           if(node == NULL){
               addReply(c, shared.nil);
           }else{
               lobj *ele = listNodeValue(node);
               addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(ele->ptr)));
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
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    if(de == NULL){
        addReply(c, shared.nil);
        return;
    }else{
        lobj *o = dictGetEntryVal(de);
        if(o->type == LEDIS_LIST){
            list *list = o->ptr;
            int llen = listLength(list);

            //校正入参start和end
            if(start < 0) start = llen + start;
            if(end < 0) end = llen + end;
            if(start < 0) start = 0;    //start为大负数，依然会小于0
            if(end < 0) end = 0;    //end为大负数，依然会小于0

            if(start > end || start >= llen){
                addReply(c, shared.zero);
                return;
            }
            if(end >= llen) end = llen - 1;
            int rangelen = (end - start) + 1;   //即最终截取多少个元素

            listNode *node = listIndex(list, start);
            //返回格式为多行BULK，即第一个数字是元素总个数
            addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", rangelen));
            lobj *ele;
            for(int i = 0; i < rangelen; i++){
                ele = listNodeValue(node);
                //返回BULK类型
                addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(ele->ptr)));
                addReply(c, ele);
                addReply(c, shared.crlf);
                node = node->next;
            }
            return;
        }else{
            addReply(c, shared.wrongtypeerrbulk);
            return;
        }
    }
}

static void ltrimCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    if(de == NULL){
        addReply(c, shared.nokeyerr);
        return;
    }else{
        lobj *o = dictGetEntryVal(de);
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
            addReply(c, shared.ok);
            server.dirty++; //只算一次变动
            return;
        }else{
            addReply(c, shared.wrongtypeerr);
            return;
        }
    }
}

/*====================================== SET相关 ===============================*/

static void saddCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    lobj *set;
    if(de == NULL){
        //没有这个key则新增，val为SET类型的obj
        set = createSetObject();
        dictAdd(c->dict, c->argv[1], set);  //find过了所以肯定成功
        incrRefCount(c->argv[1]);
    }else{
        //已经有key了，则追加
        set = dictGetEntryVal(de);
        if(set->type != LEDIS_SET){
            addReply(c, shared.minus2);
            return;
        }
    }
    //只是处理好了set本身，还要处理set里面dict的key们
    if(dictAdd(set->ptr, c->argv[2], NULL) == DICT_OK){
        server.dirty++;
        addReply(c, shared.one);
        return;
    }else{
        addReply(c, shared.zero);
        return;
    }
}

static void sremCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
        return;
    }else{
        lobj *set = dictGetEntryVal(de);
        if(set->type != LEDIS_SET){
            addReply(c, shared.minus2);
            return;
        }else{
            //尝试删除里面dict的key（也就是单个元素）
            if(dictDelete(set->ptr, c->argv[2]) == DICT_OK){
                server.dirty++;
                addReply(c, shared.one);
            }else{
                addReply(c, shared.zero);
            }
            return;
        }
    }
}

static void sismemberCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
        return;
    }else{
        lobj *set = dictGetEntryVal(de);
        if(set->type != LEDIS_SET){
            addReply(c, shared.minus2);
            return;
        }else{
            //查找
            if(dictFind(set->ptr, c->argv[2])){
                addReply(c, shared.one);
            }else{
                addReply(c, shared.zero);
            }
            return;
        }
    }
}

static void scardCommand(ledisClient *c){
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
        return;
    }else{
        lobj *set = dictGetEntryVal(de);
        if(set->type != LEDIS_SET){
            addReply(c, shared.minus2);
            return;
        }
        dict *s = set->ptr;
        addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", dictGetHashTableUsed(s)));
        return;
    }
}

static int qsortCompareSetsByCardinality(const void *s1, const void *s2){
    //注意s1和s2必须是指针，所以要多一层
    dict **d1 = (void *)s1;
    dict **d2 = (void *)s2;
    return dictGetHashTableUsed(*d1) - dictGetHashTableUsed(*d2);
}

/**
 * 取不同SET的交集，因此c->argc至少要为3
 * 此command还兼容smember命令，即只传一个SET的KEY，让取交集的for直接跳过，从此造成交集等于自身SET的情况，并依次返回
 */ 
static void sinterCommand(ledisClient *c){
    dict **dv = malloc(sizeof(dict*)*(c->argc-1));
    if(!dv) oom("sinterCommand");
    //尝试处理参数传来的每个SET
    for(int i = 0; i < c->argc-1; i++){
        dictEntry *de = dictFind(c->dict, c->argv[i+1]);
        if(de == NULL){ //每个key参数必须要有效
            free(dv);
            addReply(c, shared.nil);
            return;
        }
        lobj *setobj = dictGetEntryVal(de);
        if(setobj->type != LEDIS_SET){
            free(dv);
            addReply(c, shared.wrongtypeerrbulk);
            return;
        }
        dv[i] = setobj->ptr;
    }

    //开始处理dict数组中的每个dict了
    qsort(dv, c->argc-1, sizeof(dict*), qsortCompareSetsByCardinality); //将dv里面的dict按总量排序
    //返回类型为multi-bulk，但是现在还不知道SET最终交集的数目，所以先输出一个空字符串，在最后会修改实际值
    lobj *lenobj = createObject(LEDIS_STRING, NULL);
    addReply(c, lenobj);
    decrRefCount(lenobj);
    //开始迭代最小的SET，测试每一个在其他SET里面是否存在即可，有一个不在就不算
    dictIterator *di = dictGetIterator(dv[0]);
    if(!di) oom("dictGetIterator");

    dictEntry *de;
    int cardinality = 0;
    while((de = dictNext(di)) != NULL){
        lobj *ele;
        int j;
        //从dv的第2个元素开始比
        for(j = 1; j < c->argc-1; j++){
            //没找到直接退出for
            if(dictFind(dv[j], dictGetEntryKey(de)) == NULL) break;
        }
        if(j != c->argc-1) continue;    //如果不是最后一个SET，说明for是被break出来的，不是后面SET都有这个元素，换下一个元素尝试
        ele = dictGetEntryKey(de);
        addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", sdslen(ele->ptr)));
        addReply(c, ele);
        addReply(c, shared.crlf);
        cardinality++;
    }

    //因为是单线程的，所以向客户端write的操作要等到下一轮ae迭代了
    lenobj->ptr = sdscatprintf(sdsempty(), "%d\r\n", cardinality);  
    dictReleaseIterator(di);
    free(dv);
}

/*====================================== 主从相关 ===============================*/

/**
 * 发送所有未发的reply到客户端（即调sync命令的一端，从服务端），只在主从功能中被使用
 */ 
static int flushClientOutput(ledisClient *c){
    
    int retval;
    time_t start = time(NULL);

    while(listLength(c->reply)){
        if(time(NULL)-start > 5) return LEDIS_ERR;  //只让运行5秒
        retval = aeWait(c->fd, AE_WRITABLE, 1000);
        if(retval == -1){
            return LEDIS_ERR;
        }else if(retval & AE_WRITABLE){
            sendReplyToClient(NULL, c->fd, c, AE_WRITABLE);
        }
    }
    return LEDIS_OK;
}

/**
 * 本质还是调用write向fd写入数据，但要以间歇的方式，并且考虑数据分批
 */ 
static int syncWrite(int fd, void *ptr, ssize_t size, int timeout){

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
static int syncRead(int fd, void *ptr, ssize_t size, int timeout){

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
    
    struct stat sb;
    int fd = -1;
    time_t start = time(NULL);
    char sizebuf[32];   //dump文件的大小
    ledisLog(LEDIS_NOTICE, "slave ask for syncronization");
    if(flushClientOutput(c) == LEDIS_ERR || saveDb("dump.ldb") != LEDIS_OK){
        goto closeconn;
    }

    fd = open("dump.ldb", O_RDONLY);
    if(fd == -1 || fstat(fd, &sb) == -1) goto closeconn;
    int len = sb.st_size;

    snprintf(sizebuf, 32, "%d\r\n", len);
    //先返回文件大小
    if(syncWrite(c->fd, sizebuf, strlen(sizebuf), 5) == -1) goto closeconn;
    //直接传递整个ldb文件。。。
    while(len){
        char buf[1024];
        
        //60秒的同步时间
        if(time(NULL)-start > LEDIS_MAX_SYNC_TIME) goto closeconn;
        int nread = read(fd, buf, 1024);
        if(nread == -1) goto closeconn;
        len -= nread;
        if(syncWrite(c->fd, buf, nread, 5) == -1) goto closeconn;
    }
    if(syncWrite(c->fd, "\r\n", 2, 5) == -1) goto closeconn;
    close(fd);
    c->flags |= LEDIS_SLAVE;
    c->slaveseldb = 0;
    if(!listAddNodeTail(server.slaves, c)) oom("listAddNodeTail");
    ledisLog(LEDIS_NOTICE, "Syncronization with slave succeeded");
    return;
closeconn:
    if(fd != -1) close(fd);
    c->flags |= LEDIS_CLOSE;
    ledisLog(LEDIS_WARNING, "Syncronization with slave failed");
    return;
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
    if(syncReadLine(fd, buf, 1024, 5) == -1){
        close(fd);
        ledisLog(LEDIS_WARNING, "I/O error reading bulk count from MASTER: %s", 
                    strerror(errno));
        return LEDIS_ERR;
    }
    //先获取到的是dump文件大小
    int dumpsize = atoi(buf);
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
    if(rename(tmpfile, "dump.ldb") == -1){
        ledisLog(LEDIS_WARNING, "Failed trying to rename the temp DB into dump.ldb in MASTER <-> SLAVE synchronization: %s", 
                    strerror(errno));
        unlink(tmpfile);
        close(fd);
        return LEDIS_ERR;
    }
    emptyDb();
    if(loadDb("dump.ldb") != LEDIS_OK){
        ledisLog(LEDIS_WARNING, "Failed trying to load the MASTER synchronization DB from disk");
        close(fd);
        return LEDIS_ERR;
    }
    server.master = createClient(fd);
    server.master->flags |= LEDIS_MASTER;
    server.replstate = LEDIS_REPL_CONNECTED;
    return LEDIS_OK;
}

/*====================================== 主函数 ===============================*/

int main(int argc, char *argv[]){

    LEDIS_NOTUSED(argc);
    LEDIS_NOTUSED(argv);
    //初始化server配置
    initServerConfig();
    if(argc == 2){  //制定了conf文件
        ResetServerSaveParams();    //清空saveparams字段
        loadServerConfig(argv[1]);
        ledisLog(LEDIS_NOTICE, "Configuration loaded");
    }else if(argc > 2){
        fprintf(stderr, "Usage: ./ledis-server [/path/to/ledis.conf]\n");
        exit(EXIT_FAILURE);
    }
    //初始化server
    initServer();
    ledisLog(LEDIS_NOTICE, "Server started");
    //尝试恢复数据库dump.ldb文件
    if(loadDb("dump.ldb") == LEDIS_OK){
        ledisLog(LEDIS_NOTICE, "DB loaded from disk");
    }
    //假定恢复db用了5s
    //sleep(5);

    //基于server的fd，创建fileEvent
    if(aeCreateFileEvent(server.el, server.fd, AE_READABLE,
            acceptHandler, NULL, NULL) == AE_ERR){
        oom("creating file event");
    }
    ledisLog(LEDIS_NOTICE, "The server is now ready to accept connections");
    aeMain(server.el);  //开始轮询，直到el的stop被置位
    aeDeleteEventLoop(server.el);
    exit(EXIT_SUCCESS);
}