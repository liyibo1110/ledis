#define _GNU_SOURCE
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
#include <netdb.h>

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
#define LEDIS_HT_MINFILL    10  //dict实际占用百分数，小于这个比率就会尝试再收缩（前提是扩张过）
#define LEDIS_HT_MINSLOTS   16384   //最小容量，如果不扩长，也就不会继续收缩


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

static void freeClient(ledisClient *c);
static void addReply(ledisClient *c, lobj *obj);
static void addReplySds(ledisClient *c, sds s);

//所有函数均只在本文件被调用
static void pingCommand(ledisClient *c);
static void echoCommand(ledisClient *c);
static void dbsizeCommand(ledisClient *c);

static void setCommand(ledisClient *c);
static void setnxCommand(ledisClient *c);
static void getCommand(ledisClient *c);
static void keysCommand(ledisClient *c);
static void delCommand(ledisClient *c);
static void existsCommand(ledisClient *c);
static void incrCommand(ledisClient *c);
static void decrCommand(ledisClient *c);
static void selectCommand(ledisClient *c);
static void randomKeyCommand(ledisClient *c);
static void lastsaveCommand(ledisClient *c);
static void shutdownCommand(ledisClient *c);

static void renameCommand(ledisClient *c);
static void renamenxCommand(ledisClient *c);
static void moveCommand(ledisClient *c);

static void lpushCommand(ledisClient *c);
static void rpushCommand(ledisClient *c);
static void llenCommand(ledisClient *c);
/*====================================== 全局变量 ===============================*/
static struct ledisServer server;
static struct ledisCommand cmdTable[] = {
    {"ping",pingCommand,1,LEDIS_CMD_INLINE},
    {"echo",echoCommand,2,LEDIS_CMD_BULK},
    {"dbsize",dbsizeCommand,1,LEDIS_CMD_INLINE},
    {"set",setCommand,3,LEDIS_CMD_BULK},
    {"setnx",setnxCommand,3,LEDIS_CMD_BULK},
    {"get",getCommand,2,LEDIS_CMD_INLINE},
    {"keys",keysCommand,2,LEDIS_CMD_INLINE},
    {"del",delCommand,2,LEDIS_CMD_INLINE},
    {"exists",existsCommand,2,LEDIS_CMD_INLINE},
    {"incr",incrCommand,2,LEDIS_CMD_INLINE},
    {"decr",decrCommand,2,LEDIS_CMD_INLINE},
    {"select",selectCommand,2,LEDIS_CMD_INLINE},
    {"randomKey",randomKeyCommand,1,LEDIS_CMD_INLINE},
    {"lastsave",lastsaveCommand,1,LEDIS_CMD_INLINE},
    {"shutdown",shutdownCommand,1,LEDIS_CMD_INLINE},
    {"rename",renameCommand,3,LEDIS_CMD_INLINE},
    {"renamenx",renamenxCommand,3,LEDIS_CMD_INLINE},
    {"move",moveCommand,3,LEDIS_CMD_INLINE},
    {"lpush",lpushCommand,3,LEDIS_CMD_BULK},
    {"rpush",rpushCommand,3,LEDIS_CMD_BULK},
    {"llen",llenCommand,2,LEDIS_CMD_INLINE},
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
        //检查上次交互的时间
        if(now - c->lastinteraction > server.maxidletime){
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
        ledisLog(LEDIS_DEBUG, "%d clients connected", listLength(server.clients));
    }

    if(!(loops % 10)){
        closeTimedoutClients();
    }

    //尝试bgsave，暂不实现
    if(server.bgsaveinprogress){
        //如果正在bgsave，则阻塞
    }else{
        //检测是否需要bgsave
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
    printf("server.fd=%d\n", server.fd);
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
        argv = sdssplitlen(line, sdslen(line), " ", 1, &argc);

        //开始实施这一行的配置
        if(!strcmp(argv[0], "timeout") && argc == 2){
            server.maxidletime = atoi(argv[1]);
            if(server.maxidletime < 1){
                err = "Invalid timeout value";
                goto loaderr;
            }
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
            if(!strcmp(server.logfile, "stdout"))   server.logfile = NULL;
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
        }else{
            err = "Bad directive or wrong number of arguments";
            goto loaderr;
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
        sdsfree(c->argv[i]);
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
    free(c);
}

static void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask){

    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);

    ledisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    lobj *o;
    while(listLength(c->reply)){
        o = listNodeValue(listFirst(c->reply));
        objlen = sdslen(o->ptr);    //实际字符串长度
        if(objlen == 0){
            listDelNode(c->reply, listFirst(c->reply));
            continue;
        }

        //开始写入client，分段写入，并没有用到anet.c里面的anetWrite函数
        nwritten = write(fd, o->ptr + c->sentlen, objlen - c->sentlen);
        printf("nwritten=%d\n", nwritten);
        if(nwritten <= 0)   break;
        c->sentlen += nwritten;
        totwritten += nwritten;
        //检查是否写完了，可能因为网络或者client问题，造成部分写入，则进入新的循环再写剩下的（sentlen保存了已写的字节数）
        if(c->sentlen == objlen){
            //说明写完一个reply了
            listDelNode(c->reply, listFirst(c->reply));
            c->sentlen = 0;
        }
        printf("reply over.\n");
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

    //sdstolower(c->argv[0]);

    if(!strcmp(c->argv[0], "quit")){
        freeClient(c);
        return 0;
    }

    struct ledisCommand *cmd = lookupCommand(c->argv[0]);

    if(!cmd){
        addReplySds(c, sdsnew("-ERR unknown command\r\n"));
        resetClient(c);
        return 1;
    }else if(cmd->arity != c->argc){
        addReplySds(c, sdsnew("-ERR wrong number of arguments\r\n"));
        resetClient(c);
        return 1;
    }else if(cmd->type == LEDIS_CMD_BULK && c->bulklen == -1){
        int bulklen = atoi(c->argv[c->argc-1]); //获取后面bulk数据的长度
        sdsfree(c->argv[c->argc-1]);    //转成int原来的就没用了
        if(bulklen < 0 || bulklen > 1024*1024*1024){
            c->argc--;
            c->argv[c->argc] = NULL;
            addReplySds(c, sdsnew("-ERR invalid bulk write count\r\n"));
            resetClient(c);
            return 1;
        }
        //清理
        c->argv[c->argc-1] = NULL;
        c->argc--;
        c->bulklen = bulklen + 2;   //bulk数据后面还有\r\n要计算，解析时会被跳过
        //检查querybuf里有没有bulk数据，这个不一定有，没有则要退回readQueryFromClient的again中
        if((signed)sdslen(c->querybuf) >= c->bulklen){
            //有则填充最后一个argv，用真正的bulk参数
            c->argv[c->argc] = sdsnewlen(c->querybuf, c->bulklen - 2);  //bulk实际数据不包括\r\n
            c->argc++;
            c->querybuf = sdsrange(c->querybuf, c->bulklen, -1);    //包括bulk数据和\r\n一起跳过
        }else{
            return 1;
        }
    }
    //运行命令，可是inline的，也可以是bulk类型的
    cmd->proc(c);
    resetClient(c);
    return 1;
}

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask){
    //此版本只需要使用fd和privdata
    LEDIS_NOTUSED(el);
    LEDIS_NOTUSED(mask);
    printf("readQueryFromClient\n");
    ledisClient *c = (ledisClient *)privdata;
    char buf[LEDIS_QUERYBUF_LEN];

    int nread = read(fd, buf, LEDIS_QUERYBUF_LEN);
    printf("nread=%d\n", nread);
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
        printf("%s", c->querybuf);
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
            if(argv == NULL)    oom("Splitting query in token");
            for(int i = 0; i < argc && i < LEDIS_MAX_ARGS; i++){
                if(sdslen(argv[i])){
                    //终于把命令弄进去了
                    c->argv[c->argc] = argv[i];
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
            c->argv[c->argc] = sdsnewlen(c->querybuf, c->bulklen - 2);  //填充bulk数据
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
    return LEDIS_OK;
}

static int createClient(int fd){
    ledisClient *c = malloc(sizeof(*c));

    anetNonBlock(NULL, fd); //开启非阻塞
    anetTcpNoDelay(NULL, fd);   //开启TCP_NODELAY
    if(!c)  return LEDIS_ERR;
    selectDb(c, 0); //将客户端对接0号表
    //初始化各个字段
    c->fd = fd;
    c->querybuf = sdsempty();
    c->argc = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->lastinteraction = time(NULL);
    if((c->reply = listCreate()) == NULL)   oom("listCreate");
    listSetFreeMethod(c->reply, decrRefCount);
    //将client的fd也加入到eventLoop中（存在2种fd，监听新请求/建立起来的客户端后续请求）
    if(aeCreateFileEvent(server.el, fd, AE_READABLE, readQueryFromClient, c, NULL) == AE_ERR){
        freeClient(c);
        return LEDIS_ERR;
    }
    //添加到server.clients的尾部
    if(!listAddNodeTail(server.clients, c)) oom("listAddNodeTail");
    return LEDIS_OK;
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
    printf("sdf=%d\n", fd);
    int cfd = anetAccept(server.neterr, fd, cip, &cport);
    printf("cdf=%d\n", cfd);
    if(cfd == AE_ERR){
        ledisLog(LEDIS_DEBUG, "Accepting client connection: %s", server.neterr);
        return;
    }
    //得到客户端fd了
    ledisLog(LEDIS_DEBUG, "Accepted %s:%d", cip, cport);
    //根据得到的fd创建client结构
    if(createClient(cfd) == LEDIS_ERR){
        ledisLog(LEDIS_WARNING, "Error allocating resoures for the client");
        close(cfd); //状态此时不一定
        return;
    }
    printf("createClient is ok!\n");
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

/*====================================== 各种命令实现 ===============================*/

static void pingCommand(ledisClient *c){
    addReply(c, shared.pong);
}

static void echoCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(c->argv[1])));
    addReplySds(c, c->argv[1]);
    addReply(c, shared.crlf);
    //已经被reply内部指向，要变成裸指针，防止被free
    c->argv[1] = NULL;
}

static void dbsizeCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%lu\r\n", dictGetHashTableUsed(c->dict)));
}

static void setGenericCommand(ledisClient *c, int nx){
    lobj *o = createObject(LEDIS_STRING, c->argv[2]);
    c->argv[2] = NULL;
    int retval = dictAdd(c->dict, c->argv[1], o);
    if(retval == DICT_ERR){
        if(!nx){    //如果是setCommand，直接覆盖原来的val
            dictReplace(c->dict, c->argv[1], o); 
        }else{  //如果是setnxCommand，则撤销val
            decrRefCount(o);
        }
    }else{
        //已经被dict内部指向，要变成裸指针，防止被free
        c->argv[1] = NULL;
    }
    server.dirty++;
    addReply(c, shared.ok);
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
            char *err = "GET against key not holding a string value";
            //err是局部的，但sdscatprintf会去复制字符串
            addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n%s\r\n", -((int)strlen(err)), err));
        }else{
            //正常情况
            addReplySds(c, sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(o->ptr)));
            addReply(c, o);
            addReply(c, shared.crlf);
        }
    }
}

static void delCommand(ledisClient *c){
    
    if(dictDelete(c->dict, c->argv[1]) == DICT_OK){
        server.dirty++;
    }
    addReply(c, shared.ok);
}

static void existsCommand(ledisClient *c){
    
    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReply(c, shared.zero);
    }else{
        addReply(c, shared.one);
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
    sds newval = sdscatprintf(sdsempty(), "%lld", value);
    lobj *obj = createObject(LEDIS_STRING, newval);
    //放冰箱里
    if(dictAdd(c->dict, c->argv[1], obj) == DICT_OK){
        //已经被dict内部指向，要变成裸指针，防止被free
        c->argv[1] = NULL;
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

static void selectCommand(ledisClient *c){
    int id = atoi(c->argv[1]);
    if(selectDb(c, id) == LEDIS_OK){
        addReply(c, shared.ok);
    }else{
        addReplySds(c, sdsnew("-ERR invalid DB index\r\n"));    //原版直接返回C字符串，可能有问题，只有sds才能使用sds相关函数
    }
}

static void randomKeyCommand(ledisClient *c){
    
    dictEntry *de = dictGetRandomKey(c->dict);
    if(de){
        lobj *o = dictGetEntryVal(de);
        addReply(c, o);
        addReply(c, shared.crlf);
    }else{
        addReply(c, shared.crlf);
    }
}

static void keysCommand(ledisClient *c){
    sds pattern = c->argv[1];
    int plen = sdslen(c->argv[1]);
    dictIterator *di = dictGetIterator(c->dict);
    sds keys = sdsempty();

    //遍历dict，寻找匹配的key
    dictEntry *de;
    while((de = dictNext(di)) != NULL){
        sds key = dictGetEntryKey(de);
        if((pattern[0] == '*' && pattern[1] == '\0') || 
            stringmatchlen(pattern, plen, key, sdslen(key), 0)){
            //匹配则加入结果字符串中，并用空格分隔
            keys = sdscatlen(keys, key, sdslen(key));
            keys = sdscatlen(keys, " ", 1);
        }
    }
    dictReleaseIterator(di);
    keys = sdstrim(keys, " ");  //trim
    sds reply = sdscatprintf(sdsempty(), "%lu\r\n", sdslen(keys));
    reply = sdscatlen(reply, keys, sdslen(keys));
    reply = sdscatlen(reply, "\r\n", 2);
    sdsfree(keys);  //必须
    addReplySds(c, reply);
}

static void lastsaveCommand(ledisClient *c){
    addReplySds(c, sdscatprintf(sdsempty(), "%lu\r\n", server.lastsave));
}

static void shutdownCommand(ledisClient *c){
    //暂时没有save
    ledisLog(LEDIS_NOTICE, "server exit now, bye bye...");
    exit(EXIT_SUCCESS);
}

static void renameGenericCommand(ledisClient *c, int nx){
    
    //新旧key不能一样
    if(sdscmp(c->argv[1], c->argv[2]) == 0){
        ledisLog(LEDIS_WARNING, "ERR src and dest are the same\r\n");
        return;
    }

    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReplySds(c, sdsnew("-ERR no such key\r\n"));
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
            addReplySds(c, sdsnew("-ERR destination key exists"));
            return;
        }else{
            dictReplace(c->dict, c->argv[2], o);
        }
    }else{
        //如果ok，argv[2]里面的key已经被dict的结构指向了，所以最好干掉argv[1]的指向，以防被free
        c->argv[2] = NULL;
    }
    dictDelete(c->dict, c->argv[1]);
    server.dirty++;
    addReply(c, shared.ok);
}

static void renameCommand(ledisClient *c){
    renameGenericCommand(c, 0);
}
static void renamenxCommand(ledisClient *c){
    renameGenericCommand(c, 1);
}

static void moveCommand(ledisClient *c){
    dict *src = c->dict;    //备份指向
    if(selectDb(c, atoi(c->argv[2])) == LEDIS_ERR){
        addReplySds(c, sdsnew("-ERR target DB out of range\r\n"));
        return;
    }
    dict *dst = c->dict;    //新的DB指向
    c->dict = src;  //指回去

    if(src == dst){ //不能一样
        addReplySds(c, sdsnew("-ERR source DB is the same as target DB\r\n"));
        return;
    }

    dictEntry *de = dictFind(c->dict, c->argv[1]);
    if(de == NULL){
        addReplySds(c, sdsnew("-ERR no such key\r\n"));
        return;
    }

    //将key尝试放到新的DB里面
    sds *key = dictGetEntryKey(de); //得是指针
    lobj *o = dictGetEntryVal(de);
    if(dictAdd(dst, key, o) == DICT_ERR){
        addReplySds(c, sdsnew("-ERR target DB already contains the moved key\r\n"));
        return;
    }

    //清理src的节点，但是只是移除结构并不能free，只是指向改变了而已
    dictDeleteNoFree(src, c->argv[1]);
    server.dirty++;
    addReply(c, shared.ok);
}

static void pushGenericCommand(ledisClient *c, int where){
    
    lobj *ele = createObject(LEDIS_STRING, c->argv[2]);
    c->argv[2] = NULL;

    dictEntry *de = dictFind(c->dict, c->argv[1]);
    lobj *lobj;
    list *list;
    if(de == NULL){
        lobj = createListObject();
        list = lobj->ptr;
        if(where == LEDIS_HEAD){
            if(!listAddNodeHead(list, ele)) oom("listAddNodeHead");
        }else{
            if(!listAddNodeTail(list, ele)) oom("listAddNodeTail");
        }
        dictAdd(c->dict, c->argv[1], lobj);
        c->argv[1] = NULL;  //变成裸指针，防止arv[1]被free
    }else{
        lobj = dictGetEntryVal(de);
        //检查类型，必须是list
        if(lobj->type != LEDIS_LIST){
            decrRefCount(ele);
            addReplySds(c, sdsnew("-ERR push against existing key not holding a list\r\n"));
            return;
        }
        list = lobj->ptr;
        if(where == LEDIS_HEAD){
            if(!listAddNodeHead(list, ele)) oom("listAddNodeHead");
        }else{
            if(!listAddNodeTail(list, ele)) oom("listAddNodeTail");
        }
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
            addReplySds(c, sdsnew("-1\r\n"));
            return;
        }
    }
}

/*====================================== 主函数 ===============================*/

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
    //尝试恢复数据库dump.ldb文件，暂不开发

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