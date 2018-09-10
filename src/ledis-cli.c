#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>

#include <netdb.h>

#include "anet.h"
#include "sds.h"
#include "adlist.h"
#include "zmalloc.h"

#define LEDIS_CMD_INLINE 1
#define LEDIS_CMD_BULK 2
#define LEDIS_CMD_INTREPLY 4
#define LEDIS_CMD_RETCODEREPLY 8
#define LEDIS_CMD_BULKREPLY 16
#define LEDIS_CMD_MULTIBULKREPLY 32
#define LEDIS_CMD_SINGLELINEREPLY 64

#define LEDIS_NOTUSED(V) ((void) V)

static struct config{
    char *hostip;
    int hostport;
} config;

struct ledisCommand{
    char *name;
    int arity;
    int flags;
};

static struct ledisCommand cmdTable[] = {
    {"get",2,LEDIS_CMD_INLINE|LEDIS_CMD_BULKREPLY},
    {"set",3,LEDIS_CMD_BULK|LEDIS_CMD_RETCODEREPLY},
    {"setnx",3,LEDIS_CMD_BULK|LEDIS_CMD_INTREPLY},
    {"del",2,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"exists",2,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"incr",2,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"decr",2,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"rpush",3,LEDIS_CMD_BULK|LEDIS_CMD_RETCODEREPLY},
    {"lpush",3,LEDIS_CMD_BULK|LEDIS_CMD_RETCODEREPLY},
    {"rpop",2,LEDIS_CMD_INLINE|LEDIS_CMD_BULKREPLY},
    {"lpop",2,LEDIS_CMD_INLINE|LEDIS_CMD_BULKREPLY},
    {"llen",2,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"lindex",3,LEDIS_CMD_INLINE|LEDIS_CMD_BULKREPLY},
    {"lset",4,LEDIS_CMD_BULK|LEDIS_CMD_RETCODEREPLY},
    {"lrange",4,LEDIS_CMD_INLINE|LEDIS_CMD_MULTIBULKREPLY},
    {"ltrim",4,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"lrem",4,LEDIS_CMD_BULK|LEDIS_CMD_INTREPLY},
    {"sadd",3,LEDIS_CMD_BULK|LEDIS_CMD_INTREPLY},
    {"srem",3,LEDIS_CMD_BULK|LEDIS_CMD_INTREPLY},
    {"sismember",3,LEDIS_CMD_BULK|LEDIS_CMD_INTREPLY},
    {"scard",2,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"sinter",-2,LEDIS_CMD_INLINE|LEDIS_CMD_MULTIBULKREPLY},
    {"sinterstore",-3,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"smembers",2,LEDIS_CMD_INLINE|LEDIS_CMD_MULTIBULKREPLY},
    {"incrby",3,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"decrby",3,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"randomkey",1,LEDIS_CMD_INLINE|LEDIS_CMD_SINGLELINEREPLY},
    {"select",2,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"move",3,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"rename",3,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"renamenx",3,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"keys",2,LEDIS_CMD_INLINE|LEDIS_CMD_BULKREPLY},
    {"dbsize",1,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"ping",1,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"echo",2,LEDIS_CMD_BULK|LEDIS_CMD_BULKREPLY},
    {"save",1,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"bgsave",1,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"shutdown",1,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"lastsave",1,LEDIS_CMD_INLINE|LEDIS_CMD_INTREPLY},
    {"type",2,LEDIS_CMD_INLINE|LEDIS_CMD_SINGLELINEREPLY},
    {"flushdb",1,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"flushall",1,LEDIS_CMD_INLINE|LEDIS_CMD_RETCODEREPLY},
    {"sort",-2,LEDIS_CMD_INLINE|LEDIS_CMD_MULTIBULKREPLY},
    {"info",1,LEDIS_CMD_INLINE|LEDIS_CMD_BULKREPLY},
    {"mget",-2,LEDIS_CMD_INLINE|LEDIS_CMD_MULTIBULKREPLY},
    {NULL,0,0}
};

static struct ledisCommand *lookupCommand(char *name){
    int i = 0;
    while(cmdTable[i].name != NULL){
        if(!strcasecmp(name, cmdTable[i].name)) return &cmdTable[i];
        i++;
    }
    //找一圈也没找到
    return NULL;
}

/**
 * 以NoDelay方式连接server
 */ 
static int cliConnect(void){
    
    char err[ANET_ERR_LEN];
    int fd = anetTcpConnect(err, config.hostip, config.hostport);
    if(fd == ANET_ERR){
        fprintf(stderr, "Connect: %s\n", err);
        return -1;
    } 
    anetTcpNoDelay(NULL, fd);
    return fd;
}

static sds cliReadLine(int fd){

    sds line = sdsempty();
    //逐个字符读取
    while(true){
        char c;
        ssize_t ret = read(fd, &c, 1);
        if(ret == -1){  //出错了
            sdsfree(line);
            return NULL;
        }else if((ret == 0) || (c == '\n')){    //EOF或者\n就完事儿
            break;
        }else{
            line = sdscatlen(line, &c, 1);  //追加
        }
    }
    return sdstrim(line, "\r\n");   //去掉\r\n
}

/**
 * 读取并输出inline类型的reply（包含3种，对应SINGLELINE，INT和RETCODE）
 * 返回0说明正常，返回1说明不正常
 */ 
static int cliReadInlineReply(int fd, int type){

    sds reply = cliReadLine(fd);
    if(reply == NULL) return 1;
    printf("%s\n", reply);
    if(type == LEDIS_CMD_SINGLELINEREPLY) return 0; //对的
    if(type == LEDIS_CMD_INTREPLY) return atoi(reply) < 0;  //如果是负数，说明是错误，要返回1
    if(type == LEDIS_CMD_RETCODEREPLY) return reply[0] == '-';  //如果是-，说明是错误，要返回1
    return 0;
}

/**
 * 读取并输出bulk类型的reply（包含BULK），MULTIBULK也会调用，但是只能取到一部分数据
 * 返回0说明正常，返回1说明不正常
 */ 
static int cliReadBulkReply(int fd, int multibulk){

    int error = 0; //错误标记
    sds replylen = cliReadLine(fd); //先读取bulk的长度信息，也可能是nil

    if(replylen == NULL) return 1; 
    if(strcmp(replylen, "nil") == 0){
        sdsfree(replylen);
        printf("(nil)\n");
        return 0;   //正常
    }
    int bulklen = atoi(replylen);
    if(multibulk && bulklen == -1){ //如果是多行bulk，bulklen应该是集合的长度，而不是信息大小
        sdsfree(replylen);
        printf("(nil)");    //multi来的，没有换行
        return 0;   //正常
    }
    if(bulklen < 0){    //server返回错误信息，是通过将bulklen变成负值来标识，但是信息还是有用的
        bulklen = -bulklen;
        error = 1;
    }
    char *reply = zmalloc(bulklen); //信息长度是动态的
    char crlf[2];
    //读取实际信息
    anetRead(fd, reply, bulklen);
    anetRead(fd, crlf, 2);
    if(bulklen && fwrite(reply, bulklen, 1, stdout) == 0){  //错误
        zfree(reply);
        return 1;
    }
    if(!multibulk && isatty(fileno(stdout)) && reply[bulklen-1] != '\n'){
        printf("\n");
    }
    zfree(reply);
    return error;
}

static int cliReadMultiBulkReply(int fd){
    sds replylen = cliReadLine(fd);
    if(replylen == NULL) return 1;
    if(strcmp(replylen, "nil") == 0){
        sdsfree(replylen);
        printf("(nil)\n");
        return 0;
    }

    int elements = atoi(replylen);  //容器的长度
    int c = 1;
    while(elements--){
        printf("%d. ", c);  //前缀编号
        if(cliReadBulkReply(fd, 1)) return 1;   //如果内部返回1（错误），直接也返回错误
        printf("\n");
    }
    return 0;
}

/**
 * 处理实际的命令（不包括程序名，argv[0]即为命令的开始）
 */ 
static int cliSendCommand(int argc, char **argv){

    int fd;
    struct ledisCommand *lc = lookupCommand(argv[0]);
    sds cmd = sdsempty();

    //验证合法
    if(!lc){
        fprintf(stderr, "Unknown command '%s'\n", argv[0]);
        return 1;
    }
    if((lc->arity > 0 && argc != lc->arity) || 
        (lc->arity < 0 && argc < -lc->arity)){
        fprintf(stderr, "Wrong number of arguments for '%s'\n", lc->name);
        return 1;
    }
    if((fd = cliConnect()) == -1) return 1;

    //准备发送
    for(int i = 0; i < argc; i++){
        if(i != 0) cmd = sdscat(cmd, " ");  //空格分隔
        if(i == argc-1 && lc->flags & LEDIS_CMD_BULK){  //bulk类型的实际数据，只写长度部分
            cmd = sdscatprintf(cmd, "%d", sdslen(argv[i]));
        }else{  //非bulk类型，直接写信息
            cmd = sdscatlen(cmd, argv[i], sdslen(argv[i]));
        }
    }
    cmd = sdscat(cmd, "\r\n");  //不管是不是bulk都得加
    if(lc->flags & LEDIS_CMD_BULK){ //额外处理bulk真实数据
        cmd = sdscatlen(cmd, argv[argc-1], sdslen(argv[argc-1]));
        cmd = sdscat(cmd, "\r\n");
    }
    anetWrite(fd, cmd, sdslen(cmd));
    //准备读取
    int retval = 0;
    if(lc->flags & LEDIS_CMD_INTREPLY){
        retval = cliReadInlineReply(fd, LEDIS_CMD_INTREPLY);
    }else if(lc->flags & LEDIS_CMD_RETCODEREPLY){
        retval = cliReadInlineReply(fd, LEDIS_CMD_RETCODEREPLY);
    }else if(lc->flags & LEDIS_CMD_SINGLELINEREPLY){
        retval = cliReadInlineReply(fd, LEDIS_CMD_SINGLELINEREPLY);
    }else if(lc->flags & LEDIS_CMD_BULKREPLY){
        retval = cliReadBulkReply(fd, 0);
    }else if(lc->flags & LEDIS_CMD_MULTIBULKREPLY){
        retval = cliReadMultiBulkReply(fd);
    }
    if(retval){
        close(fd);
        return retval;
    }
    close(fd);
    return 0;
}

/**
 * 返回第一个非配置参数的下标
 */ 
static int parseOptions(int argc, char **argv){

    int i;
    for(i = 1; i < argc; i++){
        int lastarg = i==argc-1;    //是否是最后一个参数
        if(!strcmp(argv[i], "-h") && !lastarg){
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
        }else{
            break;  //直接退出，让函数返回当前其他参数第一个下标
        }
    }
    return i;
}

/**
 * 从其他来源读取命令参数（例如管道输出）
 */ 
static sds readArgFromStdin(void){
    char buf[1024];
    sds arg = sdsempty();

    while(true){
        int nread = read(fileno(stdin), buf, 1024);
        if(nread == 0){ //读完了
            break;  
        }else if(nread == -1){
            perror("Reading from standard input");  //输出字面量内容，再输出errno
            exit(EXIT_FAILURE);
        }
        arg = sdscatlen(arg, buf, nread);   //追加
    }
    return arg;
}

int main(int argc, char *argv[]){

    config.hostip = "127.0.0.1";
    config.hostport = 6379;

    int firstarg = parseOptions(argc, argv);
    argc -= firstarg;   //剩余的真实参数，不包括原来的程序名
    argv += firstarg;   //真实参数的起始位置

    char **argvcopy = zmalloc(sizeof(char*)*argc+1);    //额外的1，是给管道输入的
    for(int i = 0; i < argc; i++){
        argvcopy[i] = sdsnew(argv[i]);  //弄成sds
    }

    //命令最后一个参数，可以对应非终端来源，例如管道输出
    if(!isatty(fileno(stdin))){
        sds lastarg = readArgFromStdin();
        argvcopy[argc] = lastarg;
        argc++;
    }

    if(argc < 1){
        fprintf(stderr, "usage: ledis-cli [-h host] [-p port] cmd arg1 arg2 arg3 ... argN\n");
        fprintf(stderr, "usage: echo \"argN\" | ledis-cli [-h host] [-p port] cmd arg1 arg2 ... arg(N-1)\n");
        fprintf(stderr, "\nIf a pipe from standard input is detected this data is used as last argument.\n\n");
        fprintf(stderr, "example: cat /etc/passwd | ledis-cli set my_passwd\n");
        fprintf(stderr, "example: ledis-cli get my_passwd\n");
        exit(EXIT_FAILURE);
    }

    return cliSendCommand(argc, argvcopy);
}