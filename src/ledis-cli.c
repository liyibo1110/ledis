#include "fmacros.h"

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
    {"get",2,LEDIS_CMD_INLINE},
    {"set",3,LEDIS_CMD_BULK},
    {"setnx",3,LEDIS_CMD_BULK},
    {"del",2,LEDIS_CMD_INLINE},
    {"exists",2,LEDIS_CMD_INLINE},
    {"incr",2,LEDIS_CMD_INLINE},
    {"decr",2,LEDIS_CMD_INLINE},
    {"rpush",3,LEDIS_CMD_BULK},
    {"lpush",3,LEDIS_CMD_BULK},
    {"rpop",2,LEDIS_CMD_INLINE},
    {"lpop",2,LEDIS_CMD_INLINE},
    {"llen",2,LEDIS_CMD_INLINE},
    {"lindex",3,LEDIS_CMD_INLINE},
    {"lset",4,LEDIS_CMD_BULK},
    {"lrange",4,LEDIS_CMD_INLINE},
    {"ltrim",4,LEDIS_CMD_INLINE},
    {"lrem",4,LEDIS_CMD_BULK},
    {"sadd",3,LEDIS_CMD_BULK},
    {"srem",3,LEDIS_CMD_BULK},
    {"sismember",3,LEDIS_CMD_BULK},
    {"scard",2,LEDIS_CMD_INLINE},
    {"sinter",-2,LEDIS_CMD_INLINE},
    {"sinterstore",-3,LEDIS_CMD_INLINE},
    {"smembers",2,LEDIS_CMD_INLINE},
    {"incrby",3,LEDIS_CMD_INLINE},
    {"decrby",3,LEDIS_CMD_INLINE},
    {"randomkey",1,LEDIS_CMD_INLINE},
    {"select",2,LEDIS_CMD_INLINE},
    {"move",3,LEDIS_CMD_INLINE},
    {"rename",3,LEDIS_CMD_INLINE},
    {"renamenx",3,LEDIS_CMD_INLINE},
    {"keys",2,LEDIS_CMD_INLINE},
    {"dbsize",1,LEDIS_CMD_INLINE},
    {"ping",1,LEDIS_CMD_INLINE},
    {"echo",2,LEDIS_CMD_BULK},
    {"save",1,LEDIS_CMD_INLINE},
    {"bgsave",1,LEDIS_CMD_INLINE},
    {"shutdown",1,LEDIS_CMD_INLINE},
    {"lastsave",1,LEDIS_CMD_INLINE},
    {"type",2,LEDIS_CMD_INLINE},
    {"flushdb",1,LEDIS_CMD_INLINE},
    {"flushall",1,LEDIS_CMD_INLINE},
    {"sort",-2,LEDIS_CMD_INLINE},
    {"info",1,LEDIS_CMD_INLINE},
    {"mget",-2,LEDIS_CMD_INLINE},
    {"expire",3,LEDIS_CMD_INLINE},
    {NULL,0,0}
};

static int cliReadReply(int fd);

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
static int cliReadSingleLineReply(int fd, int type){

    sds reply = cliReadLine(fd);
    if(reply == NULL) return 1;
    printf("%s\n", reply);
    return 0;
}

/**
 * 读取并输出bulk类型的reply（包含BULK），MULTIBULK也会调用，但是只能取到一部分数据
 * 返回0说明正常，返回1说明不正常
 */ 
static int cliReadBulkReply(int fd){

    sds replylen = cliReadLine(fd); //先读取bulk的长度信息，也可能是nil

    if(replylen == NULL) return 1; 
    
    int bulklen = atoi(replylen);
    if(bulklen == -1){ //如果是多行bulk，bulklen应该是集合的长度，而不是信息大小
        sdsfree(replylen);
        printf("(nil)");    //multi来的，没有换行
        return 0;   //正常
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
    if(isatty(fileno(stdout)) && reply[bulklen-1] != '\n'){
        printf("\n");
    }
    zfree(reply);
    return 0;
}

static int cliReadMultiBulkReply(int fd){
    sds replylen = cliReadLine(fd);
    if(replylen == NULL) return 1;

    int elements = atoi(replylen);
    if(elements == -1){
        sdsfree(replylen);
        printf("(nil)\n");
        return 0;
    }
    if(elements == 0){
        printf("(empty list or set)\n");
    }

    int c = 1;
    while(elements--){
        printf("%d. ", c);  //前缀编号
        if(cliReadReply(fd)) return 1;   //如果内部返回1（错误），直接也返回错误
        printf("\n");
    }
    return 0;
}

static int cliReadReply(int fd){
    char type;
    //读第1个字符，表示信息类型
    if(anetRead(fd, &type, 1) <= 0) exit(EXIT_SUCCESS);
    switch(type){
        case '-':{
            printf("(error)");
            cliReadSingleLineReply(fd);
            return 1;
        }
        case '+':
        case ':':
            return cliReadSingleLineReply(fd);
        case '$':{
            return cliReadMultiBulkReply(fd);
        }
        default:{
            printf("protocol error, get '%c' as reply type byte\n", type);
            return 1;
        }
    }
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
    int retval = cliReadReply(fd);
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