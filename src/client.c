#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "anet.h"

void pingCommandTest(char *err, int sfd){

    char *data = "ping\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    int result = read(sfd, res, 1024);
    printf("ping command resultLength: %d\n", result);
    printf("ping command result: %s\n", res);
}

void echoCommandTest(char *err, int sfd){

    char *msg = "hello!";
    char data[1024];
    char res[1024];
    memset(res, 0, 1024);
    sprintf(data, "echo %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("echo command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, 1024);
    printf("echo command resultLength: %d\n", result);
    printf("echo command result: %s\n", res);
}



void setCommandTest(char *err, int sfd){
    
    char *msg = "myvalue";
    char data[1024];
    char res[1024];
    memset(res, 0, 1024);
    sprintf(data, "set mykey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("set command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, 1024);
    printf("set command resultLength: %d\n", result);
    printf("set command result: %s\n", res);
}

void setnxCommandTest(char *err, int sfd){
    
    char *msg = "yourvalue";
    char data[1024];
    char res[1024];
    memset(res, 0, 1024);
    sprintf(data, "setnx mykey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("setnx command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, 1024);
    printf("setnx command resultLength: %d\n", result);
    printf("setnx command result: %s\n", res);
}

void keysCommandTest(char *err, int sfd){
    
    char *data = "keys *\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("keys command resultLength: %d\n", result);
    printf("keys command result: %s\n", res);
}

void renameCommandTest(char *err, int sfd){
    
    char *data = "rename mykey yourkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("rename command resultLength: %d\n", result);
    printf("rename command result: %s\n", res);
}

void renamenxCommandTest(char *err, int sfd){
    
    char *data = "renamenx yourkey theirkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("renamenx command resultLength: %d\n", result);
    printf("renamenx command result: %s\n", res);
}

void moveCommandTest(char *err, int sfd){
    
    char *data = "move theirkey 1\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("move command resultLength: %d\n", result);
    printf("move command result: %s\n", res);
}

void getCommandTest(char *err, int sfd){
    
    char *data = "get mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("get command resultLength: %d\n", result);
    printf("get command result: %s\n", res);
}

void existsCommandTest(char *err, int sfd){
    
    char *data = "exists mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("exists command resultLength: %d\n", result);
    printf("exists command result: %s\n", res);
}

void incrCommandTest(char *err, int sfd){
    
    char *data = "incr mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("incr command resultLength: %d\n", result);
    printf("incr command result: %s\n", res);
}

void decrCommandTest(char *err, int sfd){
    
    char *data = "decr mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("decr command resultLength: %d\n", result);
    printf("decr command result: %s\n", res);
}

void dbsizeCommandTest(char *err, int sfd){

    char *data = "dbsize\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("dbsize command resultLength: %d\n", result);
    printf("dbsize command result: %s\n", res);
}

void randomKeyCommandTest(char *err, int sfd){
    
    char *data = "randomKey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("randomKey command resultLength: %d\n", result);
    printf("randomKey command result: %s\n", res);
}

void lastsaveCommandTest(char *err, int sfd){
    
    char *data = "lastsave\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("lastsave command resultLength: %d\n", result);
    printf("lastsave command result: %s\n", res);
}

void delCommandTest(char *err, int sfd){
    
    char *data = "del mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("del command resultLength: %d\n", result);
    printf("del command result: %s\n", res);
}

void selectCommandTest(char *err, int sfd){
    
    char *data = "select 1\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("select command resultLength: %d\n", result);
    printf("select command result: %s\n", res);
}

void shutdownCommandTest(char *err, int sfd){
    
    char *data = "shutdown\r\n";
    anetWrite(sfd, data, (int)strlen(data));
}

int main(int argc, char *argv[]){
    
    char err[ANET_ERR_LEN];
    int sfd = anetTcpConnect(err, "127.0.0.1", 6379);
    if(sfd == -1){
        fprintf(stderr, "anetTcpConnect\n");
        exit(EXIT_FAILURE);
    }

    //测试ping命令
    //pingCommandTest(err, sfd);
    //测试echo命令
    //echoCommandTest(err, sfd);
    //测试dbsize命令
    dbsizeCommandTest(err, sfd);
    //测试set命令
    setCommandTest(err, sfd);
    //测试setnx命令
    setnxCommandTest(err, sfd);
    //测试keys命令
    keysCommandTest(err, sfd);
    //测试get命令
    getCommandTest(err, sfd);
    //测试exists命令
    existsCommandTest(err, sfd);
    //测试incr命令
    incrCommandTest(err, sfd);
    //测试decr命令
    decrCommandTest(err, sfd);
    //测试randomKey命令
    randomKeyCommandTest(err, sfd);
    //测试lastsave命令
    lastsaveCommandTest(err, sfd);
    //测试rename命令
    renameCommandTest(err, sfd);
    //测试renamenx命令
    renamenxCommandTest(err, sfd);
    //测试move命令
    moveCommandTest(err, sfd);
    //测试shutdown命令
    //shutdownCommandTest(err, sfd);
    //测试del命令
    //delCommandTest(err, sfd);
    //测试select命令
    selectCommandTest(err, sfd);
    //sleep(3600);

    exit(EXIT_SUCCESS);
}