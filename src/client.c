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

void dbsizeCommandTest(char *err, int sfd){

    char *data = "dbsize\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[1024];
    memset(res, 0, 1024);
    int result = read(sfd, res, 1024);
    printf("dbsize command resultLength: %d\n", result);
    printf("dbsize command result: %s", res);
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
    //dbsizeCommandTest(err, sfd);
    //测试set命令
    setCommandTest(err, sfd);
    //测试setnx命令
    setnxCommandTest(err, sfd);
    //测试get命令
    getCommandTest(err, sfd);
    //测试exists命令
    existsCommandTest(err, sfd);
    //测试del命令
    delCommandTest(err, sfd);
    //sleep(3600);

    exit(EXIT_SUCCESS);
}