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
    printf("ping command result: %s", res);
}

void echoCommandTest(char *err, int sfd){

    char *msg = "hello!";
    char data[1024];
    char res[1024];
    sprintf(data, "echo %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("echo command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, 1024);
    printf("echo command resultLength: %d\n", result);
    printf("echo command result: %s", res);
}

int main(int argc, char *argv[]){
    
    char err[ANET_ERR_LEN];
    int sfd = anetTcpConnect(err, "127.0.0.1", 6379);
    if(sfd == -1){
        fprintf(stderr, "anetTcpConnect\n");
        exit(EXIT_FAILURE);
    }

    //测试ping命令
    pingCommandTest(err, sfd);
    //测试echo命令
    echoCommandTest(err, sfd);

    //sleep(3600);

    exit(EXIT_SUCCESS);
}