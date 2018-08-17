#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "anet.h"

void echoCommandTest(char *err, int sfd){

    char *msg = "hello!";
    char data[1024];
    char res[1024];
    sprintf(data, "echo %d\r\n%s", (int)strlen(msg), msg);

    printf("sfd=%d\n", sfd);
    printf("msg=%s\n", msg);
    printf("data=%s\n", data);

    int result1 = write(sfd, data, (int)strlen(data));

    printf("write1 ok, result1=%d\n", result1);

    int result2 = write(sfd, "\r\n", 2);
    printf("write2 ok, result2=%d\n", result2);
    //开始读
    int result3 = read(sfd, res, 1024);
    printf("result3: %s\n", res);
}

int main(int argc, char *argv[]){
    
    char err[ANET_ERR_LEN];
    int sfd = anetTcpConnect(err, "127.0.0.1", 6379);
    if(sfd == -1){
        fprintf(stderr, "anetTcpConnect\n");
        exit(EXIT_FAILURE);
    }

    //测试echo命令
    echoCommandTest(err, sfd);

    exit(EXIT_SUCCESS);
}