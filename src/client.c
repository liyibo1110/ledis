#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "anet.h"

#define WRITE_BUF_SIZE 1024
#define READ_BUF_SIZE 1024
#define CLIENT_NOTUSED(V) ((void)V)

void pingCommandTest(char *err, int sfd){

    CLIENT_NOTUSED(err);
    char *data = "ping\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("ping command resultLength: %d\n", result);
    printf("ping command result: %s\n", res);
}

void echoCommandTest(char *err, int sfd){

    CLIENT_NOTUSED(err);
    char *msg = "hello!";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "echo %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("echo command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("echo command resultLength: %d\n", result);
    printf("echo command result: %s\n", res);
}

void setCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "myvalue";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "set mykey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("set command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("set command resultLength: %d\n", result);
    printf("set command result: %s\n", res);
}

void setnxCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "yourvalue";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "setnx mykey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("setnx command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("setnx command resultLength: %d\n", result);
    printf("setnx command result: %s\n", res);
}

void keysCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "keys *\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("keys command resultLength: %d\n", result);
    printf("keys command result: %s\n", res);
}

void renameCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "rename mykey yourkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("rename command resultLength: %d\n", result);
    printf("rename command result: %s\n", res);
}

void renamenxCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "renamenx yourkey theirkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("renamenx command resultLength: %d\n", result);
    printf("renamenx command result: %s\n", res);
}

void moveCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "move theirkey 1\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("move command resultLength: %d\n", result);
    printf("move command result: %s\n", res);
}

void getCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "get mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("get command resultLength: %d\n", result);
    printf("get command result: %s\n", res);
}

void existsCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "exists mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("exists command resultLength: %d\n", result);
    printf("exists command result: %s\n", res);
}

void incrCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "incr mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("incr command resultLength: %d\n", result);
    printf("incr command result: %s\n", res);
}

void decrCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "decr mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("decr command resultLength: %d\n", result);
    printf("decr command result: %s\n", res);
}

void dbsizeCommandTest(char *err, int sfd){

    CLIENT_NOTUSED(err);
    char *data = "dbsize\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("dbsize command resultLength: %d\n", result);
    printf("dbsize command result: %s\n", res);
}

void randomKeyCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "randomKey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("randomKey command resultLength: %d\n", result);
    printf("randomKey command result: %s\n", res);
}

void lastsaveCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "lastsave\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lastsave command resultLength: %d\n", result);
    printf("lastsave command result: %s\n", res);
}

void delCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "del mykey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("del command resultLength: %d\n", result);
    printf("del command result: %s\n", res);
}

void lpushCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "mylistvalue1";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "lpush mylistkey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("lpush command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lpush command resultLength: %d\n", result);
    printf("lpush command result: %s\n", res);
}

void rpushCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "mylistvalue2";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "rpush mylistkey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("rpush command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("rpush command resultLength: %d\n", result);
    printf("rpush command result: %s\n", res);
}

void llenCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "llen mylistkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("llen command resultLength: %d\n", result);
    printf("llen command result: %s\n", res);
}

void lindexCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "lindex mylistkey 1\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lindex command resultLength: %d\n", result);
    printf("lindex command result: %s\n", res);
}

void lrangeCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "lrange mylistkey 0 1\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lrange command resultLength: %d\n", result);
    printf("lrange command result: %s\n", res);
}

void saveCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "save\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("save command resultLength: %d\n", result);
    printf("save command result: %s\n", res);
}

void bgsaveCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "bgsave\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("bgsave command resultLength: %d\n", result);
    printf("bgsave command result: %s\n", res);
}

void lsetCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "mylistvalue3";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "lset mylistkey 0 %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("lset command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lset command resultLength: %d\n", result);
    printf("lset command result: %s\n", res);
}

void lpopCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "lpop mylistkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lpop command resultLength: %d\n", result);
    printf("lpop command result: %s\n", res);
}

void rpopCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "rpop mylistkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("rpop command resultLength: %d\n", result);
    printf("rpop command result: %s\n", res);
}

void selectCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "select 1\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("select command resultLength: %d\n", result);
    printf("select command result: %s\n", res);
}

void saddCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "set1";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "sadd mysetkey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("sadd command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));
    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("sadd command1 resultLength: %d\n", result);
    printf("sadd command1 result: %s\n", res);

    //再来一个
    msg = "set2";
    sprintf(data, "sadd mysetkey %d\r\n%s\r\n", (int)strlen(msg), msg);
    printf("sadd command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));
    //开始读
    result = read(sfd, res, READ_BUF_SIZE);
    printf("sadd command2 resultLength: %d\n", result);
    printf("sadd command2 result: %s\n", res);

}

void sismemberCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "set2";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "sismember mysetkey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("sismember command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lismember command resultLength: %d\n", result);
    printf("lismember command result: %s\n", res);
}

void smembersCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "smembers mysetkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("smembers command resultLength: %d\n", result);
    printf("smembers command result: %s\n", res);
}

void sremCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *msg = "set2";
    char data[WRITE_BUF_SIZE];
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    sprintf(data, "srem mysetkey %d\r\n%s\r\n", (int)strlen(msg), msg);
    
    printf("srem command send: %s\n", data);
    anetWrite(sfd, data, (int)strlen(data));

    //开始读
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("lrem command resultLength: %d\n", result);
    printf("lrem command result: %s\n", res);
}

void scardCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "scard mysetkey\r\n";
    anetWrite(sfd, data, (int)strlen(data));
    
    //开始读
    char res[READ_BUF_SIZE];
    memset(res, 0, READ_BUF_SIZE);
    int result = read(sfd, res, READ_BUF_SIZE);
    printf("scard command resultLength: %d\n", result);
    printf("scard command result: %s\n", res);
}

void shutdownCommandTest(char *err, int sfd){
    
    CLIENT_NOTUSED(err);
    char *data = "shutdown\r\n";
    anetWrite(sfd, data, (int)strlen(data));
}

int main(int argc, char *argv[]){
    
    CLIENT_NOTUSED(argc);
    CLIENT_NOTUSED(argv);
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
    //setCommandTest(err, sfd);
    //测试setnx命令
    //setnxCommandTest(err, sfd);
    //测试keys命令
    //keysCommandTest(err, sfd);
    //测试get命令
    //getCommandTest(err, sfd);
    //测试exists命令
    //existsCommandTest(err, sfd);
    //测试incr命令
    //incrCommandTest(err, sfd);
    //测试decr命令
    //decrCommandTest(err, sfd);
    //测试randomKey命令
    //randomKeyCommandTest(err, sfd);
    //测试lastsave命令
    //lastsaveCommandTest(err, sfd);
    //测试rename命令
    //renameCommandTest(err, sfd);
    //测试renamenx命令
    //renamenxCommandTest(err, sfd);
    //测试move命令
    //moveCommandTest(err, sfd);

    //测试lpush命令
    lpushCommandTest(err, sfd);
    //测试rpush命令
    rpushCommandTest(err, sfd);
    //测试llen命令
    //llenCommandTest(err, sfd);
    //测试lindex命令
    //lindexCommandTest(err, sfd);
    //测试lrange命令
    //lrangeCommandTest(err, sfd);
    //测试save命令
    //saveCommandTest(err, sfd);
    //测试bgsave命令
    //bgsaveCommandTest(err, sfd);
    //测试lset命令
    lsetCommandTest(err, sfd);
    //测试lpop命令
    lpopCommandTest(err, sfd);
    //测试rpop命令
    rpopCommandTest(err, sfd);
    //测试shutdown命令
    //shutdownCommandTest(err, sfd);
    //测试del命令
    //delCommandTest(err, sfd);
    //测试select命令
    selectCommandTest(err, sfd);

    //测试sadd命令
    saddCommandTest(err, sfd);
    //测试sismember命令
    sismemberCommandTest(err, sfd);
    //测试smembers命令
    smembersCommandTest(err, sfd);
    //测试srem命令
    sremCommandTest(err, sfd);
    //测试scard命令
    scardCommandTest(err, sfd);

    //sleep(3600);

    exit(EXIT_SUCCESS);
}