#ifndef __ANET_H
#define __ANET_H

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

int anetTcpConnect(char *err, char *addr, int port);
int anetTcpServer(char *err, int port, char *bindaddr);
int anetAccept(char *err, int serversock, char *ip, int *port);
int anetResolve(char *err, char *host, char *ipbuf);

int anetNonBlock(char *err, int fd);
int anetTcpNoDelay(char *err, int fd);
int anetTcpKeepAlive(char *err, int fd);
int anetSetSendBuffer(char *err, int fd, int bufferSize);

int anetRead(int fd, void *buf, int count);
int anetWrite(int fd, void *buf, int count);

#endif