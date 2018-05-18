#include "adlist.h"
#include <stdlib.h>

list *listCreate(void){
    list *l = malloc(sizeof(*l));
    if(l == NULL) return NULL;
    l->head = NULL;
    l->tail = NULL;
    l->dup = NULL;
    l->free = NULL;
    l->match = NULL;
    l->len = 0;
    return l;
}

void listRelease(list *list){
    listNode *next;
    listNode *current = list->head;
    int len = list->len;
    while(len--){   //遍历释放Node
        next = current->next;
        if(list->free) list->free(current->value);  //定义了free则调用
        free(current);
        current = next;
    }
    free(list);
}

list *listAddNodeHead(list *list, void *value){
    listNode *node = malloc(sizeof(*node));
    if(node == NULL) return NULL;
    node->value = value;
    if(list->len == 0){ //链表如果为空
        node->prev = NULL;
        node->next = NULL;
        list->head = node;
        list->tail = node;
    }else{
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
    return list;
}

list *listAddNodeTail(list *list, void *value){
    listNode *node = malloc(sizeof(*node));
    if(node == NULL) return NULL;
    node->value = value;
    if(list->len == 0){ //链表如果为空
        node->prev = NULL;
        node->next = NULL;
        list->head = node;
        list->tail = node;
    }else{
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    list->len++;
    return list;
}

void listDelNode(list *list, listNode *node){
    //尝试从链表移除自身
    if(node->prev){
        node->prev->next = node->next;
    }else{  //说明要删的是首元素
        list->head = node->next;
    }
    if(node->next){
        node->next->prev = node->prev;
    }else{  //说明要删的是尾元素
        list->tail = node->prev;
    }
    if(list->free)  list->free(node->value);
    free(node);
    list->len--;
}