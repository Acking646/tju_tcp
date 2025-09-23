#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#include <netinet/in.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <stdbool.h> // 使用 bool 类型
#include <signal.h>	 // 使用 signal 函数

#define SERVER_IP "172.17.0.6"
#define CLIENT_IP "172.17.0.5"

// 初始化序列号
#define SERVER_ISN 0
#define CLIENT_ISN 0

#define FIN_SEQ 0

// 重传线程信号
bool RETRANS;
// 超时标志
bool TIMEOUT_FLAG;

// 单位是byte
#define SIZE32 4
#define SIZE16 2
#define SIZE8 1

// 一些Flag
#define NO_FLAG 0
#define NO_WAIT 1
#define TIMEOUT 2
#define TRUE 1
#define FALSE 0

// 定义最大包长 防止IP层分片
#define MAX_DLEN 1375 // 最大包内数据长度
#define MAX_LEN 1400  // 最大包长度

// TCP socket 状态定义
#define CLOSED 0
#define LISTEN 1
#define SYN_SENT 2
#define SYN_RECV 3
#define ESTABLISHED 4
#define FIN_WAIT_1 5
#define FIN_WAIT_2 6
#define CLOSE_WAIT 7
#define CLOSING 8
#define LAST_ACK 9
#define TIME_WAIT 10

// TCP 拥塞控制状态
#define SLOW_START 0
#define CONGESTION_AVOIDANCE 1
#define FAST_RECOVERY 2

// TCP 接受窗口大小
#define TCP_RECVWN_SIZE 32 * MAX_DLEN // 比如最多放32个满载数据包

#define MAX_BUF_SIZE 1000 * MAX_LEN // 最多放140个10KB

// 最大窗口大小
#define MAX_WINDOW_SIZE 32 * MAX_DLEN

// TCP 发送窗口
// 注释的内容如果想用就可以用 不想用就删掉 仅仅提供思路和灵感
typedef struct
{
	uint32_t window_size; // 由发送来的adv_window来决定, adv_window根据接收方的接收缓冲区决定
	uint32_t base;
	uint32_t nextseq; // base 与 nextseq 之间是“已发未确认”区段
	//   uint32_t estmated_rtt;
	int ack_cnt;
	uint32_t same_ack_cnt;
	// pthread_mutex_t ack_cnt_lock;
	struct timeval send_time;
	struct itimerval timeout;
	//   int congestion_status;
	//   uint16_t cwnd;
	//   uint16_t ssthresh;
} sender_window_t;

// TCP 接受窗口
// 注释的内容如果想用就可以用 不想用就删掉 仅仅提供思路和灵感
typedef struct
{
	// char *wnd_recv_buf;
	uint32_t wnd_available_size; // 接收窗口还可以放入数据的大小

	//   received_packet_t* head;
	//   char buf[TCP_RECVWN_SIZE];
	uint8_t *marked;
	uint32_t expect_seq; // 当前希望收到的序号，等于 rcv_nxt
} receiver_window_t;

// TCP 窗口 每个建立了连接的TCP都包括发送和接受两个窗口
typedef struct
{
	sender_window_t *wnd_send;
	receiver_window_t *wnd_recv;
} window_t;

typedef struct
{
	uint32_t ip;
	uint16_t port;
} tju_sock_addr;

// TJU_TCP 结构体 保存TJU_TCP用到的各种数据
typedef struct
{
	int state; // TCP的状态

	tju_sock_addr bind_addr;			   // 存放bind和listen时该socket绑定的IP和端口
	tju_sock_addr established_local_addr;  // 存放建立连接后 本机的 IP和端口
	tju_sock_addr established_remote_addr; // 存放建立连接后 连接对方的 IP和端口

	pthread_mutex_t send_lock; // 发送数据锁
	char *sending_buf;		   // 发送数据缓存区
	int sending_len;		   // 发送数据缓存长度
	int send_cleaned_len;

	pthread_mutex_t recv_lock; // 接收数据锁
	char *received_buf;		   // 接收数据缓存区
	int received_len;		   // 接收数据缓存长度
	int recv_cleaned_len;

	pthread_cond_t wait_cond; // 可以被用来唤醒recv函数调用时等待的线程

	window_t window; // 发送和接受窗口

	char *packet_FIN; // FIN或ACK或FIN_ACK

} tju_tcp_t;

#endif