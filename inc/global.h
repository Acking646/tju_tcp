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

#define SERVER_IP "172.17.0.3"
#define CLIENT_IP "172.17.0.2"

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
/* 序号空间：[0 ... base ... nextseq ... base+window_size ...]
- 左区（≤ base-1）：已发送且已确认（可释放资源）
- 中区（base ≤ seq < nextseq）：已发送但未确认（需监控超时/重复ACK，准备重传）
- 右区（≥ nextseq 且 ≤ base+window_size）：未发送但允许发送（可根据窗口大小发送）
- 右区外（> base+window_size）：禁止发送（超出接收方通告窗口） */
typedef struct
{
	uint32_t window_size; // 接收方通告的窗口大小（流量控制依据）
	uint32_t base;		  // 发送窗口的“左边界”：已发送但未确认的最小序号
	uint32_t nextseq;	  // 发送窗口的“右边界”：下一个待发送的序号
	// uint32_t estmated_rtt;    // （预留）估计往返时间（用于动态调整超时时间）
	int ack_cnt;		   // （预留）ACK计数器（可能用于统计确认次数）
	uint32_t same_ack_cnt; // 重复ACK计数器（用于快速重传触发）
	// pthread_mutex_t ack_cnt_lock; // （预留）ACK计数的线程安全锁
	struct timeval send_time; // （已发送未确认数据的）发送时间戳（用于超时判断）
	struct itimerval timeout; // 超时定时器（用于重传触发）
							  // int congestion_status;    // （预留）拥塞控制状态（慢启动/拥塞避免等）
							  // uint16_t cwnd;            // （预留）拥塞窗口大小（拥塞控制核心）
							  // uint16_t ssthresh;        // （预留）慢启动阈值（拥塞控制阈值）
} sender_window_t;

// TCP 接受窗口
// 注释的内容如果想用就可以用 不想用就删掉 仅仅提供思路和灵感
typedef struct
{
	// char *wnd_recv_buf;       // （预留）接收窗口专用缓冲区（可替代全局received_buf）
	uint32_t wnd_available_size; // 接收窗口的可用空间（流量控制反馈依据）
	// received_packet_t* head;  // （预留）乱序数据包链表（存储未按序到达的数据）
	// char buf[TCP_RECVWN_SIZE];// （预留）接收窗口固定大小缓冲区
	uint8_t *marked;	 // 接收标记数组（标记哪些序号的数据已收到）
	uint32_t expect_seq; // 期望接收的下一个序号（即 TCP 协议中的 rcv_nxt）
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