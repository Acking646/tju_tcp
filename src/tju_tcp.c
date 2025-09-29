#include "tju_tcp.h"

/*
创建 TCP socket
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t *tju_socket()
{

    tju_tcp_t *sock = (tju_tcp_t *)malloc(sizeof(tju_tcp_t));
    sock->state = CLOSED;

    pthread_mutex_init(&(sock->send_lock), NULL);
    sock->sending_buf = (char *)malloc(MAX_BUF_SIZE);
    sock->sending_len = 0;
    sock->send_cleaned_len = 0; // 为了方便继承之前的seq

    pthread_mutex_init(&(sock->recv_lock), NULL);
    sock->received_buf = (char *)malloc(MAX_BUF_SIZE);
    sock->received_len = 0;
    sock->recv_cleaned_len = 0;

    if (pthread_cond_init(&sock->wait_cond, NULL) != 0)
    {
        perror("ERROR condition variable not set\n");
        exit(-1);
    }
    sock->window.wnd_send = (sender_window_t *)malloc(sizeof(sender_window_t));
    sock->window.wnd_send->window_size = MAX_BUF_SIZE; // 用来控制发送量
    sock->window.wnd_send->base = 1;                   // 已发送但未收到ack的第一个数据   数据从1开始
    sock->window.wnd_send->ack_cnt = 0;                // 在收到ack数据中的排最后的一个
    sock->window.wnd_send->nextseq = 1;                // 即将要发送的第一个数据
    sock->window.wnd_send->same_ack_cnt = 0;           // 当连续收到三个相同的ack, 进行快速重传
    sock->window.wnd_send->timeout.it_value.tv_sec = 0;
    sock->window.wnd_send->timeout.it_value.tv_usec = 600000;
    // 定时器不会自动重启
    sock->window.wnd_send->timeout.it_interval.tv_sec = 0;
    sock->window.wnd_send->timeout.it_interval.tv_usec = 0;

    sock->window.wnd_recv = (receiver_window_t *)malloc(sizeof(receiver_window_t));
    sock->window.wnd_recv->expect_seq = 1; // 可以接收数据的第一位
    sock->window.wnd_recv->wnd_available_size = MAX_BUF_SIZE;
    // sock->window.wnd_recv->marked = (uint8_t *)malloc(MAX_BUF_SIZE);
    // sock->window.wnd_recv->wnd_recv_buf = (char *)malloc(MAX_WINDOW_SIZE); // 这个可能会去掉

    sock->packet_FIN = NULL;

    return sock;
}

/*
绑定监听的地址 包括ip和端口
*/
int tju_bind(tju_tcp_t *sock, tju_sock_addr bind_addr)
{
    sock->bind_addr = bind_addr;
    return 0;
}

/*
被动打开 监听bind的地址和端口
设置socket的状态为LISTEN
注册该socket到内核的监听socket哈希表
*/
int tju_listen(tju_tcp_t *sock)
{
    init_queue();
    sock->state = LISTEN;
    int hashval = cal_hash(sock->bind_addr.ip, sock->bind_addr.port, 0, 0);
    listen_socks[hashval] = sock;
    return 0;
}

/*
接受连接
返回与客户端通信用的socket
这里返回的socket一定是已经完成3次握手建立了连接的socket
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
tju_tcp_t *tju_accept(tju_tcp_t *listen_sock)
{
    // 判断全连接队列中是否有 socket
    tju_tcp_t *accept_socket = get_from_full(); // 队列为空 阻塞
    printf("从全连接队列中取出一个sock\n");

    tju_tcp_t *new_conn = accept_socket;
    if (new_conn == NULL)
    {
        printf("accpet取出失败\n");
        exit(-1);
    }

    // 将新的conn放到内核建立连接的socket哈希表中
    int hashval = cal_hash(new_conn->established_local_addr.ip, new_conn->established_local_addr.port,
                           new_conn->established_remote_addr.ip, new_conn->established_remote_addr.port);
    established_socks[hashval] = new_conn;

    // 如果new_conn的创建过程放到了tju_handle_packet中 那么accept怎么拿到这个new_conn呢
    // 在linux中 每个listen socket都维护一个已经完成连接的socket队列
    // 每次调用accept 实际上就是取出这个队列中的一个元素
    // 队列为空,则阻塞
    printf("服务器三次握手完成\n");

    // 创建发送线程
    pthread_t sending_thread_id = 555;
    int rst1 = pthread_create(&sending_thread_id, NULL, sending_thread, (void *)new_conn);
    if (rst1 < 0)
    {
        printf("sending thread 创建失败\n");
        exit(-1);
    }
    printf("sending thread 创建成功\n");

    // 创建重传线程
    pthread_t retrans_thread_id = 556;
    int rst2 = pthread_create(&retrans_thread_id, NULL, retrans_thread, (void *)new_conn);
    if (rst2 < 0)
    {
        printf("retrans thread 创建失败\n");
        exit(-1);
    }
    printf("retrans thread 创建成功\n");

    return new_conn;
}

/*
连接到服务端
该函数以一个socket为参数
调用函数前, 该socket还未建立连接
函数正常返回后, 该socket一定是已经完成了3次握手, 建立了连接
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
int tju_connect(tju_tcp_t *sock, tju_sock_addr target_addr)
{

    sock->established_remote_addr = target_addr;

    tju_sock_addr local_addr;
    local_addr.ip = inet_network(CLIENT_IP);
    local_addr.port = generate_port(); // 连接方进行connect连接的时候 内核中是随机分配一个可用的端口
    sock->established_local_addr = local_addr;

    // 这里也不能直接建立连接 需要经过三次握手
    // 实际在linux中 connect调用后 会进入一个while循环
    // 循环跳出的条件是socket的状态变为ESTABLISHED 表面看上去就是 正在连接中 阻塞
    // 而状态的改变在别的地方进行 在我们这就是tju_handle_packet
    uint32_t seq = CLIENT_ISN;
    uint32_t ack = 0;
    char *packet_SYN = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, seq, ack,
                                         DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, SYN_FLAG_MASK, 1, 0, NULL, 0);
    sendToLayer3(packet_SYN, DEFAULT_HEADER_LEN);
    sock->state = SYN_SENT;
    printf("Client send syn and the first way handshake\n");
    /*****bug, no establish_socks in time*****/
    int hashval = cal_hash(local_addr.ip, local_addr.port, target_addr.ip, target_addr.port);
    established_socks[hashval] = sock;

    struct timeval start, current;
    long timeout = 3; // 超时时间为 2 秒
    gettimeofday(&start, NULL);
    while (sock->state != ESTABLISHED)
    {
        gettimeofday(&current, NULL);
        long elapsed = (current.tv_sec - start.tv_sec) +
                       (current.tv_usec - start.tv_usec) / 1000000.0;
        if (elapsed > timeout)
        {
            // 超时，重发 SYN_ACK 包
            printf("Resend syn and the first way handshake\n");
            sendToLayer3(packet_SYN, DEFAULT_HEADER_LEN);
            gettimeofday(&start, NULL);
        }
    }

    // 将建立了连接的socket放入内核 已建立连接哈希表中
    free(packet_SYN);
    printf("客户端三次握手完成\n");

    // 创建线程来专门发送数据
    pthread_t sending_thread_id = 557;
    int rst1 = pthread_create(&sending_thread_id, NULL, sending_thread, (void *)sock);
    if (rst1 < 0)
    {
        perror("pthread_create failed\n");
        exit(-1);
    }
    printf("sending thread 创建成功\n");

    // 创建线程来专门重发数据
    pthread_t retrans_thread_id = 558;
    int rst2 = pthread_create(&retrans_thread_id, NULL, retrans_thread, (void *)sock);
    if (rst2 < 0)
    {
        perror("retrans thread 创建失败\n");
        exit(-1);
    }
    printf("retrans thread 创建成功\n");

    return 0;
}

// 测试中一次性发10KB的数据，会调用该函数5000次
// 把要发送的数据放入发送缓冲区
int tju_send(tju_tcp_t *sock, const void *buffer, int len)
{
    // 因为buffer和len在发送过程中会改变,所以用新的变量
    char *data_buffer = (char *)buffer;
    int data_len = len;
    printf("调用 tju_send()\n");
    // 仍有数据未发送
    while (data_len)
    {
        // 发送缓冲区足够放入数据
        if (data_len <= MAX_BUF_SIZE - sock->sending_len + sock->send_cleaned_len)
        {
            // 发送缓冲区现存长度
            uint32_t send_buf_exist_len = sock->sending_len - sock->send_cleaned_len;
            pthread_mutex_lock(&sock->send_lock);
            memcpy(sock->sending_buf + send_buf_exist_len, data_buffer, data_len); // 将数据拷贝到发送缓冲区末尾
            sock->sending_len += data_len;                                         // bug, 忘了加len,导致send_thread一直不进入
            pthread_mutex_unlock(&sock->send_lock);
            data_len = 0;
            printf("将数据放入发送缓冲区\n");
            break;
        }
        // 如果不足够就等待缓冲区清除
    }
    return 0;
}

int tju_recv(tju_tcp_t *sock, void *buffer, int len)
{
    while ((sock->received_len - sock->recv_cleaned_len) <= 0)
    {
        // 阻塞
    }

    while (pthread_mutex_lock(&(sock->recv_lock)) != 0)
        ; // 加锁

    int read_len = 0;
    if ((sock->received_len - sock->recv_cleaned_len) >= len)
    { // 从中读取len长度的数据
        read_len = len;
    }
    else
    {
        read_len = (sock->received_len - sock->recv_cleaned_len); // 读取sock->received_len长度的数据(全读出来)
    }
    memcpy(buffer, sock->received_buf, read_len);

    if (read_len < (sock->received_len - sock->recv_cleaned_len))
    { // 还剩下一些
        char *new_buf = (char *)malloc(MAX_BUF_SIZE);
        memcpy(new_buf, sock->received_buf + read_len, sock->received_len - sock->recv_cleaned_len - read_len);
        free(sock->received_buf);
        sock->recv_cleaned_len += read_len;
        sock->received_buf = new_buf;
        sock->window.wnd_recv->wnd_available_size += read_len; // bug处。缓冲区满了就无法发了的原因
    }
    else
    {
        free(sock->received_buf);
        sock->received_buf = (char *)malloc(MAX_BUF_SIZE);
        sock->recv_cleaned_len += read_len;
        sock->window.wnd_recv->wnd_available_size += read_len;
    }
    printf("tju_recv取出数据, len=%d,cleaned_len=%d\n", read_len, sock->recv_cleaned_len);

    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

    return read_len; // bug处没有返回正确的len
}

// 理下seq和ack的关系, 同时关闭的具体过程是怎样的
int tju_handle_packet(tju_tcp_t *sock, char *pkt)
{

    if (sock->state == LISTEN)
    {
        // Server in LISTEN state receive SYN
        if (get_flags(pkt) == SYN_FLAG_MASK)
        {
            // send SYN_ACK
            char *packet_SYN_ACK = create_packet_buf(get_dst(pkt), get_src(pkt), SERVER_ISN, get_seq(pkt) + 1,
                                                     DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, (SYN_FLAG_MASK | ACK_FLAG_MASK), 1, 0, NULL, 0);
            sendToLayer3(packet_SYN_ACK, DEFAULT_HEADER_LEN);
            printf("In LISTEN receive SYN and send SYN_ACK and second way handshake\n");
            // receive syn and put sock in semi_conn_queue
            tju_tcp_t *new_conn = (tju_tcp_t *)malloc(sizeof(tju_tcp_t));
            memcpy(new_conn, sock, sizeof(tju_tcp_t)); // 这块对new_conn已经初始化了
            // 绑定地址
            new_conn->established_local_addr = new_conn->bind_addr;
            new_conn->established_remote_addr.ip = inet_network(CLIENT_IP);
            new_conn->established_remote_addr.port = get_src(pkt);

            new_conn->state = SYN_RECV;

            en_semi_conn_queue(new_conn, packet_SYN_ACK);
            printf("new conn get into SYN_RECV and Enter in semi_conn_queue\n");
        }
        // bug solve, sock LISTEN not change, Server in LISTEN state receive ACK
        else if (get_flags(pkt) == ACK_FLAG_MASK)
        {
            // recieve ACK and put sock in full_conn_queue
            tju_tcp_t *tmp_conn = get_from_semi(pkt);
            printf("conn get out sock from semi_conn_queue\n");
            if (tmp_conn == NULL)
            {
                printf("semi_conn_queue not exists the conn\n");
                return 0;
            }

            tmp_conn->state = ESTABLISHED;

            en_full_conn_queue(tmp_conn);
            printf("Server in LISTEN receive ACK and enter full_conn_queue\n");
        }
    }
    // Client in SYN_SENT state receive SYN_ACK
    else if (sock->state == SYN_SENT)
    {
        if (get_flags(pkt) == (SYN_FLAG_MASK | ACK_FLAG_MASK))
        {
            // send ACK
            char *packet_ACK = create_packet_buf(get_dst(pkt), get_src(pkt), get_ack(pkt), get_seq(pkt) + 1,
                                                 DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
            sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);
            sock->state = ESTABLISHED;
            free(packet_ACK);
            printf("Client in SYN_SENT receive SYN_ACK send ACK ---- third way handshake\n");
        }
    }
    // In four handshake, get_seq
    // Server in ESTABLISHED state receive FIN
    else if (sock->state == ESTABLISHED)
    {
        // 接收到数据
        if (get_flags(pkt) == NO_FLAG)
        {
            // 首先判断收到的数据是否等于所期待的expect_seq
            if (get_seq(pkt) == sock->window.wnd_recv->expect_seq)
            {

                printf("收到数据 seq=%d\n", get_seq(pkt));
                uint16_t dlen = get_plen(pkt) - get_hlen(pkt);
                uint32_t expt_seq = sock->window.wnd_recv->expect_seq;
                uint32_t wnd_aval_size = sock->window.wnd_recv->wnd_available_size;
                // 接收到的数据能放入接收缓冲区, 不能就发送ACK
                if (get_seq(pkt) + dlen < expt_seq + wnd_aval_size)
                {
                    pthread_mutex_lock(&(sock->recv_lock));
                    memcpy(
                        sock->received_buf + get_seq(pkt) - sock->recv_cleaned_len - 1, // 目标地址：接收缓冲区中的写入位置
                        pkt + get_hlen(pkt),                                            // 源地址：数据包中的有效数据起始位置
                        dlen                                                            // 复制长度：有效数据的字节数
                    );
                    uint32_t place = get_seq(pkt) - sock->recv_cleaned_len - 1;
                    printf("在接收缓冲区place=%d放入数据大小dlen=%d\n", place, dlen);
                    // memcpy(sock->window.wnd_recv->marked+get_seq(pkt)-sock->recv_cleaned_len-1, 1, dlen);
                    sock->window.wnd_recv->wnd_available_size -= dlen;
                    sock->window.wnd_recv->expect_seq += dlen;
                    sock->received_len += dlen;
                    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁
                }
                // 发送ACK
                uint32_t ack = sock->window.wnd_recv->expect_seq;
                uint32_t adv_window = sock->window.wnd_recv->wnd_available_size;
                uint32_t seq = sock->window.wnd_send->nextseq;
                char *packet_ACK = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, seq, ack,
                                                     DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, adv_window, 0, NULL, 0);
                sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);
                free(packet_ACK);
            }
            else
            { // 收到的序列号不是所期望的, 直接发送ACK
                uint32_t seq = sock->window.wnd_send->nextseq;
                uint32_t ack = sock->window.wnd_recv->expect_seq;
                uint32_t adv_window = sock->window.wnd_recv->wnd_available_size;
                char *packet_ACK = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, seq, ack,
                                                     DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, adv_window, 0, NULL, 0);
                sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);

                free(packet_ACK);
            }
        }
        // 数据发送后收到ACK
        else if (get_flags(pkt) == ACK_FLAG_MASK)
        {
            // 如果收到的 ack 在窗口外则直接丢掉
            if (get_ack(pkt) < sock->window.wnd_send->base)
            {
                printf("收到的ack报文在发送窗口外 丢弃报文 \n");
            }
            // 收到重复的ACK
            else if (get_ack(pkt) == sock->window.wnd_send->base)
            {

                printf("收到重复ACK报文 ack=%d\n", get_ack(pkt));

                // 快速重传
                sock->window.wnd_send->same_ack_cnt++;
                if (sock->window.wnd_send->same_ack_cnt == 3)
                {
                    RETRANS = TRUE;
                    sock->window.wnd_send->same_ack_cnt = 0;
                }
            }
            // 收到用于更新的ACK
            else
            {

                printf("收到有效ACK报文 ack=%d\n", get_ack(pkt));
                sock->window.wnd_send->base = get_ack(pkt);
                sock->window.wnd_send->ack_cnt = sock->window.wnd_send->base - 1;
                sock->window.wnd_send->window_size = get_advertised_window(pkt); //  根据发来的ACK包改变发送窗口大小

                // 发送队列正式结束，结束计时
                if (sock->window.wnd_send->base == sock->window.wnd_send->nextseq)
                {
                    stopTimer();
                    printf("发送队列正式结束\n");
                }
                // 重新开始计时
                else
                {
                    startTimer(sock);
                }
                // 清除发送缓冲区已收到确认的数据的数据
                if (sock->window.wnd_send->ack_cnt - sock->send_cleaned_len > 0)
                {
                    pthread_mutex_lock(&sock->send_lock);
                    char *new_sending_buf = (char *)malloc(MAX_BUF_SIZE);
                    memcpy(new_sending_buf, sock->sending_buf + sock->window.wnd_send->ack_cnt - sock->send_cleaned_len, sock->sending_len - sock->window.wnd_send->ack_cnt);
                    free(sock->sending_buf);
                    sock->sending_buf = new_sending_buf;
                    sock->send_cleaned_len = sock->window.wnd_send->ack_cnt;
                    pthread_mutex_unlock(&sock->send_lock);
                }
            }
        }
        // close
        else if (get_flags(pkt) == FIN_FLAG_MASK)
        {
            // send ACK
            uint32_t ack = get_seq(pkt) + 1;
            char *packet_ACK = create_packet_buf(get_dst(pkt), get_src(pkt), get_ack(pkt), ack,
                                                 DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
            sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);

            sock->state = CLOSE_WAIT;
            printf("Server in ESTABLISHED state receive FIN and send ACK and get into CLOSE_WAIT ---- second way byehandshake finish\n");
            free(packet_ACK);

            sleep(1); // wait for data transmit

            // send FIN_ACK
            char *packet_FIN_ACK = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, FIN_SEQ,
                                                     ack, DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, (FIN_FLAG_MASK | ACK_FLAG_MASK), 1, 0, NULL, 0); // different
            sendToLayer3(packet_FIN_ACK, DEFAULT_HEADER_LEN);
            sock->packet_FIN = packet_FIN_ACK;
            sock->state = LAST_ACK;
            printf("Server in CLOSE_WAIT send FIN_ACK after data transmit end and get into LAST_ACK ---- finish third way byehandshake\n");

            // Server don't wait for ACK to close and resend FIN_ACK
            pthread_t id;
            int rst = pthread_create(&id, NULL, tju_close_thread, (void *)sock);
            if (rst < 0)
            {
                printf("ERROR open tju_close_thread\n");
                exit(-1);
            }
        }
    }
    else if (sock->state == FIN_WAIT_1)
    {
        // Client in FIN_WATT_1 receive ACK
        if (get_flags(pkt) == ACK_FLAG_MASK)
        {
            sock->state = FIN_WAIT_2;
            printf("Client get into FIN_WAT_2\n");
        }
        // 要加收到FIN的情况
        else if (get_flags(pkt) == FIN_FLAG_MASK)
        { // concurrent close
            // send ACK
            char *packet_ACK = create_packet_buf(get_dst(pkt), get_src(pkt), FIN_SEQ + 1, get_seq(pkt) + 1,
                                                 DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
            sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);
            free(packet_ACK);
            sock->state = CLOSING;
            printf("concurrent in FIN_WATT-1 receive FIN and get into CLOSING\n");
        }
        else if (get_flags(pkt) == (FIN_FLAG_MASK | ACK_FLAG_MASK))
        {
            char *packet_ACK = create_packet_buf(get_dst(pkt), get_src(pkt), FIN_SEQ + 1, get_seq(pkt) + 1,
                                                 DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
            sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);
            free(packet_ACK);
            sock->state = TIME_WAIT;
            printf("Receive SYN_ACK and send ACK and get into TIME_WATT, wait for 2ML\n");
            sleep(2);
            sock->state = CLOSED;
        }
    }
    // Client in FIN_WATT_2 receive FIN_ACK
    else if (sock->state == FIN_WAIT_2)
    {
        if (get_flags(pkt) == (FIN_FLAG_MASK | ACK_FLAG_MASK))
        {
            // send ACK
            char *packet_ACK = create_packet_buf(get_dst(pkt), get_src(pkt), get_ack(pkt), get_seq(pkt) + 1,
                                                 DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
            sendToLayer3(packet_ACK, DEFAULT_HEADER_LEN);
            free(packet_ACK);
            sock->state = TIME_WAIT;
            printf("Client in FIN_WATT_2 receive FIN_ACK and send ACK and get into TIME_WATT, wait for 2ML to close --- fourth way byehandshake finish\n");
            sleep(2);
            sock->state = CLOSED;
            printf("Connection close\n");
        }
    }
    // Server in LAST_ACK receive ACK
    else if (sock->state == LAST_ACK)
    {
        if (get_flags(pkt) == ACK_FLAG_MASK)
        {
            printf("Server in LAST_ACK receive ACK and connection close\n");
            sock->state = CLOSED;
        }
    }
    // concurrent under CLOSING receive ACK
    else if (sock->state == CLOSING)
    {
        if (get_flags(pkt) == ACK_FLAG_MASK && get_ack(pkt) == FIN_SEQ + 1)
        {
            sock->state = TIME_WAIT;
            printf("concurrent in CLOSING receive ACK and get into TIME_WATT, wait for 2ML to close\n");
            sleep(2);
            sock->state = CLOSED;
            printf("Connection close\n");
        }
    }

    // uint32_t data_len = get_plen(pkt) - DEFAULT_HEADER_LEN;

    // // 把收到的数据放到接受缓冲区
    // while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    // if(sock->received_buf == NULL){
    //     sock->received_buf = malloc(data_len);
    // }else {
    //     sock->received_buf = realloc(sock->received_buf, sock->received_len + data_len);
    // }
    // memcpy(sock->received_buf + sock->received_len, pkt + DEFAULT_HEADER_LEN, data_len);
    // sock->received_len += data_len;

    // pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

    return 0;
}

void *tju_close_thread(void *arg)
{
    tju_tcp_t *sock = (tju_tcp_t *)arg;
    tju_close(sock);
}

int tju_close(tju_tcp_t *sock)
{
    clock_t time_point;
    if (sock->state == ESTABLISHED)
    {
        // send FIN
        char *packet_FIN = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, FIN_SEQ,
                                             0, DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, FIN_FLAG_MASK, 1, 0, NULL, 0);
        sendToLayer3(packet_FIN, DEFAULT_HEADER_LEN);
        sock->packet_FIN = packet_FIN;
        time_point = clock();
        sock->state = FIN_WAIT_1;
        printf("Client in ESTABLISHED send FIN and get into FIN_WATT_1 ---- first way byehandshake finish\n");
    }
    else if (sock->state == LAST_ACK)
    {
        time_point = clock();
    }
    else
    {
        printf("Close sock state fault\n");
        exit(-1);
    }

    // 阻塞等待（支持超时重传）
    printf("Wait......\n");
    while (sock->state != CLOSED)
    {
        if ((clock() - time_point) >= 8000000)
        {
            sendToLayer3(sock->packet_FIN, DEFAULT_HEADER_LEN);
            printf("Timeout resend packet_FIN\n");
            time_point = clock();
        }
    }

    printf("Connection close\n");

    int hashval = cal_hash(sock->established_local_addr.ip, sock->established_local_addr.port,
                           sock->established_remote_addr.ip, sock->established_remote_addr.port);
    established_socks[hashval] = NULL;
    free(sock->packet_FIN);
    free(sock);
    return 0;
}

uint16_t generate_port()
{
    srand((unsigned int)time(NULL));
    uint16_t port = rand() % 10001 + 3333; // 3333 - 13333
    return port;
}

/*
void* resend_FIN_ACK(void*arg){
    tju_tcp_t* sock=(tju_tcp_t* )arg;
    clock_t time_point;
    if(sock->state == LAST_ACK){
        time_point = clock();
        while(sock->state!=CLOSED){
            if((clock()-time_point)>6000000){
                sendToLayer3(sock->packet_FIN, DEFAULT_HEADER_LEN);
                printf("Timeout resend FIN_ACK");
                time_point=clock();
            }
        }
    }
}
*/

// 持续检查发送缓冲区中是否有待发送数据，并在满足发送条件时将数据封装成 TCP 数据包发送到网络层
void *sending_thread(void *arg)
{
    tju_tcp_t *sock = (tju_tcp_t *)arg; // 获取发送方的sock
    // 不停的处于准备发送的状态
    while (1)
    {
        // 发现缓冲区中有未发送的数据 && 并且由空间可以发送 && 并且不是在重发的时候
        if ((sock->window.wnd_send->nextseq <= sock->sending_len) && ((sock->window.wnd_send->nextseq - sock->window.wnd_send->base) < sock->window.wnd_send->window_size) && !RETRANS)
        {
            pthread_mutex_lock(&sock->send_lock);
            int dlen;      // 数据长度
            uint16_t plen; // 数据包总长
            uint32_t wnd_size = sock->window.wnd_send->window_size;
            uint32_t wnd_next_seq = sock->window.wnd_send->nextseq;
            uint32_t wnd_base = sock->window.wnd_send->base;
            // 最终数据可以发送长度, min(当未发送的数据长度, 发送窗口空余大小)
            uint32_t data_send_len = min(sock->sending_len - wnd_next_seq + 1, wnd_size - (wnd_next_seq - wnd_base));
            char *data = sock->sending_buf + (wnd_next_seq - sock->send_cleaned_len - 1); // 提取发送数据
            // 不分包, 一次性发送完
            if (data_send_len <= MAX_DLEN)
            {
                dlen = data_send_len;
                plen = dlen + DEFAULT_HEADER_LEN; // bug 导致发过去的dlen = 24
                char *packet1 = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, wnd_next_seq, 0,
                                                  DEFAULT_HEADER_LEN, plen, NO_FLAG, 1, 0, data, dlen);
                sendToLayer3(packet1, plen);
                free(packet1);
                printf("发送数据 seq = %d, dlen = %d\n", wnd_next_seq, dlen);
                data_send_len = 0;
                // 此次发送队列开始, 启动计时器, 这种情况，要么是发送队列开始，要么是发送队列正式结束
                if (wnd_base == wnd_next_seq)
                    startTimer(sock);
                wnd_next_seq += dlen;
            }
            // 分包
            else
            {
                // 发送完为止
                while (data_send_len)
                {
                    if (data_send_len > MAX_DLEN)
                        dlen = MAX_DLEN;
                    else
                        dlen = data_send_len;
                    plen = dlen + DEFAULT_HEADER_LEN;
                    char *packet2 = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, wnd_next_seq, 0,
                                                      DEFAULT_HEADER_LEN, plen, NO_FLAG, 1, 0, data, dlen);
                    sendToLayer3(packet2, plen);
                    free(packet2);
                    printf("发送数据 seq = %d, dlen = %d\n", wnd_next_seq, dlen);
                    if (wnd_base == wnd_next_seq)
                        startTimer(sock);
                    data_send_len -= dlen;
                    data = data + dlen;
                    wnd_next_seq = wnd_next_seq + dlen;
                }
            }
            sock->window.wnd_send->nextseq = wnd_next_seq;
            pthread_mutex_unlock(&sock->send_lock);
        }
    }
}

// 用于进行超时重传的线程
void *retrans_thread(void *arg)
{
    tju_tcp_t *sock = (tju_tcp_t *)arg;
    while (1)
    {
        // 重传标志为TRUE, 重传所有发送但没有收到ack的数据
        if (RETRANS)
        {
            pthread_mutex_lock(&(sock->send_lock));
            uint32_t wnd_base = sock->window.wnd_send->base;
            uint32_t wnd_next_seq = sock->window.wnd_send->nextseq;
            uint32_t wnd_ack = sock->window.wnd_send->ack_cnt;
            // 存在发送但没有收到ack的数据
            while (wnd_base < wnd_next_seq)
            {
                uint32_t dlen;
                char *data = sock->sending_buf + wnd_ack - sock->send_cleaned_len; // 提取数据
                // 剩余发送数据 < MAX_DLEN
                if ((wnd_next_seq - wnd_base) < MAX_DLEN)
                {
                    dlen = wnd_next_seq - wnd_base;
                    char *packet1 = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, wnd_base, 0,
                                                      DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN + dlen, NO_FLAG, 1, 0, data, dlen);
                    sendToLayer3(packet1, DEFAULT_HEADER_LEN + dlen);
                    free(packet1);
                    printf("重传数据 seq = %d, dlen = %d\n", wnd_base, dlen);
                }
                // 分包
                else
                {
                    dlen = MAX_DLEN;
                    char *packet2 = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, wnd_base, 0,
                                                      DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN + dlen, NO_FLAG, 1, 0, data, dlen);
                    sendToLayer3(packet2, DEFAULT_HEADER_LEN + dlen);
                    free(packet2);
                    printf("重传数据 seq = %d, dlen = %d\n", wnd_base, dlen);
                }
                if (wnd_base == sock->window.wnd_send->base)
                    startTimer(sock);
                wnd_base += dlen;
                wnd_ack += dlen;
            }
            // 重发完毕
            RETRANS = FALSE;
            pthread_mutex_unlock(&(sock->send_lock));
        }
    }
}

void timeout_handler(int signo)
{ // 超时处理函数
    RETRANS = TRUE;
    TIMEOUT_FLAG = FALSE;
}

// 开始计时器
void startTimer(tju_tcp_t *sock)
{
    struct itimerval tick; // 定时器结构体（Linux 系统标准定时器类型，用于设置定时时间和周期）
    RETRANS = FALSE;       // 启动定时器前，重置重传标志（避免之前的重传状态干扰）
    TIMEOUT_FLAG = FALSE;  // 重置超时标志（表示新的定时周期开始）

    // 绑定信号处理函数：将 SIGALRM 信号与 timeout_handler 关联
    signal(SIGALRM, timeout_handler);

    // 初始化 tick 结构体：将其所有字节置为 0，避免未初始化的垃圾值影响
    memset(&tick, 0, sizeof(tick));

    // 复制超时配置：从发送窗口的 timeout 字段（sock->window.wnd_send->timeout）复制到 tick
    // （发送窗口的 timeout 字段应提前配置好超时时间，如 1 秒）
    memcpy(&tick, &sock->window.wnd_send->timeout, sizeof(tick));

    // 启动实时定时器（ITIMER_REAL）：超时后触发 SIGALRM 信号
    if (setitimer(ITIMER_REAL, &tick, NULL) < 0)
    {
        printf("Set timer failed!\n"); // 启动失败时打印错误日志
    }
}

// 关闭计时器
void stopTimer(void)
{
    struct itimerval value;
    value.it_value.tv_sec = 0;
    value.it_value.tv_usec = 0;
    value.it_interval.tv_sec = 0;
    value.it_interval.tv_usec = 0;
    setitimer(ITIMER_REAL, &value, NULL);
}

uint16_t get_wnd_move_len(uint8_t *mark)
{ // 获取窗口前移长度
    uint16_t ans = 0;
    while (mark[ans] != 0)
    {
        ans++;
    }
    return ans;
}
