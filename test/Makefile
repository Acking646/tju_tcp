BUILD_DIR = ../build #设置变量 BUILD_DIR 指向上一级目录的 build 文件夹，用于存放编译产生的目标文件
OBJS = $(BUILD_DIR)/tju_packet.o \
	   $(BUILD_DIR)/kernel.o \
	   $(BUILD_DIR)/tju_tcp.o \
	#    $(BUILD_DIR)/timer.o \
	#    $(BUILD_DIR)/SKB.o \
	#    $(BUILD_DIR)/debug.o 

# -pthread：链接 pthread 线程库
# -g -ggdb：生成调试信息，支持 GDB 调试
# -DDEBUG：定义 DEBUG 宏，启用调试模式
# -I../inc：包含头文件目录
# ./test_client.c：源文件
# -o test_client：输出文件名
# $(OBJS)：链接之前编译的目标文件

default:all

all: clean test_client test_server rdt_server rdt_client close_client

clean:
	-rm -f test_client test_server rdt_server rdt_client close_client client.log server.log rdt_server.log rdt_client.log

test_client:
	gcc -pthread -g -ggdb -DDEBUG -I../inc ./test_client.c -o test_client $(OBJS)

test_server:
	gcc -pthread -g -ggdb -DDEBUG -I../inc ./test_server.c -o test_server $(OBJS)

rdt_server:
	gcc -pthread -g -ggdb -DDEBUG -I../inc ./test_rdt_server.c -o rdt_server $(OBJS)
rdt_client:
	gcc -pthread -g -ggdb -DDEBUG -I../inc ./test_rdt_client.c -o rdt_client $(OBJS)

close_client:
	gcc -pthread -g -ggdb -DDEBUG -I../inc ./test_close_client.c -o close_client $(OBJS)