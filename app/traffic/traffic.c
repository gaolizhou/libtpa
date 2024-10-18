#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/mman.h>
#include <tpa.h>
#include <time.h>


static struct tpa_worker *worker;
#define POOL_SIZE 64
#define BUF_SIZE 4096
#define DATA_SIZE (1<<30ULL)
#define TEST_ROUND (7*5ULL)

static uint64_t tx_bytes = 0;
static uint64_t rx_bytes = 0;

int both_dir = 1;

typedef struct {
    void *data[POOL_SIZE];
    int head;
    int tail;
    int count;
} MemoryPool;

void pool_init(MemoryPool *pool) {
    pool->head = 0;
    pool->tail = 0;
    pool->count = 0;
}

bool pool_enqueue(MemoryPool *pool, void *data) {
    if (pool->count == POOL_SIZE) {
        return false;
    }
    pool->data[pool->tail] = data;
    pool->tail = (pool->tail + 1) % POOL_SIZE;
    pool->count++;
    return true;
}

void *pool_dequeue(MemoryPool *pool) {
    if (pool->count == 0) {
        return NULL;
    }
    void *data = pool->data[pool->head];
    pool->head = (pool->head + 1) % POOL_SIZE;
    pool->count--;
    return data;
}

void pool_free(MemoryPool *pool) {
    while (pool->count > 0) {
        pool_dequeue(pool);
    }
}

#define BUF_TYPE_EXTERNAL       ((void *)(uintptr_t)2)
#define PAGE_SIZE               4096
#define EXTBUF_SIZE             (1*PAGE_SIZE)
static void *zwrite_extbuf_alloc(size_t size)
{
    void *buf = aligned_alloc(PAGE_SIZE, EXTBUF_SIZE);
    assert(buf != NULL);

    if (tpa_extmem_register(buf, EXTBUF_SIZE, NULL, EXTBUF_SIZE / PAGE_SIZE, PAGE_SIZE) != 0) {
        fprintf(stderr, "failed to register external memory: %s\n", strerror(errno));
        exit(1);
    }
    assert(size <= EXTBUF_SIZE);
    return buf;
}


void run_server(uint16_t port)
{
    int sid;
    printf(":: listening on port %hu ...\n", port);
    if (tpa_listen_on(NULL, port, NULL) < 0) {
        fprintf(stderr, "failed to listen on port %hu: %s\n",
                port, strerror(errno));
        exit(1);
    }
    while (1) {
        tpa_worker_run(worker);
        if (tpa_accept_burst(worker, &sid, 1) == 1) {
            printf("Get connection\n");
            break;
        }
    }

    struct timespec start_time, current_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    uint64_t total_send_bytes_left = TEST_ROUND * DATA_SIZE;;
    uint64_t total_recv_bytes_left = TEST_ROUND * DATA_SIZE;;

    while (1) {
        clock_gettime(CLOCK_MONOTONIC, &current_time);
        double elapsed = (current_time.tv_sec - start_time.tv_sec) * 1000 +
                         (current_time.tv_nsec - start_time.tv_nsec) / 1e6;
        if (elapsed >= 1000) {
            double send_rate = (double)tx_bytes / elapsed * 1000 / (1024.0 * 1024.0 * 1024.0);
            double receive_rate = (double)rx_bytes / elapsed * 1000 / (1024.0 * 1024.0 * 1024.0);

            printf("tx: %.2f GB/s, rx: %.2f GB\n", send_rate, receive_rate);
            start_time = current_time;
            tx_bytes = 0;
            rx_bytes = 0;
        }

        tpa_worker_run(worker);
        struct tpa_iovec iov;
        ssize_t ret;

        ret = tpa_zreadv(sid, &iov, 1);
        if (ret <= 0) {
            if (ret < 0 && errno == EAGAIN) {
                continue;
            }
            tpa_close(sid);
            printf("shutdown conn! %s\n", strerror(errno));
            return;
        }
        rx_bytes += iov.iov_len;
        total_recv_bytes_left -= iov.iov_len;

        //iov.iov_read_done(iov.iov_base, iov.iov_param);
        if (both_dir) {
            ret = tpa_zwritev(sid, &iov, 1);
            if (ret < 0)
                iov.iov_read_done(iov.iov_base, iov.iov_param);
            tx_bytes += iov.iov_len;
            total_send_bytes_left -= iov.iov_len;
        } else {
            iov.iov_read_done(iov.iov_base, iov.iov_param);
        }
    }
}

MemoryPool pool;
static void zero_copy_write_done(void *iov_base, void *iov_param)
{
    pool_enqueue(&pool, iov_param);
}

void run_client(uint16_t port, const char *ip_address) {
    int sid, i;
    printf(":: connecting to %s:%hu ...\n", ip_address, port);
    sid = tpa_connect_to(ip_address, port, NULL);
    if (sid < 0) {
        fprintf(stderr, "failed to connect: %s\n", strerror(errno));
        return;
    }
    for(i=0;i<1000;i++) {
        tpa_worker_run(worker);
    }
    pool_init(&pool);
    uint8_t *send_data_page = mmap(NULL, DATA_SIZE, PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30 << 26), -1, 0);
    if (send_data_page == MAP_FAILED) {
        perror("mmap send_data_page");
        return;
    }
    for(i = 0; i < DATA_SIZE; i++) {
        send_data_page[i] = i;
    }
    uint8_t *recv_data_page = mmap(NULL, DATA_SIZE, PROT_READ | PROT_WRITE,
                                MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (30 << 26), -1, 0);
    if (recv_data_page == MAP_FAILED) {
        perror("mmap recv_data_page");
        return;
    }

    struct tpa_iovec iov[POOL_SIZE];
    int ret;
    for (i = 0; i < POOL_SIZE; i++) {
        iov[i].iov_base = zwrite_extbuf_alloc(BUF_SIZE);
        iov[i].iov_phys = 1;
        iov[i].iov_param = &iov[i];
        iov[i].iov_len = BUF_SIZE;
        iov[i].iov_write_done = zero_copy_write_done;
        pool_enqueue(&pool, &iov[i]);
    }

    struct timespec start_time, current_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    uint32_t send_offset = 0;
    uint32_t recv_offset = 0;
    uint64_t total_send_bytes_left = TEST_ROUND * DATA_SIZE;;
    uint64_t total_recv_bytes_left = TEST_ROUND * DATA_SIZE;;

    while (total_send_bytes_left > 0 || total_recv_bytes_left > 0) {
        clock_gettime(CLOCK_MONOTONIC, &current_time);
        double elapsed = (current_time.tv_sec - start_time.tv_sec) *1000+
                         (current_time.tv_nsec - start_time.tv_nsec) / 1e6;
        if (elapsed >= 1000) {
            double send_rate = (double)tx_bytes / elapsed * 1000 / (1024.0 * 1024.0 * 1024.0);
            double receive_rate = (double)rx_bytes / elapsed * 1000 / (1024.0 * 1024.0 * 1024.0);

            printf("tx: %.2f GB/s, rx: %.2f GB/s left=%ld/%ld\n",
                   send_rate, receive_rate, total_send_bytes_left, total_recv_bytes_left);
            start_time = current_time;
            tx_bytes = 0;
            rx_bytes = 0;
        }
        tpa_worker_run(worker);
        if (pool.count > 0) {
            if (total_send_bytes_left > 0) {
                struct tpa_iovec *iov = pool_dequeue(&pool);
                uint8_t *send_ptr = send_data_page + send_offset;
                iov->iov_len = DATA_SIZE - send_offset > BUF_SIZE ? BUF_SIZE : DATA_SIZE - send_offset;
                memcpy(iov->iov_base, send_ptr, iov->iov_len);
                send_offset += iov->iov_len;
                if (send_offset == DATA_SIZE) {
                    send_offset = 0;
                }
                ret = tpa_zwritev(sid, iov, 1);
                if (ret < 0) {
                    iov->iov_write_done(iov->iov_base, iov);
                    printf("failed to write to socket: %s\n", strerror(errno));
                    return;
                }
                tx_bytes += iov->iov_len;
                total_send_bytes_left -= iov->iov_len;
            }
            if (both_dir && total_recv_bytes_left > 0) {
                struct tpa_iovec iov;
                ret = tpa_zreadv(sid, &iov, 1);
                if (ret <= 0) {
                    if (ret < 0 && errno == EAGAIN) {
                        continue;
                    }
                    tpa_close(sid);
                    printf("shutdown conn %s\n", strerror(errno));
                    return;
                }
                rx_bytes += iov.iov_len;
                total_recv_bytes_left -= iov.iov_len;
                if (recv_offset + iov.iov_len > DATA_SIZE) {
                    uint8_t *recv_ptr = recv_data_page + recv_offset;
                    size_t first_chunk_size = DATA_SIZE - recv_offset;
                    memcpy(recv_ptr, iov.iov_base, first_chunk_size);
                    recv_offset = 0;
                    recv_ptr = recv_data_page + recv_offset;
                    size_t second_chunk_size = iov.iov_len - first_chunk_size;
                    memcpy(recv_ptr, iov.iov_base + first_chunk_size, second_chunk_size);
                    recv_offset += second_chunk_size;
                } else {
                    uint8_t *recv_ptr = recv_data_page + recv_offset;
                    memcpy(recv_ptr, iov.iov_base, iov.iov_len);
                    recv_offset += iov.iov_len;
                    if (recv_offset == DATA_SIZE) {
                        recv_offset = 0;
                    }
                }
                iov.iov_read_done(iov.iov_base, iov.iov_param);
            }
        }
    }
    while(pool.count != POOL_SIZE) {
        tpa_worker_run(worker);
    }
    printf("Done left=%lu/%lu\n",  total_send_bytes_left, total_recv_bytes_left);
    int res = memcmp(send_data_page, recv_data_page, DATA_SIZE);
    if (res == 0) {
        printf("Data is correct\n");
    } else {
        printf("Data is incorrect\n");
    }
}

int main(int argc, char **argv)
{
    uint16_t port;
    if (tpa_init(1) < 0) {
        perror("tpa_init");
        return -1;
    }
    worker = tpa_worker_init();
    if (!worker) {
        fprintf(stderr, "failed to init worker: %s\n", strerror(errno));
        return -1;
    }
    if (argc < 2) {
        printf("Usage:\n");
        printf("  %s <port>     # Run in server mode\n", argv[0]);
        printf("  %s <port> <server_ip> # Run in client mode, connecting to server_ip\n", argv[0]);
        return 1;
    }
    port = atoi(argv[1]);

    if (argc == 2) {
        run_server(port);
    } else if (argc == 3) {
        const char *ip_address = argv[2];
        run_client(port, ip_address);
    } else {
        printf("Usage:\n");
        printf("  %s            # Run in server mode\n", argv[0]);
        printf("  %s <server_ip> # Run in client mode, connecting to server_ip\n", argv[0]);
        return 1;
    }

    return 0;
}
