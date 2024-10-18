#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <tpa.h>

static struct tpa_worker *worker;
#define POOL_SIZE 64

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

    while (1) {
        tpa_worker_run(worker);
        struct tpa_iovec iov;
        ssize_t ret;

        ret = tpa_zreadv(sid, &iov, 1);
        if (ret <= 0) {
            if (ret < 0 && errno == EAGAIN) {
                continue;
            }
            tpa_close(sid);
            printf("shutdown conn!\n");
            return;
        }

        //iov.iov_read_done(iov.iov_base, iov.iov_param);
        if (both_dir) {
            ret = tpa_zwritev(sid, &iov, 1);
            if (ret < 0)
                iov.iov_read_done(iov.iov_base, iov.iov_param);
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
    struct tpa_iovec iov[POOL_SIZE];
    int ret;
    for (i = 0; i < POOL_SIZE; i++) {
        iov[i].iov_base = zwrite_extbuf_alloc(4096);
        iov[i].iov_phys = 1;
        iov[i].iov_param = &iov[i];
        iov[i].iov_len = 4096;
        iov[i].iov_write_done = zero_copy_write_done;
        pool_enqueue(&pool, &iov[i]);
    }

    while (1) {
        tpa_worker_run(worker);
        while (pool.count > 0) {
            struct tpa_iovec *iov = pool_dequeue(&pool);
            ret = tpa_zwritev(sid, iov, 1);
            if (ret < 0)
                iov->iov_write_done(iov->iov_base, iov);
            if (both_dir) {
                struct tpa_iovec iov;
                ret = tpa_zreadv(sid, &iov, 1);
                if (ret <= 0) {
                    if (ret < 0 && errno == EAGAIN) {
                        continue;
                    }
                    tpa_close(sid);
                    printf("shutdown conn!\n");
                    return;
                }
                iov.iov_read_done(iov.iov_base, iov.iov_param);
            }
        }
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
