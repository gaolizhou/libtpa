#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <tpa.h>

static struct tpa_worker *worker;

#define POOL_SIZE 64
#define BUF_SIZE 1400

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


void run_server(uint16_t port, const char *filename)
{
    int sid;
    int fd = open(filename, O_CREAT|O_WRONLY, 0644);
    if (fd < 0) {
        fprintf(stderr, "failed to open file: %s\n", strerror(errno));
        exit(1);
    }

    printf(":: listening on port %hu ...\n", port);
    if (tpa_listen_on(NULL, port, NULL) < 0) {
        fprintf(stderr, "failed to listen on port %hu: %s\n",
                port, strerror(errno));
        close(fd);
        exit(1);
    }

    while (1) {
        tpa_worker_run(worker);
        if (tpa_accept_burst(worker, &sid, 1) == 1) {
            printf("Get connection\n");
            break;
        }
    }
    uint64_t file_size = 0, left_size = UINT64_MAX;
    while (left_size > 0) {
        tpa_worker_run(worker);
        struct tpa_iovec iov;
        ssize_t ret;
        ret = tpa_zreadv(sid, &iov, 1);
        if (ret <= 0) {
            if (ret < 0 && errno == EAGAIN) {
                continue;
            }
            printf("shutdown conn!\n");
            tpa_close(sid);
            close(fd);
            return;
        }
        uint8_t *ptr = iov.iov_base;
        uint32_t len = iov.iov_len;
        if(file_size == 0) {
            memcpy(&file_size, iov.iov_base, sizeof(file_size));
            left_size = file_size;
            printf("file_size: %lu\n", file_size);
            ptr += sizeof(file_size);
            len -= sizeof(file_size);
        }
        left_size -= len;
        while (len > 0) {
            ssize_t write_size = write(fd, ptr, len);
            if (write_size < 0) {
                fprintf(stderr, "failed to write to file: %s\n", strerror(errno));
                tpa_close(sid);
                close(fd);
                exit(1);
            }
            len -= write_size;
            ptr += write_size;
        }
        //printf("DATA:%s", (char*)iov.iov_base);
        iov.iov_read_done(iov.iov_base, iov.iov_param);
    }
    tpa_close(sid);
    close(fd);
}

MemoryPool pool;
ssize_t tx_size = 0;
static void zero_copy_write_done(void *iov_base, void *iov_param)
{
    pool_enqueue(&pool, iov_param);
    tx_size += ((struct tpa_iovec*)iov_param)->iov_len;
}

void run_client(uint16_t port, const char *ip_address, const char *filename) {
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

    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "failed to open file: %s\n", strerror(errno));
        exit(1);
    }
    uint64_t file_size = 0;
    file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    printf("file_size: %lu\n", file_size);


    pool_init(&pool);
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

    struct tpa_iovec *iov_tmp = pool_dequeue(&pool);
    memcpy(iov_tmp->iov_base, &file_size, sizeof(file_size));
    iov_tmp->iov_len = sizeof(file_size);
    ret = tpa_zwritev(sid, iov_tmp, 1);
    if (ret < 0) {
        iov_tmp->iov_write_done(iov_tmp->iov_base, iov_tmp);
        printf("failed to write to socket: %s\n", strerror(errno));
        return;
    }

    ssize_t left_size = file_size;
    while (left_size > 0) {
        tpa_worker_run(worker);
        while (pool.count > 0 && left_size > 0) {
            iov_tmp = pool_dequeue(&pool);
            int read_size = left_size > BUF_SIZE ? BUF_SIZE : left_size;
            iov_tmp->iov_len = read(fd, iov_tmp->iov_base, read_size);
            if (iov_tmp->iov_len <= 0) {
                printf("failed to read file: %s, ret=%lu, read_size=%d, left_size=%ld\n",
                       strerror(errno), iov_tmp->iov_len, read_size, left_size);
                close(fd);
                return;
            }
            left_size -= iov_tmp->iov_len;
            ret = tpa_zwritev(sid, iov_tmp, 1);
            if (ret < 0) {
                iov_tmp->iov_write_done(iov_tmp->iov_base, iov_tmp);
                printf("failed to write to socket: %s\n", strerror(errno));
                close(fd);
                return;
            }
        }
    }
    close(fd);
    while(pool.count != POOL_SIZE) {
        tpa_worker_run(worker);
    }
    printf("Done tx_size=%lu\n", tx_size);
    tpa_close(sid);
}

int main(int argc, char **argv)
{
    char *filename = NULL;
    int port = 0;
    char *server_ip = NULL;
    int opt;

    while ((opt = getopt(argc, argv, "f:p:c:")) != -1) {
        switch (opt) {
            case 'f':
                filename = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                if (port <= 0 || port > 65535) {
                    fprintf(stderr, "Invalid port number. Please use a number between 1 and 65535.\n");
                    return EXIT_FAILURE;
                }
                break;
            case 'c':
                server_ip = optarg;
                break;
            default:
                fprintf(stderr, "Usage: %s -f <filename> -p <port> [-c <server_ip>]\n", argv[0]);
                return EXIT_FAILURE;
        }
    }

    if (!filename || port == 0) {
        fprintf(stderr, "Error: -f <filename> and -p <port> are required.\n");
        return EXIT_FAILURE;
    }

    printf("Filename: %s\n", filename);
    printf("Port: %d\n", port);
    if (server_ip) {
        printf("Running in client mode, Server IP: %s\n", server_ip);
    } else {
        printf("Running in server mode.\n");
    }

    if (tpa_init(1) < 0) {
        perror("tpa_init");
        return -1;
    }
    worker = tpa_worker_init();
    if (!worker) {
        fprintf(stderr, "failed to init worker: %s\n", strerror(errno));
        return -1;
    }

    if (server_ip) {
        run_client(port, server_ip, filename);
    } else {
        run_server(port, filename);
    }

    return 0;
}
