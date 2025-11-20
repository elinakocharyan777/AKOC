// Кочарян Элина Самвеловна БПИ-244

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#define SHM_NAME "/akos_elina_shm"

typedef struct {
    volatile sig_atomic_t stop;   // общий флаг остановки
    volatile sig_atomic_t alive_client; // клиент в работе
    volatile sig_atomic_t alive_server; // сервер в работе
    volatile unsigned int seq;  // счётчик
    volatile int value; // последнее значение
} shm_region;

static shm_region *shm = NULL;

static void err(const char *msg) { perror(msg); _exit(1); }
// по сигналу остановиться
static void on_sig(int sig) { (void)sig; if (shm) shm->stop = 1; }

int main(void) {
    // создаём shm или подключаемся
    int fd = shm_open(SHM_NAME, O_RDWR | O_CREAT | O_EXCL, 0666);
    int created = (fd >= 0);
    if (!created) {
        if (errno != EEXIST) err("shm_open");
        fd = shm_open(SHM_NAME, O_RDWR, 0666);
        if (fd < 0) err("shm_open existing");
    }

    if (ftruncate(fd, (off_t)sizeof(shm_region)) < 0) err("ftruncate");
    shm = mmap(NULL, sizeof(shm_region), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shm == MAP_FAILED) err("mmap");
    if (created) memset((void*)shm, 0, sizeof(*shm));

    // сигналы
    struct sigaction sa; memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sig; sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    shm->alive_server = 1;
    printf("server: started, waiting for data...\n"); fflush(stdout);

    // печатаем, когда меняется seq
    unsigned int last = shm->seq;
    while (!shm->stop) {
        if (shm->seq != last) {
            last = shm->seq;
            printf("seq=%u value=%d\n", last, shm->value);
            fflush(stdout);
        } else {
            usleep(50 * 1000); 
        }
    }

    shm->alive_server = 0;
    if (munmap((void*)shm, sizeof(*shm)) < 0) err("munmap");
    if (close(fd) < 0) err("close");
    if (shm_unlink(SHM_NAME) < 0 && errno != ENOENT) err("shm_unlink");
    return 0;
}