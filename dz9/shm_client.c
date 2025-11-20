// Кочарян Элина Самвеловна БПИ-244

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#define SHM_NAME "/akos_elina_shm"
#define RNG_MAX  1000  // диапазон чисел

typedef struct {
    volatile sig_atomic_t stop;    // 1 — завершаться
    volatile sig_atomic_t alive_client; // клиент запущен
    volatile sig_atomic_t alive_server; // сервер запущен
    volatile unsigned int seq;   // номер сообщения
    volatile int value;    // значение
} shm_region;

static shm_region *shm = NULL;   // указатель на общую память

static void err(const char *msg) { perror(msg); _exit(1); }

// по сигналу stop в shared memory
static void on_sig(int sig) { (void)sig; if (shm) shm->stop = 1; }

int main(void) {
    // создаём shm или подключаемся к существующей
    int fd = shm_open(SHM_NAME, O_RDWR | O_CREAT | O_EXCL, 0666);
    int created = (fd >= 0);
    if (!created) {
        if (errno != EEXIST) err("shm_open");
        fd = shm_open(SHM_NAME, O_RDWR, 0666);
        if (fd < 0) err("shm_open existing");
    }

    // задаём размер и делаем мап
    if (ftruncate(fd, (off_t)sizeof(shm_region)) < 0) err("ftruncate");
    shm = mmap(NULL, sizeof(shm_region), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shm == MAP_FAILED) err("mmap");
    if (created) memset((void*)shm, 0, sizeof(*shm)); // если первые то обнулим

    // ctrl-c/term
    struct sigaction sa; memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sig; sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // работает
    shm->alive_client = 1;
    printf("client: started\n"); fflush(stdout);

    srand((unsigned)(time(NULL) ^ getpid()));
    while (!shm->stop) {
        shm->value = rand() % RNG_MAX;  // число
        shm->seq   = shm->seq + 1u;     // увеличиваем счётчик
        usleep(150 * 1000);    // ждём
    }

    // выключаем, чистим ресурсы
    shm->alive_client = 0;
    if (munmap((void*)shm, sizeof(*shm)) < 0) err("munmap");
    if (close(fd) < 0) err("close");
    if (shm_unlink(SHM_NAME) < 0 && errno != ENOENT) err("shm_unlink");

    return 0;
}