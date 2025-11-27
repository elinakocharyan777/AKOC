#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>

static volatile sig_atomic_t got_bit = 0; // пришёл бит
static volatile sig_atomic_t last_bit = 0;  
static volatile sig_atomic_t finished = 0;   
static volatile sig_atomic_t peer_pid = 0; // PID передатчика

static void on_bit(int sig) {
    // Сохраняем бит и сразу шлём ACK тем же сигналом
    last_bit = (sig == SIGUSR2) ? 1 : 0;
    got_bit = 1;
    if (peer_pid > 0) {
        kill((pid_t)peer_pid, sig); 
    }
}
static void on_fin(int sig) {
    (void)sig;
    finished = 1;
}

int main(void) {
    printf("Receiver PID: %d\n", getpid());
    printf("Введите PID передатчика: ");
    fflush(stdout);
    if (scanf("%d", (int*)&peer_pid) != 1 || peer_pid <= 0) {
        fprintf(stderr, "Неверный PID\n");
        return 1;
    }

    // Обработчики сигналов
    struct sigaction sa = {0};
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sa.sa_handler = on_bit;
    sigaction(SIGUSR1, &sa, NULL);
    sigaction(SIGUSR2, &sa, NULL);

    struct sigaction sf = {0};
    sigemptyset(&sf.sa_mask);
    sf.sa_flags = SA_RESTART;
    sf.sa_handler = on_fin;
    sigaction(SIGINT, &sf, NULL);

    // Блокируем рабочие сигналы и ждём их через sigsuspend()
    sigset_t block, waitset;
    sigemptyset(&block);
    sigaddset(&block, SIGUSR1);
    sigaddset(&block, SIGUSR2);
    sigaddset(&block, SIGINT);
    sigprocmask(SIG_BLOCK, &block, NULL);

    waitset = block;
    sigdelset(&waitset, SIGUSR1);
    sigdelset(&waitset, SIGUSR2);
    sigdelset(&waitset, SIGINT);

    uint32_t acc = 0;
    int bits = 0;

    while (!finished) {
        while (!got_bit && !finished) {
            sigsuspend(&waitset); // асинхронное ожидание событий
        }
        if (finished) break;

        if (got_bit) {
            if (last_bit && bits < 32) acc |= (1u << bits);
            if (bits < 32) bits++;
            got_bit = 0;
        }
    }

    int32_t value = (int32_t)acc; // учитывает знаковый бит
    printf("Принятое число: %d\n", value);
    return 0;
}
