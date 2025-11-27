// Кочарян Элина Самвеловна БПИ-244

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#ifndef ACK_TIMEOUT_MS
  #define ACK_TIMEOUT_MS 1000  // таймаут ожидания ACK одного бита
#endif

static int wait_ack(pid_t recv_pid, int expected_sig, const sigset_t *ackset) {
    //ждём ACK нужного типа от нужного процесса, с таймаутом
    struct timespec ts = { .tv_sec = ACK_TIMEOUT_MS/1000, .tv_nsec = (ACK_TIMEOUT_MS%1000)*1000000L };
    siginfo_t si;
    for (;;) {
        int r = sigtimedwait(ackset, &si, &ts);
        if (r == -1) {
            if (errno == EAGAIN) return 0; // таймаут, нет подтверждения
            perror("sigtimedwait");
            exit(1);
        }
        if (si.si_pid != recv_pid) continue;  // игнорируем чужие сигналы
        if (r == expected_sig) return 1;   // корректный ACK
        // иначе продолжаем ждать
    }
}

int main(void) {
    pid_t recv_pid;
    long long input;

    printf("Sender PID: %d\n", getpid());
    printf("Введите PID приёмника: ");
    fflush(stdout);
    if (scanf("%d", &recv_pid) != 1 || recv_pid <= 0) {
        fprintf(stderr, "Неверный PID\n");
        return 1;
    }

    printf("Введите целое число: ");
    fflush(stdout);
    if (scanf("%lld", &input) != 1 || input < INT32_MIN || input > INT32_MAX) {
        fprintf(stderr, "Число вне диапазона int32_t\n");
        return 1;
    }
    int32_t val = (int32_t)input;
    uint32_t u = (uint32_t)val;

    //блокируем сигналы-ACK и ждём их через sigtimedwait()
    sigset_t ackset, oldmask;
    sigemptyset(&ackset);
    sigaddset(&ackset, SIGUSR1);
    sigaddset(&ackset, SIGUSR2);
    if (sigprocmask(SIG_BLOCK, &ackset, &oldmask) == -1) {
        perror("sigprocmask");
        return 1;
    }

    // отправляем каждый бит с подтверждением
    for (int i = 0; i < 32; ++i) {
        int bit = (u >> i) & 1;
        int sig = bit ? SIGUSR2 : SIGUSR1;
        if (kill(recv_pid, sig) == -1) {
            perror("kill(bit)");
            return 1;
        }
        if (!wait_ack(recv_pid, sig, &ackset)) {
            fprintf(stderr, "Ошибка: нет подтверждения на бит #%d в течение %d мс\n", i, ACK_TIMEOUT_MS);
            return 2; 
        }
    }

    if (kill(recv_pid, SIGINT) == -1) {
        perror("kill(SIGINT)");
        return 1;
    }
    sigprocmask(SIG_SETMASK, &oldmask, NULL);
    printf("Передача завершена.\n");
    return 0;
}
