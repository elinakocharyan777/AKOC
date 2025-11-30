#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>

#define OBS_FIFO "/tmp/p3_observer.fifo"   // именованный канал
#define BUFSZ 2048

// сигналы
static volatile sig_atomic_t g_stop = 0;
static void on_sigint(int s){ (void)s; g_stop = 1; }

int main(void){
    // обработчик ctrl+c
    struct sigaction sa={0}; sa.sa_handler=on_sigint; sigemptyset(&sa.sa_mask);
    sa.sa_flags=SA_RESTART; sigaction(SIGINT,&sa,NULL);

    // создать fifo
    if (mkfifo(OBS_FIFO, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo"); return 1;
    }

    // открыть на чтение non-block 
    int fd_r = open(OBS_FIFO, O_RDONLY | O_NONBLOCK);
    if (fd_r < 0) { perror("open fifo rd"); return 1; }

    int fd_w_keep = open(OBS_FIFO, O_WRONLY | O_NONBLOCK);
    if (fd_w_keep < 0) {}

    // буфер для неполной строки
    char acc[BUFSZ]; size_t acc_len = 0;

    fprintf(stdout, "[OBSERVER] started, fifo=%s\n", OBS_FIFO); fflush(stdout);

    while(!g_stop){
        char buf[BUFSZ];
        ssize_t r = read(fd_r, buf, sizeof(buf));
        if (r > 0){
            size_t off = 0;
            while (off < (size_t)r){
                // дописываем до \n или конца
                size_t i = off;
                while (i < (size_t)r && buf[i] != '\n') i++;
                size_t chunk = i - off;

                if (acc_len + chunk < sizeof(acc)-1){
                    memcpy(acc + acc_len, buf + off, chunk);
                    acc_len += chunk;
                } else {
                    //если переполнение сбросим строку
                    acc_len = 0;
                }

                if (i < (size_t)r && buf[i] == '\n'){
                    acc[acc_len] = '\0';
                    // выводим строку
                    fprintf(stdout, "%s\n", acc);
                    acc_len = 0;
                }
                off = (i < (size_t)r) ? (i+1) : i;
            }
        } else if (r == 0){
            // никто не пишет 
            struct timespec sl = { .tv_sec=0, .tv_nsec=5*1000*1000L };
            nanosleep(&sl, NULL);
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK){
                struct timespec sl = { .tv_sec=0, .tv_nsec=1*1000*1000L };
                nanosleep(&sl, NULL);
            } else if (errno == EINTR){
                continue;
            } else {
                perror("read fifo");
                break;
            }
        }
    }

    fprintf(stdout, "[OBSERVER] stopping...\n"); fflush(stdout);
    if (fd_w_keep >= 0) close(fd_w_keep);
    if (fd_r >= 0) close(fd_r);
    unlink(OBS_FIFO); // удалить fifo
    fprintf(stdout, "[OBSERVER] cleanup done, exit\n"); fflush(stdout);
    return 0;
}
