#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>

#define REG_FIFO "/tmp/p4_reg_fifo"
#define BUFSZ 2048

// сигналы
static volatile sig_atomic_t g_stop = 0;
static void on_sigint(int s){ (void)s; g_stop=1; }

// утилит
static void ensure_fifo(const char* path){
    struct stat st;
    if (stat(path, &st)==0){
        if (!S_ISFIFO(st.st_mode)) unlink(path); // если был обычный файл то удалить
    }
    mkfifo(path, 0666); // если уже есть fifo то ок
}

int main(void){
    // обработка ctrl+c
    struct sigaction sa={0}; sa.sa_handler=on_sigint; sigemptyset(&sa.sa_mask);
    sa.sa_flags=SA_RESTART; sigaction(SIGINT,&sa,NULL);

    // личный fifo наблюдателя
    char myfifo[128]; snprintf(myfifo,sizeof(myfifo),"/tmp/p4_obs_%d.fifo", getpid());
    ensure_fifo(myfifo);

    // открываем на чтение неблокирующе + держим writer, чтобы не было eof
    int fd_r = open(myfifo, O_RDONLY | O_NONBLOCK);
    if (fd_r < 0) { perror("open myfifo rd"); return 1; }
    int fd_w_keep = open(myfifo, O_WRONLY | O_NONBLOCK); // может быть -1 

    // регистрация в общей трубе
    ensure_fifo(REG_FIFO);
    int reg_w = open(REG_FIFO, O_WRONLY | O_NONBLOCK);
    if (reg_w >= 0) { dprintf(reg_w, "%s\n", myfifo); close(reg_w); }

    fprintf(stdout, "[OBSERVER %d] started, fifo=%s\n", getpid(), myfifo); fflush(stdout);

    // аккуратное чтение по строкам
    char acc[BUFSZ]; size_t acc_len=0;
    while(!g_stop){
        char buf[BUFSZ];
        ssize_t r = read(fd_r, buf, sizeof(buf));
        if (r > 0){
            size_t off=0;
            while (off<(size_t)r){
                size_t i=off; while (i<(size_t)r && buf[i]!='\n') i++;
                size_t chunk=i-off;
                if (acc_len+chunk < sizeof(acc)-1){ memcpy(acc+acc_len, buf+off, chunk); acc_len+=chunk; }
                else acc_len=0; // переполнение то сброс
                if (i<(size_t)r && buf[i]=='\n'){
                    acc[acc_len]='\0';
                    puts(acc);
                    acc_len=0;
                }
                off=(i<(size_t)r)?(i+1):i;
            }
        } else if (r==0 || (r<0 && (errno==EAGAIN || errno==EWOULDBLOCK))){
            // периодическая пере-регистрация (если админ стартовал позже)
            int reg_try = open(REG_FIFO, O_WRONLY | O_NONBLOCK);
            if (reg_try >= 0){ dprintf(reg_try, "%s\n", myfifo); close(reg_try); }
            usleep(2000); // маленькая пауза
        } else if (errno==EINTR){
            continue; // перезапуск после сигнала
        } else {
            perror("read myfifo");
            break;
        }
    }

    fprintf(stdout, "[OBSERVER %d] stopping\n", getpid()); fflush(stdout);
    if (fd_w_keep>=0) close(fd_w_keep);
    close(fd_r);
    return 0;
}
