// Кочарян Элина АКОС дз8 БПИ-244

#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>   
#include <string.h>

static void err(const char *what) {
    // печатаем системную ошибку и выходим
    perror(what);
    _exit(1);
}

static void write_all(int fd, const void *buf, size_t n) {
    // запишем всё, учитывая частичные записи и eintr
    const char *p = (const char*)buf;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            err("write");
        }
        p += (size_t)w;
        n -= (size_t)w;
    }
}

static void wstr(int fd, const char *s) {
    // обёртка для строк
    write_all(fd, s, strlen(s));
}

static void help(const char *prog) {
    // мини-подсказка
    wstr(STDERR_FILENO, "usage: ");
    wstr(STDERR_FILENO, prog);
    wstr(STDERR_FILENO,
         " [-32] <src> <dst>\n"
         "  -32  copy using fixed 32-byte buffer\n"
         "  default: single buffer bigger than source file\n");
}

int main(int argc, char **argv) {
    int mode32 = 0;                 
    const char *src = NULL, *dst = NULL;

    // разбор аргументов
    if (argc >= 2 && strcmp(argv[1], "-32") == 0) {
        mode32 = 1;
        if (argc == 4) { src = argv[2]; dst = argv[3]; }
    } else if (argc == 3) {
        src = argv[1]; dst = argv[2];
    }
    if (!src || !dst) { help(argv[0]); return 2; }

    int sfd = -1, dfd = -1;
    struct stat st_src, st_dst;

    // открываем источник
    sfd = open(src, O_RDONLY);
    if (sfd < 0) err("open src");
    if (fstat(sfd, &st_src) < 0) err("fstat src");

    // запретим каталоги
    if (S_ISDIR(st_src.st_mode)) { wstr(STDERR_FILENO, "source is a directory\n"); _exit(3); }

    // проверяем, что не тот же файл и не каталог
    if (stat(dst, &st_dst) == 0) {
        if (S_ISDIR(st_dst.st_mode)) { wstr(STDERR_FILENO, "destination is a directory\n"); _exit(3); }
        if (st_dst.st_ino == st_src.st_ino && st_dst.st_dev == st_src.st_dev) {
            wstr(STDERR_FILENO, "refusing to copy file onto itself\n");
            _exit(3);
        }
    }

    // создаём с минимальными правами, потом выставим точные
    dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (dfd < 0) { err("open dst"); }

    if (mode32) {
        // 32-байтный буфер
        char buf[32];
        for (;;) {
            ssize_t r = read(sfd, buf, sizeof buf);
            if (r < 0) { if (errno == EINTR) continue; err("read"); }
            if (r == 0) break;
            write_all(dfd, buf, (size_t)r);
        }
    } else {
        // один буфер больше файла
        size_t cap = (st_src.st_size > 0) ? (size_t)st_src.st_size + 1u : 1u;
        void *buf = malloc(cap);
        if (!buf) err("malloc");
        // одно чтение, буфер больше
        ssize_t r;
        for (;;) {
            r = read(sfd, buf, cap);
            if (r < 0) { if (errno == EINTR) continue; free(buf); err("read"); }
            break;
        }
        if (r > 0) write_all(dfd, buf, (size_t)r);
        free(buf);
    }

    // переносим права доступа
    if (fchmod(dfd, st_src.st_mode & 07777) < 0) err("fchmod");
    // закрываем
    if (dfd >= 0 && close(dfd) < 0) err("close dst"); 
    if (sfd >= 0 && close(sfd) < 0) err("close src");
    return 0;
}
