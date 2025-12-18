#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char g_tmpdir[PATH_MAX];  
static int  g_created = 0;     

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static void cleanup(void) {
    // если папка не создана то выходим
    if (g_tmpdir[0] == '\0') return;

    // открываем директорию и удаляем всё внутри 
    int dfd = open(g_tmpdir, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        // удаляем симлинки
        for (int d = g_created; d >= 1; --d) {
            char name[32];
            snprintf(name, sizeof(name), "s%03d", d);
            (void)unlinkat(dfd, name, 0);
        }
        // удаляем базовый файл
        (void)unlinkat(dfd, "a", 0);
        (void)close(dfd);
    }

    // удаляем саму директорию
    (void)rmdir(g_tmpdir);
}

int main(void) {
    // создаём временную директорию
    char templ[] = "/tmp/symlink-depth.XXXXXX";
    char *dir = mkdtemp(templ);
    if (!dir) die("mkdtemp");
    snprintf(g_tmpdir, sizeof(g_tmpdir), "%s", dir);
    atexit(cleanup);

    if (chdir(g_tmpdir) != 0) die("chdir");

    // создаём регулярный файл a
    int fd = open("a", O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) die("open(a)");
    if (write(fd, "x", 1) != 1) {
        int e = errno;
        (void)close(fd);
        errno = e;
        die("write(a)");
    }
    if (close(fd) != 0) die("close(a)");

    // для следующего симлинка
    char prev[NAME_MAX];
    snprintf(prev, sizeof(prev), "%s", "a");

    int depth = 0;
    int last_ok = 0;
    int fail_errno = 0;

    for (;;) {
        ++depth;
        char cur[NAME_MAX];
        snprintf(cur, sizeof(cur), "s%03d", depth);

        // создаём cur - prev
        if (symlink(prev, cur) != 0) die("symlink");
        g_created = depth;

        // открываем
        fd = open(cur, O_RDONLY);
        if (fd >= 0) {
            (void)close(fd);
            last_ok = depth;
            snprintf(prev, sizeof(prev), "%s", cur);
            continue;
        }
        fail_errno = errno;
        break;
    }

    // вывод результатов
    printf("рабочая директория: %s\n", g_tmpdir);
    printf("последняя успешная глубина (open() сработал): %d\n", last_ok);
    printf("первая неуспешная попытка открытия на глубине: %d\n", last_ok + 1);
    printf("код ошибки open(): %s\n", strerror(fail_errno));

    if (fail_errno == ELOOP) {
        printf("итог: достигнут предел уровней символьных ссылок при open()\n");
        return 0;
    } else {
        printf("предупреждение: ошибка не ELOOP, возможно упёрлись в другой лимит\n");
        return 2;
    }
}
