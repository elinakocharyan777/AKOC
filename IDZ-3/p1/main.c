#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <semaphore.h>

#define SHM_NAME "/p1_hotel_shm" 

#define ROOMS 10
#define MAX_CLIENTS 128
#define EVQ_CAP 512

// время и лог
static uint64_t now_ms(void){
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec*1000ull + ts.tv_nsec/1000000ull;
}
static uint64_t g_start_ms = 0; // старт программы

// для работы после ctrl+c
static int g_quiet_after_sigint = 1;

// единый лог
static void log_msg(const char* tag, const char* fmt, ...) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    time_t sec = ts.tv_sec; struct tm tm_local; localtime_r(&sec, &tm_local);
    int milli = (int)(ts.tv_nsec / 1000000);
    uint64_t now_abs_ms = (uint64_t)ts.tv_sec * 1000ull + ts.tv_nsec / 1000000ull;
    uint64_t rel_ms = (g_start_ms ? (now_abs_ms - g_start_ms) : 0ull);

    fprintf(stdout, "[%02d:%02d:%02d.%03d | +%08llu ms][%s][pid=%d] ", tm_local.tm_hour, tm_local.tm_min, tm_local.tm_sec, milli,(unsigned long long)rel_ms, tag, getpid());

    va_list ap; va_start(ap, fmt); vfprintf(stdout, fmt, ap); va_end(ap);
    fputc('\n', stdout); fflush(stdout);
}

// условный лог (молчит после сигинт)
#define LOGQ(SHARED_PTR, TAG, ...) \
    do { \
        if(!(g_quiet_after_sigint && (SHARED_PTR) && (SHARED_PTR)->shutting_down)) \
            log_msg((TAG), __VA_ARGS__); \
    } while(0)

// события 
typedef enum { EV_REQUEST=1, EV_CHECKOUT=2 } EvType;
typedef struct {
    EvType type;
    int    cli_index;
    int    room; // для checkout 
    pid_t  pid;
} Event;

typedef struct {
    pid_t pid;
    int   room; // -1 если нет
    int   assigned; // 1 назначено, -1 отменено, 0 ждёт
} Request;

// общее состояние
typedef struct {
    int occupied[ROOMS]; // -1 свободно, иначе pid

    int ev_head, ev_tail;
    Event ev[EVQ_CAP];
    sem_t ev_mutex, ev_items, ev_slots;

    Request req[MAX_CLIENTS];
    sem_t   cli_sem[MAX_CLIENTS];

    int shutting_down; // 1 после ctrl+c
} SharedState;

// сигналы
static volatile sig_atomic_t g_stop=0;
static void on_sigint(int s){ (void)s; g_stop=1; } // помечаем остановку

static void set_sigint_admin(void){
    struct sigaction sa={0};
    sa.sa_handler = on_sigint;   // админ ловит ctrl+c
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;    // где можно — рестарт системных вызовов
    sigaction(SIGINT, &sa, NULL);
}
static void set_sigint_client(void){
    struct sigaction sa={0};
    sa.sa_handler = SIG_IGN;     // клиенты игнорируют ctrl+c
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
}

// очередь событий
static void ev_push(SharedState* S, const Event* e){
    while (sem_wait(&S->ev_slots) == -1 && errno == EINTR) { /* retry */ }
    while (sem_wait(&S->ev_mutex) == -1 && errno == EINTR) { /* retry */ }
    S->ev[S->ev_tail] = *e;
    S->ev_tail = (S->ev_tail+1) % EVQ_CAP;
    sem_post(&S->ev_mutex);
    sem_post(&S->ev_items);
}

//
static int ev_pop_timed(SharedState* S, Event* out, int timeout_ms){
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for(;;){
        int r = sem_trywait(&S->ev_items);
        if (r == 0) break; // получили элемент
        if (r == -1){
            if (errno == EAGAIN){
                clock_gettime(CLOCK_MONOTONIC, &now);
                long long elapsed_ms =
                    (now.tv_sec - start.tv_sec) * 1000LL +
                    (now.tv_nsec - start.tv_nsec) / 1000000LL;
                if (elapsed_ms >= timeout_ms) return 0; // таймаут
                struct timespec sl = { .tv_sec = 0, .tv_nsec = 1000000L }; // 1 мс
                nanosleep(&sl, NULL);
                continue;
            }
            if (errno == EINTR) continue; // повторить
            perror("sem_trywait(ev_items)"); exit(1);
        }
    }

    while (sem_wait(&S->ev_mutex) == -1 && errno == EINTR) { /* retry */ }
    *out = S->ev[S->ev_head];
    S->ev_head = (S->ev_head+1) % EVQ_CAP;
    sem_post(&S->ev_mutex);
    sem_post(&S->ev_slots);
    return 1;
}

// локальная fifo у администратора
typedef struct {
    int q[MAX_CLIENTS];
    int head, tail, cnt;
} WaitQ;
static void wq_init(WaitQ* q){ q->head=q->tail=q->cnt=0; }
static void wq_push(WaitQ* q, int x){ q->q[q->tail]=x; q->tail=(q->tail+1)%MAX_CLIENTS; q->cnt++; }
static int  wq_pop (WaitQ* q){ int x=q->q[q->head]; q->head=(q->head+1)%MAX_CLIENTS; q->cnt--; return x; }
static int  wq_empty(WaitQ* q){ return q->cnt==0; }

// утилиты 
static int find_free_room(SharedState* S){
    for(int i=0;i<ROOMS;i++) if(S->occupied[i]==-1) return i;
    return -1;
}
static void assign_room(SharedState* S, int cli_index, pid_t pid, int room){
    S->occupied[room] = pid;
    S->req[cli_index].room = room;
    S->req[cli_index].assigned = 1;
    sem_post(&S->cli_sem[cli_index]);
}
static void cancel_waiter(SharedState* S, int cli_index){
    S->req[cli_index].room = -1;
    S->req[cli_index].assigned = -1;
    sem_post(&S->cli_sem[cli_index]);
}

// клиент
static void client_proc(SharedState* S, int cli_index, int nights, int day_ms){
    S->req[cli_index].pid      = getpid();
    S->req[cli_index].room     = -1;
    S->req[cli_index].assigned = 0;

    Event ev = { .type=EV_REQUEST, .cli_index=cli_index, .pid=getpid() };
    ev_push(S, &ev);
    LOGQ(S, "CLIENT", "request: need %d night(s)", nights);

    sem_wait(&S->cli_sem[cli_index]); // ждём назначение или отмену

    if(S->req[cli_index].assigned < 0){
        LOGQ(S, "CLIENT", "exit: request canceled (shutdown)");
        _exit(0);
    }
    int room = S->req[cli_index].room;
    LOGQ(S, "CLIENT", "got room #%d, staying %d night(s)", room, nights);

    usleep((useconds_t)((uint64_t)nights * (uint64_t)day_ms * 1000ULL));

    Event ev2 = { .type=EV_CHECKOUT, .cli_index=cli_index, .room=room, .pid=getpid() };
    ev_push(S, &ev2);
    LOGQ(S, "CLIENT", "checkout room #%d", room);
    _exit(0);
}

// администратор
static void admin_loop(SharedState* S, int total_clients){
    WaitQ W; wq_init(&W);
    int finished = 0;

    // счётчики для сводки
    uint64_t t_sigint = 0;
    int co_after = 0;   // checkout после сигинт
    int fin_after = 0;  // завершилось после сигинт

    while(1){
        Event ev;
        if(ev_pop_timed(S, &ev, 200)){
            if(ev.type==EV_REQUEST){
                int free_room = find_free_room(S);
                if(S->shutting_down){
                    LOGQ(S, "ADMIN", "cancel client %d (pid=%d) due to shutdown", ev.cli_index, ev.pid);
                    cancel_waiter(S, ev.cli_index);
                } else if(free_room>=0){
                    assign_room(S, ev.cli_index, ev.pid, free_room);
                    LOGQ(S, "ADMIN", "check-in pid=%d to room #%d (free immediately)", ev.pid, free_room);
                } else {
                    wq_push(&W, ev.cli_index);
                    LOGQ(S, "ADMIN", "no vacancy -> queued (len=%d)", W.cnt);
                }
            } else if(ev.type==EV_CHECKOUT){
                int room = ev.room;
                S->occupied[room] = -1;
                if(S->shutting_down) co_after++;
                LOGQ(S, "ADMIN", "room #%d is free (pid=%d checked out)", room, ev.pid);
                if(!wq_empty(&W) && !S->shutting_down){
                    int cli = wq_pop(&W);
                    int r = find_free_room(S);
                    if(r>=0){
                        assign_room(S, cli, S->req[cli].pid, r);
                        LOGQ(S, "ADMIN", "from queue -> check-in pid=%d to #%d (queue left=%d)",
                                S->req[cli].pid, r, W.cnt);
                    }
                }
            }
        }

        // собираем завершившихся детей
        while(1){
            int st=0; pid_t p = waitpid(-1, &st, WNOHANG);
            if(p<=0) break;
            finished++;
            if(S->shutting_down) fin_after++;
            LOGQ(S, "ADMIN", "client pid=%d finished (%d/%d)", p, finished, total_clients);
        }

        // обработка ctrl+c
        if(g_stop && !S->shutting_down){
            S->shutting_down=1;
            if(!t_sigint) t_sigint = now_ms();
            log_msg("ADMIN", "SIGINT -> shutdown: cancel waiting (%d)", W.cnt); 
            while(!wq_empty(&W)){
                int cli = wq_pop(&W);
                cancel_waiter(S, cli);
            }
        }

        if(finished >= total_clients) break;
    }

    // короткая сводка 
    if (t_sigint && g_quiet_after_sigint){
        uint64_t dur = now_ms() - t_sigint;
        log_msg("ADMIN", "shutdown summary: checkouts=%d, finished_after_sigint=%d, took ~%llu ms",
                co_after, fin_after, (unsigned long long)dur);
    }

    log_msg("ADMIN", "all clients finished, admin exits");
}

// справка
static void usage(const char* prog){
    fprintf(stderr,
        "Usage: %s --clients N [--min-d A] [--max-d B] [--day-ms MS] [--no-quiet]\n"
        "  N <= %d, ROOMS=%d\n", prog, MAX_CLIENTS, ROOMS);
}

// точка входа
int main(int argc, char** argv){
    int clients = -1, min_d=1, max_d=3, day_ms=500;

    // парсинг аргументов
    for(int i=1;i<argc;i++){
        if(!strcmp(argv[i],"--clients") && i+1<argc) clients=atoi(argv[++i]);
        else if(!strcmp(argv[i],"--min-d") && i+1<argc) min_d=atoi(argv[++i]);
        else if(!strcmp(argv[i],"--max-d") && i+1<argc) max_d=atoi(argv[++i]);
        else if(!strcmp(argv[i],"--day-ms") && i+1<argc) day_ms=atoi(argv[++i]);
        else if(!strcmp(argv[i],"--no-quiet")) g_quiet_after_sigint = 0;
        else { usage(argv[0]); return 1; }
    }
    if(clients<=0 || clients>MAX_CLIENTS || min_d<=0 || max_d<min_d){
        usage(argv[0]); return 1;
    }

    // shm
    int fd = shm_open(SHM_NAME, O_CREAT|O_RDWR, 0666);
    if(fd<0){ perror("shm_open"); return 1; }
    if(ftruncate(fd, sizeof(SharedState))<0){ perror("ftruncate"); return 1; }
    SharedState* S = mmap(NULL, sizeof(SharedState), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(S==MAP_FAILED){ perror("mmap"); return 1; }

    // инициализация
    memset(S, 0, sizeof(*S));
    for(int i=0;i<ROOMS;i++) S->occupied[i]=-1;
    S->ev_head=S->ev_tail=0;
    S->shutting_down=0;

    if( sem_init(&S->ev_mutex, 1, 1) || sem_init(&S->ev_items, 1, 0) || sem_init(&S->ev_slots, 1, EVQ_CAP) ){
        perror("sem_init ev_*"); return 1;
    }
    for(int i=0;i<MAX_CLIENTS;i++){
        S->req[i].assigned=0;
        S->req[i].room=-1;
        if( sem_init(&S->cli_sem[i], 1, 0) ){ perror("sem_init cli_sem"); return 1; }
    }

    // сигналы и лог старта
    set_sigint_admin();
    g_start_ms = now_ms();
    log_msg("ADMIN", "build: %s %s", __DATE__, __TIME__);
    log_msg("ADMIN", "start: clients=%d, nights=[%d..%d], day_ms=%d", clients, min_d, max_d, day_ms);

    // форк клиентов
    for(int i=0;i<clients;i++){
        pid_t p = fork();
        if(p<0){ perror("fork"); exit(1); }
        if(p==0){
            set_sigint_client();
            srand(getpid() ^ now_ms());
            int nights = min_d + (rand()% (max_d - min_d + 1));
            client_proc(S, i, nights, day_ms);
        }
    }

    // цикл админа
    admin_loop(S, clients);

    // очистка
    for(int i=0;i<MAX_CLIENTS;i++){
        sem_destroy(&S->cli_sem[i]);
    }
    sem_destroy(&S->ev_mutex);
    sem_destroy(&S->ev_items);
    sem_destroy(&S->ev_slots);

    munmap(S, sizeof(*S));
    close(fd);
    shm_unlink(SHM_NAME);

    log_msg("ADMIN", "cleanup done, exit");
    return 0;
}
