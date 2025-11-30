#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <limits.h>

#define SHM_NAME "/p2_hotel_shm"
#define SEM_EV_MUTEX "/p2_ev_mutex"
#define SEM_EV_ITEMS "/p2_ev_items"
#define SEM_EV_SLOTS "/p2_ev_slots"
#define SEM_REQ_MUTEX "/p2_req_mutex"

#define ROOMS 10
#define EVQ_CAP 512
#define REQ_CAP 256

#define REG_FIFO "/tmp/p4_reg_fifo"
#define OBS_MAX 64

// время/лог
static uint64_t g_start_ms=0;
static uint64_t now_ms(void){ struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
  return (uint64_t)ts.tv_sec*1000ull + ts.tv_nsec/1000000ull; }
static void make_prefix(char* out, size_t z, const char* tag){
  struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
  time_t s=ts.tv_sec; struct tm tm; localtime_r(&s,&tm);
  uint64_t rel = g_start_ms? ( (uint64_t)ts.tv_sec*1000ull + ts.tv_nsec/1000000ull - g_start_ms):0;
  snprintf(out, z, "[%02d:%02d:%02d.%03d | +%08llu ms][%s][pid=%d] ",
    tm.tm_hour,tm.tm_min,tm.tm_sec,(int)(ts.tv_nsec/1000000),
    (unsigned long long)rel, tag, getpid());
}
static void log_msg(const char* tag, const char* fmt, ...){
  char pfx[128]; make_prefix(pfx,sizeof(pfx),tag);
  fputs(pfx, stdout);
  va_list ap; va_start(ap,fmt); vfprintf(stdout,fmt,ap); va_end(ap);
  fputc('\n', stdout); fflush(stdout);
}

// список наблюдателей
typedef struct { char path[PATH_MAX]; int fd; int alive; } Obs;
static Obs g_obs[OBS_MAX];

// сигналы/тихий режим
static volatile sig_atomic_t g_stop=0;
static int g_quiet_after_sigint=1;
static int g_sd=0; // глобальный признак shutdown
static void on_sigint(int s){ (void)s; g_stop=1; }
static void set_sigint_admin(void){ struct sigaction sa={0}; sa.sa_handler=on_sigint; sigemptyset(&sa.sa_mask); sa.sa_flags=SA_RESTART; sigaction(SIGINT,&sa,NULL); }

// fifo утилиты
static void ensure_fifo(const char* path){
  struct stat st;
  if (stat(path, &st)==0){ if (!S_ISFIFO(st.st_mode)) unlink(path); }
  mkfifo(path, 0666);
}
static void add_observer_path(const char* path){
  for(int i=0;i<OBS_MAX;i++){ if (g_obs[i].alive && strcmp(g_obs[i].path,path)==0) return; }
  for(int i=0;i<OBS_MAX;i++){
    if (!g_obs[i].alive){
      strncpy(g_obs[i].path, path, sizeof(g_obs[i].path)-1);
      g_obs[i].path[sizeof(g_obs[i].path)-1]='\0';
      g_obs[i].fd = -1;
      g_obs[i].alive = 1;
      log_msg("ADMIN","observer joined: %s", g_obs[i].path);
      return;
    }
  }
}
static void broadcast_line(const char* line){
  size_t len = strlen(line);
  char out[1024];
  if (len > sizeof(out)-2) len = sizeof(out)-2;
  memcpy(out, line, len); out[len++] = '\n';
  for (int i=0;i<OBS_MAX;i++){
    if (!g_obs[i].alive) continue;
    if (g_obs[i].fd < 0){
      g_obs[i].fd = open(g_obs[i].path, O_WRONLY | O_NONBLOCK);
      if (g_obs[i].fd < 0){ g_obs[i].alive = 0; continue; }
    }
    ssize_t n = write(g_obs[i].fd, out, len);
    if (n < 0 && (errno==EPIPE || errno==ENXIO)){
      close(g_obs[i].fd); g_obs[i].fd = -1; g_obs[i].alive = 0;
    }
  }
}
static void bprintf(const char* tag, const char* fmt, ...){
  char pfx[128]; make_prefix(pfx,sizeof(pfx),tag);
  char body[768];
  va_list ap; va_start(ap,fmt); vsnprintf(body,sizeof(body),fmt,ap); va_end(ap);
  char line[128+768];
  snprintf(line,sizeof(line),"%s%s",pfx,body);
  broadcast_line(line);
}

// макросы логирования 
#define LOG_ADMIN(fmt, ...) \
  do { if (!(g_quiet_after_sigint && g_sd)) { bprintf("ADMIN", fmt, ##__VA_ARGS__); log_msg("ADMIN", fmt, ##__VA_ARGS__); } } while (0)
#define LOG_CLIENT(fmt, ...) \
  do { if (!(g_quiet_after_sigint && g_sd)) { bprintf("CLIENT", fmt, ##__VA_ARGS__); } } while (0)

// структуры данных гостиницы
typedef enum { EV_REQUEST=1, EV_CHECKOUT=2 } EvType;
typedef struct { EvType type; int req_index; int room; pid_t pid; } Event;
typedef struct { pid_t pid; int nights; int room; int assigned; char sem_name[64]; } ReqRec;
typedef struct {
  int occupied[ROOMS];
  int ev_head, ev_tail; Event ev[EVQ_CAP];
  ReqRec req[REQ_CAP];
  int shutting_down;
} SharedState;

static sem_t *ev_mutex=NULL,*ev_items=NULL,*ev_slots=NULL,*req_mutex=NULL;

// pop события 
static int ev_pop_timed(SharedState* S, Event* out, int timeout_ms){
  struct timespec start, now; clock_gettime(CLOCK_MONOTONIC,&start);
  for(;;){
    if (sem_trywait(ev_items)==0) break;
    if (errno==EAGAIN){
      clock_gettime(CLOCK_MONOTONIC,&now);
      long long dt=(now.tv_sec-start.tv_sec)*1000LL+(now.tv_nsec-start.tv_nsec)/1000000LL;
      if (dt>=timeout_ms) return 0;
      struct timespec sl={.tv_sec=0,.tv_nsec=1000000L}; nanosleep(&sl,NULL);
      continue;
    }
    if (errno==EINTR) continue;
    perror("sem_trywait(ev_items)"); exit(1);
  }
  while(sem_wait(ev_mutex)==-1 && errno==EINTR){}
  *out = S->ev[S->ev_head];
  S->ev_head=(S->ev_head+1)%EVQ_CAP;
  sem_post(ev_mutex); sem_post(ev_slots);
  return 1;
}

// локальная очередь
typedef struct { int q[REQ_CAP]; int h,t,c; } WaitQ;
static void wq_init(WaitQ* w){ w->h=w->t=w->c=0; }
static void wq_push(WaitQ* w,int x){ w->q[w->t]=x; w->t=(w->t+1)%REQ_CAP; w->c++; }
static int  wq_pop (WaitQ* w){ int x=w->q[w->h]; w->h=(w->h+1)%REQ_CAP; w->c--; return x; }
static int  wq_empty(WaitQ* w){ return w->c==0; }

// поиск свободного номера
static int find_free_room(SharedState* S){
  for(int i=0;i<ROOMS;i++) if(S->occupied[i]==-1) return i;
  return -1;
}

// ответ клиенту
static void reply_assign(SharedState* S, int idx, int room){
  ReqRec* r=&S->req[idx];
  r->room=room; r->assigned=1;
  sem_t* cli = sem_open(r->sem_name, 0);
  if(cli!=SEM_FAILED){ sem_post(cli); sem_close(cli); }
  LOG_ADMIN("check-in pid=%d to room #%d (req=%d)", r->pid, room, idx);
}
static void reply_cancel(SharedState* S, int idx){
  ReqRec* r=&S->req[idx];
  r->room=-1; r->assigned=-1;
  sem_t* cli = sem_open(r->sem_name, 0);
  if(cli!=SEM_FAILED){ sem_post(cli); sem_close(cli); }
  LOG_ADMIN("cancel pid=%d (req=%d) due to shutdown", r->pid, idx);
}

int main(int argc, char** argv){
  for(int i=1;i<argc;i++){ if(!strcmp(argv[i],"--no-quiet")) g_quiet_after_sigint=0; else { fprintf(stderr,"Usage: %s [--no-quiet]\n", argv[0]); return 1; } }

  // shm + семафоры
  int fd = shm_open(SHM_NAME, O_CREAT|O_RDWR|O_TRUNC, 0666);
  if(fd<0){ perror("shm_open"); return 1; }
  if(ftruncate(fd, sizeof(SharedState))<0){ perror("ftruncate"); return 1; }
  SharedState* S = mmap(NULL,sizeof(SharedState),PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
  if(S==MAP_FAILED){ perror("mmap"); return 1; }
  memset(S,0,sizeof(*S));
  for(int i=0;i<ROOMS;i++) S->occupied[i]=-1;
  for(int i=0;i<REQ_CAP;i++){ S->req[i].pid=0; S->req[i].nights=0; S->req[i].room=-1; S->req[i].assigned=0; S->req[i].sem_name[0]=0; }

  sem_unlink(SEM_EV_MUTEX); sem_unlink(SEM_EV_ITEMS); sem_unlink(SEM_EV_SLOTS); sem_unlink(SEM_REQ_MUTEX);
  sem_t* tmp;
  tmp = sem_open(SEM_EV_MUTEX, O_CREAT|O_EXCL, 0666, 1); if(tmp==SEM_FAILED){ perror("sem_open ev_mutex"); return 1; } ev_mutex=tmp;
  tmp = sem_open(SEM_EV_ITEMS, O_CREAT|O_EXCL, 0666, 0); if(tmp==SEM_FAILED){ perror("sem_open ev_items"); return 1; } ev_items=tmp;
  tmp = sem_open(SEM_EV_SLOTS, O_CREAT|O_EXCL, 0666, EVQ_CAP); if(tmp==SEM_FAILED){ perror("sem_open ev_slots"); return 1; } ev_slots=tmp;
  tmp = sem_open(SEM_REQ_MUTEX, O_CREAT|O_EXCL, 0666, 1); if(tmp==SEM_FAILED){ perror("sem_open req_mutex"); return 1; } req_mutex=tmp;

  // регистрационный fifo
  ensure_fifo(REG_FIFO);
  int reg_r = open(REG_FIFO, O_RDONLY | O_NONBLOCK);
  if (reg_r < 0){ perror("open reg fifo"); return 1; }
  int reg_w_keep = open(REG_FIFO, O_WRONLY | O_NONBLOCK);

  // старт
  set_sigint_admin();
  g_start_ms=now_ms();
  memset(g_obs,0,sizeof(g_obs));
  LOG_ADMIN("P4 admin (FIFO broadcast) started");
  LOG_ADMIN("bus ready, register via %s", REG_FIFO);

  WaitQ W; wq_init(&W);
  uint64_t t_sig=0; int co_after=0;
  char acc[256]; size_t acc_len=0;

  // цикл
  while(1){
    // регистрации наблюдателей
    char buf[256];
    ssize_t r = read(reg_r, buf, sizeof(buf));
    if (r > 0){
      size_t off=0;
      while (off<(size_t)r){
        size_t i=off; while (i<(size_t)r && buf[i]!='\n') i++;
        size_t chunk=i-off;
        if (acc_len+chunk < sizeof(acc)-1){ memcpy(acc+acc_len, buf+off, chunk); acc_len+=chunk; }
        else acc_len=0;
        if (i<(size_t)r && buf[i]=='\n'){
          acc[acc_len]='\0';
          if (acc_len>0) add_observer_path(acc);
          acc_len=0;
        }
        off=(i<(size_t)r)?(i+1):i;
      }
    }

    // события от клиентов
    Event ev;
    if(ev_pop_timed(S,&ev,200)){
      if(ev.type==EV_REQUEST){
        LOG_CLIENT("request from pid=%d (req=%d)", ev.pid, ev.req_index);
        if(S->shutting_down){ reply_cancel(S, ev.req_index); continue; }
        int rr = find_free_room(S);
        if(rr>=0){ S->occupied[rr]=S->req[ev.req_index].pid; reply_assign(S, ev.req_index, rr); }
        else { wq_push(&W, ev.req_index); LOG_ADMIN("no vacancy -> queued (len=%d)", W.c); }
      } else if(ev.type==EV_CHECKOUT){
        int room=ev.room; S->occupied[room]=-1;
        LOG_ADMIN("room #%d is free (pid=%d checked out)", room, ev.pid);
        if(S->shutting_down) co_after++;
        LOG_CLIENT("checkout pid=%d from room #%d (req=%d)", ev.pid, room, ev.req_index);
        if(!wq_empty(&W) && !S->shutting_down){
          int idx = wq_pop(&W);
          int rr = find_free_room(S);
          if(rr>=0){ S->occupied[rr]=S->req[idx].pid; reply_assign(S, idx, rr); }
        }
      }
    }

    // ctrl+c
    if(g_stop && !S->shutting_down){
      S->shutting_down=1; g_sd=1; t_sig=now_ms();
      LOG_ADMIN("SIGINT -> shutdown: cancel waiting (%d)", W.c);
      while(!wq_empty(&W)) reply_cancel(S, wq_pop(&W));
    }

    // условие выхода
    int busy=0; for(int i=0;i<ROOMS;i++) if(S->occupied[i]!=-1){busy=1;break;}
    if(S->shutting_down && !busy && wq_empty(&W)){
      if(g_quiet_after_sigint && t_sig){
        uint64_t dur=now_ms()-t_sig;
        LOG_ADMIN("shutdown summary: checkouts_after_sigint=%d, took ~%llums",
                  co_after,(unsigned long long)dur);
      }
      break;
    }

    usleep(1000);
  }

  LOG_ADMIN("admin exits, cleanup...");

  // cleanup
  sem_close(ev_mutex); sem_close(ev_items); sem_close(ev_slots); sem_close(req_mutex);
  sem_unlink(SEM_EV_MUTEX); sem_unlink(SEM_EV_ITEMS); sem_unlink(SEM_EV_SLOTS); sem_unlink(SEM_REQ_MUTEX);
  munmap(S,sizeof(*S)); close(fd); shm_unlink(SHM_NAME);
  if (reg_w_keep>=0) close(reg_w_keep);
  close(reg_r);

  // финальный лог
  if (!(g_quiet_after_sigint && g_sd)) bprintf("ADMIN","cleanup done, exit");
  log_msg("ADMIN","cleanup done, exit");
  return 0;
}
