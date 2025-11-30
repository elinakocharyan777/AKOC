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

#define SHM_NAME "/p2_hotel_shm"          
#define SEM_EV_MUTEX "/p2_ev_mutex"
#define SEM_EV_ITEMS "/p2_ev_items"
#define SEM_EV_SLOTS "/p2_ev_slots"
#define SEM_REQ_MUTEX "/p2_req_mutex"

#define ROOMS 10
#define EVQ_CAP 512
#define REQ_CAP 256

#define OBS_FIFO "/tmp/p3_observer.fifo"   // fifo наблюдателя

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

// отправка в fifo наблюдателя
static void obs_send_line(const char* line){
    static int fd = -1;

    // одноразовое открытие fifo(если наблюдатель не запущен то выходим)
    if (fd == -1) {
        fd = open(OBS_FIFO, O_WRONLY | O_NONBLOCK);
        if (fd == -1) return; // нет читателя 
    }

    // формируем "строка\n" и записываем целиком 
    char buf[640];
    size_t len = strlen(line);
    if (len > sizeof(buf) - 2) len = sizeof(buf) - 2; // обрезаем слишком длинное
    memcpy(buf, line, len);
    buf[len++] = '\n';

    size_t off = 0;
    for (int tries = 0; tries < 2 && off < len; ++tries) { 
        ssize_t n = write(fd, buf + off, len - off);
        if (n > 0) { off += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) { continue; }  // повторить
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {  // не блокируемся
            break;
        }
        // другая ошибка то прекращаем 
        break;
    }
}
static void obs_printf(const char* tag, const char* fmt, ...){
  char pfx[128]; make_prefix(pfx,sizeof(pfx),tag);
  char body[512];
  va_list ap; va_start(ap,fmt); vsnprintf(body,sizeof(body),fmt,ap); va_end(ap);
  char line[128+512];
  snprintf(line, sizeof(line), "%s%s", pfx, body);
  obs_send_line(line);
}

typedef enum { EV_REQUEST=1, EV_CHECKOUT=2 } EvType;
typedef struct { EvType type; int req_index; int room; pid_t pid; } Event;
typedef struct {
  pid_t pid; int nights; int room; int assigned; char sem_name[64];
} ReqRec;
typedef struct {
  int occupied[ROOMS];
  int ev_head, ev_tail; Event ev[EVQ_CAP];
  ReqRec req[REQ_CAP];
  int shutting_down;
} SharedState;

static sem_t *ev_mutex=NULL,*ev_items=NULL,*ev_slots=NULL,*req_mutex=NULL;
static volatile sig_atomic_t g_stop=0;
static int g_quiet_after_sigint=1;

static void on_sigint(int s){ (void)s; g_stop=1; }
static void set_sigint_admin(void){
  struct sigaction sa={0}; sa.sa_handler=on_sigint; sigemptyset(&sa.sa_mask);
  sa.sa_flags=SA_RESTART; sigaction(SIGINT,&sa,NULL);
}
#define LOGQ(SH, TAG, ...) do{ if(!(g_quiet_after_sigint && (SH) && (SH)->shutting_down)){ log_msg((TAG), __VA_ARGS__); obs_printf((TAG), __VA_ARGS__);} }while(0)

//pop событий
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
  S->ev_head = (S->ev_head+1) % EVQ_CAP;
  sem_post(ev_mutex); sem_post(ev_slots);
  return 1;
}

typedef struct { int q[REQ_CAP]; int h,t,c; } WaitQ;
static void wq_init(WaitQ* w){ w->h=w->t=w->c=0; }
static void wq_push(WaitQ* w,int x){ w->q[w->t]=x; w->t=(w->t+1)%REQ_CAP; w->c++; }
static int  wq_pop (WaitQ* w){ int x=w->q[w->h]; w->h=(w->h+1)%REQ_CAP; w->c--; return x; }
static int  wq_empty(WaitQ* w){ return w->c==0; }

static int find_free_room(SharedState* S){ for(int i=0;i<ROOMS;i++) if(S->occupied[i]==-1) return i; return -1; }

static void reply_assign(SharedState* S, int idx, int room){
  ReqRec* r=&S->req[idx];
  r->room=room; r->assigned=1;
  sem_t* cli = sem_open(r->sem_name, 0);
  if(cli!=SEM_FAILED){ sem_post(cli); sem_close(cli); }
  LOGQ(S,"ADMIN","check-in pid=%d to room #%d (req=%d)", r->pid, room, idx);
}
static void reply_cancel(SharedState* S, int idx){
  ReqRec* r=&S->req[idx];
  r->room=-1; r->assigned=-1;
  sem_t* cli = sem_open(r->sem_name, 0);
  if(cli!=SEM_FAILED){ sem_post(cli); sem_close(cli); }
  LOGQ(S,"ADMIN","cancel pid=%d (req=%d) due to shutdown", r->pid, idx);
}

int main(int argc, char** argv){
  for(int i=1;i<argc;i++){
    if(!strcmp(argv[i],"--no-quiet")) g_quiet_after_sigint=0;
    else { fprintf(stderr,"Usage: %s [--no-quiet]\n", argv[0]); return 1; }
  }

  int fd = shm_open(SHM_NAME, O_CREAT|O_RDWR|O_TRUNC, 0666);
  if(fd<0){ perror("shm_open"); return 1; }
  if(ftruncate(fd, sizeof(SharedState))<0){ perror("ftruncate"); return 1; }
  SharedState* S = mmap(NULL,sizeof(SharedState),PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
  if(S==MAP_FAILED){ perror("mmap"); return 1; }
  memset(S,0,sizeof(*S));
  for(int i=0;i<ROOMS;i++) S->occupied[i]=-1;
  // инициализация таблицы заявок
for (int i = 0; i < REQ_CAP; i++) {
    S->req[i].pid = 0;
    S->req[i].nights = 0;
    S->req[i].room = -1;          
    S->req[i].assigned = 0;
    S->req[i].sem_name[0] = '\0';
}


  ev_mutex = sem_open("/p2_ev_mutex", O_CREAT|O_EXCL, 0666, 1);
  if(ev_mutex==SEM_FAILED){ sem_unlink(SEM_EV_MUTEX); ev_mutex=sem_open(SEM_EV_MUTEX,O_CREAT|O_EXCL,0666,1);}
  ev_items = sem_open("/p2_ev_items", O_CREAT|O_EXCL, 0666, 0);
  if(ev_items==SEM_FAILED){ sem_unlink(SEM_EV_ITEMS); ev_items=sem_open(SEM_EV_ITEMS,O_CREAT|O_EXCL,0666,0);}
  ev_slots = sem_open("/p2_ev_slots", O_CREAT|O_EXCL, 0666, EVQ_CAP);
  if(ev_slots==SEM_FAILED){ sem_unlink(SEM_EV_SLOTS); ev_slots=sem_open(SEM_EV_SLOTS,O_CREAT|O_EXCL,0666,EVQ_CAP);}
  req_mutex= sem_open("/p2_req_mutex", O_CREAT|O_EXCL, 0666, 1);
  if(req_mutex==SEM_FAILED){ sem_unlink(SEM_REQ_MUTEX); req_mutex=sem_open(SEM_REQ_MUTEX,O_CREAT|O_EXCL,0666,1);}

  set_sigint_admin();
  g_start_ms=now_ms();
  log_msg("ADMIN","P3 admin started (named semaphores + FIFO observer).");
  obs_printf("ADMIN","observer link ready (best-effort)");

  WaitQ W; wq_init(&W);
  uint64_t t_sig=0; int co_after=0;

  while(1){
    Event ev;
    if(ev_pop_timed(S,&ev,200)){
      if(ev.type==EV_REQUEST){
        if(S->shutting_down){ reply_cancel(S, ev.req_index); continue; }
        int r = find_free_room(S);
        if(r>=0){ S->occupied[r]=S->req[ev.req_index].pid; reply_assign(S, ev.req_index, r); }
        else { wq_push(&W, ev.req_index); LOGQ(S,"ADMIN","no vacancy -> queued (len=%d)", W.c); }
      } else if(ev.type==EV_CHECKOUT){
        int room=ev.room; S->occupied[room]=-1;
        if(S->shutting_down) co_after++;
        LOGQ(S,"ADMIN","room #%d is free (pid=%d checked out)", room, ev.pid);
        if(!wq_empty(&W) && !S->shutting_down){
          int idx = wq_pop(&W);
          int r = find_free_room(S);
          if(r>=0){ S->occupied[r]=S->req[idx].pid; reply_assign(S, idx, r); }
        }
      }
    }

    if(g_stop && !S->shutting_down){
      S->shutting_down=1; t_sig=now_ms();
      log_msg("ADMIN","SIGINT -> shutdown: cancel waiting (%d)", W.c);
      obs_printf("ADMIN","SIGINT -> shutdown: cancel waiting (%d)", W.c);
      while(!wq_empty(&W)){ reply_cancel(S, wq_pop(&W)); }
    }

    int busy=0; for(int i=0;i<ROOMS;i++) if(S->occupied[i]!=-1){busy=1;break;}
    if(S->shutting_down && !busy && wq_empty(&W)){
      if(g_quiet_after_sigint && t_sig){
        uint64_t dur=now_ms()-t_sig;
        log_msg("ADMIN","shutdown summary: checkouts_after_sigint=%d, took ~%llu ms",
          co_after,(unsigned long long)dur);
        obs_printf("ADMIN","shutdown summary: checkouts_after_sigint=%d, took ~%llu ms",
          co_after,(unsigned long long)dur);
      }
      break;
    }
  }

  log_msg("ADMIN","admin exits, cleanup...");
  obs_printf("ADMIN","cleanup...");

  // cleanup
  sem_close(ev_mutex); sem_close(ev_items); sem_close(ev_slots); sem_close(req_mutex);
  sem_unlink(SEM_EV_MUTEX); sem_unlink(SEM_EV_ITEMS); sem_unlink(SEM_EV_SLOTS); sem_unlink(SEM_REQ_MUTEX);
  munmap(S,sizeof(*S)); close(fd); shm_unlink(SHM_NAME);
  log_msg("ADMIN","cleanup done, exit");
  obs_printf("ADMIN","cleanup done, exit");
  return 0;
}
