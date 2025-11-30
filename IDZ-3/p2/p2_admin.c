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
#include <sys/wait.h>
#include <semaphore.h>

#define SHM_NAME "/p2_hotel_shm"
#define SEM_EV_MUTEX "/p2_ev_mutex"
#define SEM_EV_ITEMS "/p2_ev_items"
#define SEM_EV_SLOTS "/p2_ev_slots"
#define SEM_REQ_MUTEX "/p2_req_mutex"

#define ROOMS 10
#define EVQ_CAP 512
#define REQ_CAP 256

static uint64_t g_start_ms=0;
static uint64_t now_ms(void){ struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
  return (uint64_t)ts.tv_sec*1000ull + ts.tv_nsec/1000000ull; }
static void log_msg(const char* tag, const char* fmt, ...){
  struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
  time_t sec=ts.tv_sec; struct tm tm; localtime_r(&sec,&tm);
  uint64_t rel = g_start_ms? ( (uint64_t)ts.tv_sec*1000ull + ts.tv_nsec/1000000ull - g_start_ms):0;
  fprintf(stdout,"[%02d:%02d:%02d.%03d | +%08llu ms][%s][pid=%d] ",
    tm.tm_hour,tm.tm_min,tm.tm_sec,(int)(ts.tv_nsec/1000000),
    (unsigned long long)rel, tag, getpid());
  va_list ap; va_start(ap,fmt); vfprintf(stdout,fmt,ap); va_end(ap);
  fputc('\n',stdout); fflush(stdout);
}

//события
typedef enum { EV_REQUEST=1, EV_CHECKOUT=2 } EvType;
typedef struct {
  EvType type;
  int    req_index;  // индекс в таблице заявок
  int    room;      
  pid_t  pid;
} Event;

// запись заявки
typedef struct {
    pid_t pid;
    int   nights;
    int   room;         // -1 если нет
    int   assigned;     // 1 назначено, -1 отменено, 0 ждёт
    int   from_queue;   // 0 — сразу, 1 — из очереди   ← добавили
    char  sem_name[64]; // имя персонального семафора клиента
  } ReqRec;
  

//общее состояние в shm
typedef struct {
  int occupied[ROOMS];   // -1 свободно, иначе pid
  int ev_head, ev_tail;  // кольцевая очередь
  Event ev[EVQ_CAP];
  ReqRec req[REQ_CAP];
  int shutting_down; // 1 после ctrl+c
} SharedState;

static volatile sig_atomic_t g_stop=0;
static int g_quiet_after_sigint = 1; 
#define LOGQ(SH, TAG, ...) do{ if(!(g_quiet_after_sigint && (SH) && (SH)->shutting_down)) log_msg((TAG), __VA_ARGS__); }while(0)

// именованные семафоры 
static sem_t *ev_mutex=NULL,*ev_items=NULL,*ev_slots=NULL,*req_mutex=NULL;

static void on_sigint(int s){ (void)s; g_stop=1; }
static void set_sigint_admin(void){
  struct sigaction sa={0}; sa.sa_handler=on_sigint; sigemptyset(&sa.sa_mask);
  sa.sa_flags=SA_RESTART; sigaction(SIGINT,&sa,NULL);
}

// открыть/создать именованный семафор
static sem_t* sem_open_reset(const char* name, unsigned init){
  sem_unlink(name);
  sem_t* s = sem_open(name, O_CREAT|O_EXCL, 0666, init);
  if(s==SEM_FAILED){
    perror("sem_open"); fprintf(stderr,"name=%s\n",name); exit(1);
  }
  return s;
}

// очередь событий pop 
static int ev_pop_timed(SharedState* S, Event* out, int timeout_ms){
  struct timespec start, now; clock_gettime(CLOCK_MONOTONIC,&start);
  for(;;){
    int r = sem_trywait(ev_items);
    if(r==0) break;
    if(errno==EAGAIN){
      clock_gettime(CLOCK_MONOTONIC,&now);
      long long elapsed = (now.tv_sec-start.tv_sec)*1000LL + (now.tv_nsec-start.tv_nsec)/1000000LL;
      if(elapsed>=timeout_ms) return 0;
      struct timespec sl={.tv_sec=0,.tv_nsec=1000000L}; nanosleep(&sl,NULL);
      continue;
    }
    if(errno==EINTR) continue;
    perror("sem_trywait(ev_items)"); exit(1);
  }
  while(sem_wait(ev_mutex)==-1 && errno==EINTR){}
  *out = S->ev[S->ev_head];
  S->ev_head = (S->ev_head+1)%EVQ_CAP;
  sem_post(ev_mutex);
  sem_post(ev_slots);
  return 1;
}

// локальная очередь ожидания
typedef struct { int q[REQ_CAP]; int h,t,c; } WaitQ;
static void wq_init(WaitQ* w){ w->h=w->t=w->c=0; }
static void wq_push(WaitQ* w,int x){ w->q[w->t]=x; w->t=(w->t+1)%REQ_CAP; w->c++; }
static int  wq_pop (WaitQ* w){ int x=w->q[w->h]; w->h=(w->h+1)%REQ_CAP; w->c--; return x; }
static int  wq_empty(WaitQ* w){ return w->c==0; }

static int find_free_room(SharedState* S){ for(int i=0;i<ROOMS;i++) if(S->occupied[i]==-1) return i; return -1; }

static void reply_assign(SharedState* S, int idx, int room){
  ReqRec* r=&S->req[idx];
  r->room=room; r->assigned=1;
  sem_t* cli = sem_open(r->sem_name, 0); // клиент уже создал
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

  // shm
  int fd = shm_open(SHM_NAME, O_CREAT|O_RDWR|O_TRUNC, 0666);
  if(fd<0){ perror("shm_open"); return 1; }
  if(ftruncate(fd, sizeof(SharedState))<0){ perror("ftruncate"); return 1; }
  SharedState* S = mmap(NULL,sizeof(SharedState),PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
  if(S==MAP_FAILED){ perror("mmap"); return 1; }
  memset(S,0,sizeof(*S));
  for(int i=0;i<ROOMS;i++) S->occupied[i]=-1;
  S->ev_head=S->ev_tail=0; S->shutting_down=0;
  for(int i=0;i<REQ_CAP;i++){
    S->req[i].pid=0;
    S->req[i].room=-1;
    S->req[i].assigned=0;
    S->req[i].from_queue=0;         
    S->req[i].sem_name[0]=0;
  }
  

  // именованные семафоры
  ev_mutex = sem_open_reset(SEM_EV_MUTEX,1);
  ev_items = sem_open_reset(SEM_EV_ITEMS,0);
  ev_slots = sem_open_reset(SEM_EV_SLOTS,EVQ_CAP);
  req_mutex= sem_open_reset(SEM_REQ_MUTEX,1);

  set_sigint_admin();
  g_start_ms=now_ms();
  log_msg("ADMIN","P2 admin started (named semaphores, POSIX SHM).");

  WaitQ W; wq_init(&W);
  uint64_t t_sig=0; int co_after=0;

  while(1){
    // обработка событий
    Event ev;
    if(ev_pop_timed(S,&ev,200)){
      if(ev.type==EV_REQUEST){
        if(S->shutting_down){ reply_cancel(S, ev.req_index); continue; }
int r = find_free_room(S);
if(r>=0){
  S->req[ev.req_index].from_queue = 0;    
  S->occupied[r]=S->req[ev.req_index].pid;
  reply_assign(S, ev.req_index, r);
}else{
  wq_push(&W, ev.req_index);
  LOGQ(S,"ADMIN","no vacancy -> queued (len=%d)", W.c);
}

      }else if(ev.type==EV_CHECKOUT){
        int room=ev.room; S->occupied[room]=-1;
        if(S->shutting_down) co_after++;
        LOGQ(S,"ADMIN","room #%d is free (pid=%d checked out)", room, ev.pid);
        if(!wq_empty(&W) && !S->shutting_down){
            int idx = wq_pop(&W);
            int r = find_free_room(S);
            if(r>=0){
              S->req[idx].from_queue = 1;     
              S->occupied[r]=S->req[idx].pid;
              reply_assign(S, idx, r);
            }
          }
          
      }
    }

    // ctrl+c
    if(g_stop && !S->shutting_down){
      S->shutting_down=1; t_sig=now_ms();
      log_msg("ADMIN","SIGINT -> shutdown: cancel waiting (%d)", W.c);
      while(!wq_empty(&W)){ reply_cancel(S, wq_pop(&W)); }
    }

    // выходим когда нет занятых номеров, нет очереди и нет ожидаемых событий
    int busy=0; for(int i=0;i<ROOMS;i++) if(S->occupied[i]!=-1){busy=1;break;}
    if(S->shutting_down && !busy && wq_empty(&W)){
      if(g_quiet_after_sigint && t_sig){
        uint64_t dur=now_ms()-t_sig;
        log_msg("ADMIN","shutdown summary: checkouts_after_sigint=%d, took ~%llu ms",
          co_after,(unsigned long long)dur);
      }
      break;
    }
  }

  log_msg("ADMIN","admin exits, cleanup...");

  // cleanup
  sem_close(ev_mutex); sem_close(ev_items); sem_close(ev_slots); sem_close(req_mutex);
  sem_unlink(SEM_EV_MUTEX); sem_unlink(SEM_EV_ITEMS); sem_unlink(SEM_EV_SLOTS); sem_unlink(SEM_REQ_MUTEX);
  munmap(S,sizeof(*S)); close(fd); shm_unlink(SHM_NAME);
  log_msg("ADMIN","cleanup done, exit");
  return 0;
}
