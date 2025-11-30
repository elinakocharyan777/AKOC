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

// события/общая память
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

// push события в очередь
static void ev_push(SharedState* S, const Event* e){
  while(sem_wait(ev_slots)==-1 && errno==EINTR){}
  while(sem_wait(ev_mutex)==-1 && errno==EINTR){}
  S->ev[S->ev_tail]=*e;
  S->ev_tail=(S->ev_tail+1)%EVQ_CAP;
  sem_post(ev_mutex); sem_post(ev_items);
}

// выделить свободный слот заявки
static int allocate_req_index(SharedState* S){
  while(sem_wait(req_mutex)==-1 && errno==EINTR){}
  int idx=-1;
  for(int i=0;i<REQ_CAP;i++){
    if(S->req[i].pid==0 && S->req[i].assigned==0 && S->req[i].room==-1){
      idx=i; S->req[i].pid=getpid(); break;
    }
  }
  sem_post(req_mutex);
  return idx;
}

// очистить слот заявки
static void clear_req(SharedState* S, int idx){
  while(sem_wait(req_mutex)==-1 && errno==EINTR){}
  S->req[idx].pid=0; S->req[idx].nights=0; S->req[idx].room=-1; S->req[idx].assigned=0; S->req[idx].sem_name[0]=0;
  sem_post(req_mutex);
}

// справка
static void usage(const char* p){
  fprintf(stderr,"Usage: %s --min-d A --max-d B --day-ms MS [--quiet]\n", p);
}

int main(int argc, char** argv){
  int min_d=1, max_d=3, day_ms=500, quiet=0; // по умолчанию говорим
  for(int i=1;i<argc;i++){
    if(!strcmp(argv[i],"--min-d")&&i+1<argc)       min_d=atoi(argv[++i]);
    else if(!strcmp(argv[i],"--max-d")&&i+1<argc)  max_d=atoi(argv[++i]);
    else if(!strcmp(argv[i],"--day-ms")&&i+1<argc) day_ms=atoi(argv[++i]);
    else if(!strcmp(argv[i],"--quiet"))            quiet=1;
    else if(!strcmp(argv[i],"--no-quiet"))         quiet=0; // совместимость
    else { usage(argv[0]); return 1; }
  }
  if(min_d<=0 || max_d<min_d || day_ms<=0){ usage(argv[0]); return 1; }

  g_start_ms=now_ms();

  // подключение к shm (ожидаем админа)
  int fd=-1; SharedState* S=MAP_FAILED;
  for(int attempt=0; attempt<50; attempt++){
    fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if(fd>=0){
      S = mmap(NULL,sizeof(SharedState),PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
      close(fd);
      if(S!=MAP_FAILED) break;
    }
    struct timespec sl={.tv_sec=0,.tv_nsec=20*1000*1000L}; nanosleep(&sl,NULL);
  }
  if(S==MAP_FAILED){ perror("shm_open/mmap"); fprintf(stderr,"admin is not running?\n"); return 1; }

  // именованные семафоры
  ev_mutex = sem_open(SEM_EV_MUTEX,0);
  ev_items = sem_open(SEM_EV_ITEMS,0);
  ev_slots = sem_open(SEM_EV_SLOTS,0);
  req_mutex= sem_open(SEM_REQ_MUTEX,0);
  if(ev_mutex==SEM_FAILED||ev_items==SEM_FAILED||ev_slots==SEM_FAILED||req_mutex==SEM_FAILED){
    perror("sem_open"); fprintf(stderr,"admin is not running?\n"); return 1;
  }

  // личный семафор ответа
  char mysem_name[64]; snprintf(mysem_name,sizeof(mysem_name),"/p2_cli_%d", getpid());
  sem_unlink(mysem_name);
  sem_t* mysem = sem_open(mysem_name, O_CREAT|O_EXCL, 0666, 0);
  if(mysem==SEM_FAILED){ perror("sem_open(mysem)"); return 1; }

  // слот заявки
  int idx = allocate_req_index(S);
  if(idx<0){ if(!quiet) log_msg("CLIENT","no free request slots"); sem_close(mysem); sem_unlink(mysem_name); return 1; }
  srand(getpid() ^ now_ms());
  int nights = min_d + (rand()% (max_d-min_d+1));

  S->req[idx].nights = nights;
  S->req[idx].room   = -1;
  S->req[idx].assigned = 0;
  snprintf(S->req[idx].sem_name, sizeof(S->req[idx].sem_name), "%s", mysem_name);

  if(!quiet) log_msg("CLIENT","request: need %d night(s) (req=%d)", nights, idx);
  Event e = { .type=EV_REQUEST, .req_index=idx, .room=-1, .pid=getpid() };
  ev_push(S, &e);

  // ждём назначения/отмены
  while(sem_wait(mysem)==-1 && errno==EINTR){}
  if(S->req[idx].assigned < 0){
    if(!quiet) log_msg("CLIENT","exit: request canceled (shutdown)");
    clear_req(S, idx); sem_close(mysem); sem_unlink(mysem_name); return 0;
  }

  int room = S->req[idx].room;
  if(!quiet) log_msg("CLIENT","got room #%d, staying %d night(s) (req=%d)", room, nights, idx);

  // проживание
  usleep((useconds_t)((uint64_t)nights * (uint64_t)day_ms * 1000ULL));

  // выселение
  if(!quiet) log_msg("CLIENT","checkout room #%d", room);
  Event e2 = { .type=EV_CHECKOUT, .req_index=idx, .room=room, .pid=getpid() };
  ev_push(S, &e2);

  clear_req(S, idx);
  sem_close(mysem); sem_unlink(mysem_name);
  return 0;
}
