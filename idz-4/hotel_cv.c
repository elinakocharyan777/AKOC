#define _XOPEN_SOURCE 700
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>

#define MAX_CLIENTS 1000

// параметры
typedef struct {
    int rooms;
    int clients;
    int arrive_min_ms;
    int arrive_max_ms;
    int nights_min;
    int nights_max;
    int day_ms;
    unsigned seed;
    const char* out_path;
    const char* cfg_path;
} Params;

static void params_set_defaults(Params* p){
    p->rooms = 10;
    p->clients = 10;
    p->arrive_min_ms = 0;  p->arrive_max_ms = 600;
    p->nights_min = 1;     p->nights_max   = 4;
    p->day_ms = 300;
    p->seed = (unsigned)time(NULL);
    p->out_path = NULL;
    p->cfg_path = NULL;
}

// логгер
static pthread_mutex_t log_mx = PTHREAD_MUTEX_INITIALIZER;
static FILE* log_file = NULL;

static inline long long now_ms(void){
    struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
    return (long long)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}
static void tslog(const char* tag, const char* fmt, ...){
    char line[512]; long long ms=now_ms();
    int hh=(ms/3600000)%24, mm=(ms/60000)%60, ss=(ms/1000)%60, mmm=ms%1000;
    size_t off=(size_t)snprintf(line,sizeof line,"%02d:%02d:%02d.%03d | %s | ",
                                hh,mm,ss,mmm,tag);
    va_list ap; va_start(ap,fmt);
    off += (size_t)vsnprintf(line+off, sizeof line - off, fmt, ap);
    va_end(ap);
    if (off < sizeof line - 1) {
        line[off++] = '\n';
    }
    line[off] = '\0';

    pthread_mutex_lock(&log_mx);
    fputs(line, stdout); fflush(stdout);
    if (log_file){ fputs(line, log_file); fflush(log_file); }
    pthread_mutex_unlock(&log_mx);
}

static void sleep_ms(int ms){
    struct timespec ts = { .tv_sec = ms/1000, .tv_nsec = (long)(ms%1000)*1000000L };
    while (nanosleep(&ts,&ts)==-1 && errno==EINTR) { /* retry */ }
}
static int rnd_incl(int a,int b){ return a + rand() % (b - a + 1); }

//модель 
typedef struct { int client_id, nights_left; } Room;
static Room* rooms = NULL;
static int   rooms_cap = 0;

typedef struct {
    int id, nights, arrival_ms, room_no;
    // условные переменные клиента
    pthread_mutex_t mx;
    pthread_cond_t  cv_checkin;
    pthread_cond_t  cv_checkout;
    bool checkin_ready;
    bool checkout_ready;
} Client;

static Client* clients = NULL;
static int total_clients = 0;

typedef struct { int a[MAX_CLIENTS], head, tail, count; pthread_mutex_t mx; } WaitQ;
static void q_init(WaitQ* q){ q->head=q->tail=q->count=0; pthread_mutex_init(&q->mx,NULL); }
static void q_push(WaitQ* q,int id){ pthread_mutex_lock(&q->mx); q->a[q->tail]=id; q->tail=(q->tail+1)%MAX_CLIENTS; q->count++; pthread_mutex_unlock(&q->mx); }
static int  q_pop (WaitQ* q,int* ok){ pthread_mutex_lock(&q->mx); if(q->count==0){*ok=0; pthread_mutex_unlock(&q->mx); return -1;} int id=q->a[q->head]; q->head=(q->head+1)%MAX_CLIENTS; q->count--; *ok=1; pthread_mutex_unlock(&q->mx); return id; }
static int  q_count(WaitQ* q){ pthread_mutex_lock(&q->mx); int c=q->count; pthread_mutex_unlock(&q->mx); return c; }
static WaitQ waitq;

// синхронизация гостиницы
static pthread_mutex_t hotel_mx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  cv_admin = PTHREAD_COND_INITIALIZER; // сигнал пришли/очередь/освободилось/закрываемся
static pthread_barrier_t start_barrier; // стартовое выравнивание

static atomic_int  clients_done = 0;
static atomic_bool closing = false;

static int free_rooms = 0;

// парсинг
static void rtrim(char* s){
    size_t n=strlen(s);
    while(n>0 && (s[n-1]=='\r'||s[n-1]==' '||s[n-1]=='\t')) s[--n]='\0';
}
static int parse_int(const char* s, int* out){ char* e; long v=strtol(s,&e,10); if(!*s||*e) return 0; *out=(int)v; return 1; }
static int parse_range(const char* s, int* a, int* b){
    const char* sep = strchr(s, ':'); if(!sep) sep = strstr(s, ".."); if(!sep) return 0;
    char L[64], R[64]; size_t ln=(size_t)(sep-s); if(ln>=sizeof L) return 0;
    memcpy(L,s,ln); L[ln]='\0'; strcpy(R, sep[1]==':'? sep+2 : sep+1);
    char *e1,*e2; long x=strtol(L,&e1,10), y=strtol(R,&e2,10);
    if(*e1||*e2||L[0]=='\0'||R[0]=='\0') return 0;
    *a=(int)x; *b=(int)y; return 1;
}
static void usage(const char* prog){
    fprintf(stderr,"Usage: %s [--clients N] [--rooms R] [--arrive MIN:MAX] [--nights MIN:MAX]\n"
                   "          [--day-ms MS] [--seed SEED] [--out FILE] | [--config FILE]\n", prog);
}
static int parse_cli(int argc, char** argv, Params* P){
    for(int i=1;i<argc;i++){
        const char* a=argv[i];
        if(!strcmp(a,"-h")||!strcmp(a,"--help")){ usage(argv[0]); return 0; }
        else if(!strcmp(a,"--clients")&&i+1<argc){ parse_int(argv[++i],&P->clients); }
        else if(!strcmp(a,"--rooms")  &&i+1<argc){ parse_int(argv[++i],&P->rooms); }
        else if(!strcmp(a,"--day-ms") &&i+1<argc){ parse_int(argv[++i],&P->day_ms); }
        else if(!strcmp(a,"--seed")   &&i+1<argc){ int v; if(parse_int(argv[++i],&v)) P->seed=(unsigned)v; }
        else if(!strcmp(a,"--arrive") &&i+1<argc){ parse_range(argv[++i],&P->arrive_min_ms,&P->arrive_max_ms); }
        else if(!strcmp(a,"--nights") &&i+1<argc){ parse_range(argv[++i],&P->nights_min,&P->nights_max); }
        else if(!strcmp(a,"--out")    &&i+1<argc){ P->out_path=argv[++i]; }
        else if(!strcmp(a,"--config") &&i+1<argc){ P->cfg_path=argv[++i]; }
        else { fprintf(stderr,"Unknown arg: %s\n", a); usage(argv[0]); return 0; }
    }
    return 1;
}
static int load_config(const char* path, Params* P){
    FILE* f=fopen(path,"r"); if(!f){ perror("config"); return 0; }
    char line[256];
    while(fgets(line,sizeof line,f)){
        if(line[0]=='#'||line[0]=='\n') continue;
        char* nl=strchr(line,'\n'); if(nl) *nl='\0';
        char* eq=strchr(line,'='); if(!eq) continue; *eq='\0';
        char* key=line; char* val=eq+1;
        while(*key==' '||*key=='\t') key++; 
        while(*val==' '||*val=='\t') val++;
        rtrim(key); rtrim(val);
        if(!strcmp(key,"clients"))  parse_int(val,&P->clients);
        else if(!strcmp(key,"rooms")) parse_int(val,&P->rooms);
        else if(!strcmp(key,"arrive_min_ms"))parse_int(val,&P->arrive_min_ms);
        else if(!strcmp(key,"arrive_max_ms"))parse_int(val,&P->arrive_max_ms);
        else if(!strcmp(key,"nights_min"))  parse_int(val,&P->nights_min);
        else if(!strcmp(key,"nights_max"))   parse_int(val,&P->nights_max);
        else if(!strcmp(key,"day_ms")) parse_int(val,&P->day_ms);
        else if(!strcmp(key,"seed")){ int tmp; if(parse_int(val,&tmp)) P->seed=(unsigned)tmp; }
        else if(!strcmp(key,"out")) { free((void*)P->out_path); P->out_path=strdup(val); }
        else if(!strcmp(key,"arrive")) { int a,b; if(parse_range(val,&a,&b)){ P->arrive_min_ms=a; P->arrive_max_ms=b; } }
        else if(!strcmp(key,"nights"))  { int a,b; if(parse_range(val,&a,&b)){ P->nights_min=a; P->nights_max=b; } }
    }
    fclose(f); return 1;
}

// сигнал потоком
static void* sig_thread(void* _){
    (void)_; 
    sigset_t set; sigemptyset(&set); sigaddset(&set, SIGINT);
    int sig; (void)sigwait(&set,&sig);
    atomic_store(&closing, true);
    tslog("СИГНАЛ","получен SIGINT — закрываемся");
    pthread_mutex_lock(&hotel_mx);
    pthread_cond_broadcast(&cv_admin); // будим администратора
    pthread_mutex_unlock(&hotel_mx);
    return NULL;
}

//клиент
static void* client_thread(void* vp){
    Client* c=(Client*)vp;

    // стартовая синхронизация
    pthread_barrier_wait(&start_barrier);

    sleep_ms(c->arrival_ms);
    tslog("КЛИЕНТ","C%03d: пришёл, хочет %d ноч.", c->id, c->nights);

    pthread_mutex_lock(&hotel_mx);
    if (atomic_load(&closing)){
        pthread_mutex_unlock(&hotel_mx);
        tslog("КЛИЕНТ","C%03d: гостиница закрыта, уходит", c->id);
        atomic_fetch_add(&clients_done, 1);
        return NULL;
    }
    if (free_rooms > 0){
        int ri=-1; for(int i=0;i<rooms_cap;i++) if(rooms[i].client_id<0){ ri=i; break; }
        rooms[ri].client_id=c->id; rooms[ri].nights_left=c->nights; free_rooms--; c->room_no=ri;
        pthread_mutex_unlock(&hotel_mx);

        tslog("АДМИН","заселён C%03d -> номер #%02d на %d ноч.", c->id, ri, c->nights);
        tslog("КЛИЕНТ","C%03d: заселён в номер #%02d", c->id, ri);

        pthread_mutex_lock(&c->mx);
        while(!c->checkout_ready) pthread_cond_wait(&c->cv_checkout, &c->mx);
        pthread_mutex_unlock(&c->mx);

        tslog("КЛИЕНТ","C%03d: выезд из номера #%02d", c->id, c->room_no);
        atomic_fetch_add(&clients_done, 1);
        return NULL;
    }
    // очереди
    q_push(&waitq, c->id);
    int qlen = q_count(&waitq);
    pthread_cond_signal(&cv_admin);
    pthread_mutex_unlock(&hotel_mx);

    tslog("КЛИЕНТ","C%03d: ожидает на скамейке (в очереди=%d)", c->id, qlen);

    // ждём решения на заселение/отказ
    pthread_mutex_lock(&c->mx);
    while(!c->checkin_ready) pthread_cond_wait(&c->cv_checkin, &c->mx);
    int rn = c->room_no;
    pthread_mutex_unlock(&c->mx);

    if (rn < 0){
        tslog("КЛИЕНТ","C%03d: отказ (закрытие), уходит", c->id);
        atomic_fetch_add(&clients_done, 1);
        return NULL;
    }
    tslog("КЛИЕНТ","C%03d: заселён в номер #%02d", c->id, rn);

    pthread_mutex_lock(&c->mx);
    while(!c->checkout_ready) pthread_cond_wait(&c->cv_checkout, &c->mx);
    pthread_mutex_unlock(&c->mx);

    tslog("КЛИЕНТ","C%03d: выезд из номера #%02d", c->id, rn);
    atomic_fetch_add(&clients_done, 1);
    return NULL;
}

// администратор
static void* admin_thread(void* vp){
    Params* P=(Params*)vp;

    // стартовая синхронизация
    pthread_barrier_wait(&start_barrier);
    tslog("АДМИН","старт: номеров=%d, длительность суток=%d мс", P->rooms, P->day_ms);

    for(;;){
        // ждём либо событие, либо конца суток
        struct timespec abs; clock_gettime(CLOCK_REALTIME,&abs);
        long ns=(long)(P->day_ms%1000)*1000000L;
        abs.tv_sec += P->day_ms/1000; abs.tv_nsec += ns;
        if(abs.tv_nsec>=1000000000L){ abs.tv_sec++; abs.tv_nsec-=1000000000L; }

        pthread_mutex_lock(&hotel_mx);
        int rc = pthread_cond_timedwait(&cv_admin, &hotel_mx, &abs);

        if (atomic_load(&closing)){
            // отказать всем ожидающим
            while (q_count(&waitq)>0){
                int ok=0, cid=q_pop(&waitq,&ok); if(!ok) break;
                Client* c=&clients[cid];
                pthread_mutex_lock(&c->mx);
                c->room_no = -1; c->checkin_ready = true;
                pthread_cond_signal(&c->cv_checkin);
                pthread_mutex_unlock(&c->mx);
                tslog("АДМИН","закрытие: отказ клиенту C%03d", cid);
            }
            // экстренно выселить всех
            for(int i=0;i<P->rooms;i++) if(rooms[i].client_id>=0){
                int cid=rooms[i].client_id; rooms[i].client_id=-1; rooms[i].nights_left=0; free_rooms++;
                Client* c=&clients[cid];
                pthread_mutex_lock(&c->mx);
                c->checkout_ready = true;
                pthread_cond_signal(&c->cv_checkout);
                pthread_mutex_unlock(&c->mx);
                tslog("АДМИН","закрытие: экстренный выезд C%03d из #%02d", cid, i);
            }
        } else if (rc==ETIMEDOUT){
            // день прошёл
            for(int i=0;i<P->rooms;i++) if(rooms[i].client_id>=0){
                rooms[i].nights_left--;
                if(rooms[i].nights_left<=0){
                    int cid=rooms[i].client_id; rooms[i].client_id=-1; rooms[i].nights_left=0; free_rooms++;
                    Client* c=&clients[cid];
                    pthread_mutex_lock(&c->mx);
                    c->checkout_ready = true;
                    pthread_cond_signal(&c->cv_checkout);
                    pthread_mutex_unlock(&c->mx);
                    tslog("АДМИН","день прошёл: освобождён номер %02d/%03d", i, cid); 
                }
            }
        }

        // заселяем из очереди
        while(!atomic_load(&closing) && free_rooms>0 && q_count(&waitq)>0){
            int ok=0, cid=q_pop(&waitq,&ok); if(!ok) break;
            int ri=-1; for(int i=0;i<P->rooms;i++) if(rooms[i].client_id<0){ ri=i; break; }
            if(ri<0){ // если вдруг нет то вернём и выйдем
                q_push(&waitq,cid);
                break;
            }
            rooms[ri].client_id=cid; rooms[ri].nights_left=clients[cid].nights; free_rooms--;
            Client* c=&clients[cid];
            pthread_mutex_lock(&c->mx);
            c->room_no = ri; c->checkin_ready = true;
            pthread_cond_signal(&c->cv_checkin);
            pthread_mutex_unlock(&c->mx);
            tslog("АДМИН","заселён C%03d -> номер #%02d на %d ноч.", cid, ri, clients[cid].nights);
        }

        int done = atomic_load(&clients_done);
        int waiting = q_count(&waitq);
        int occupied = P->rooms - free_rooms;

        if ( (done==total_clients) || (atomic_load(&closing) && waiting==0 && occupied==0) ){
            pthread_mutex_unlock(&hotel_mx);
            tslog("ГОТОВО","моделирование завершено: всего клиентов=%d, завершили=%d", total_clients, done);
            return NULL;
        }
        pthread_mutex_unlock(&hotel_mx);
    }
}

int main(int argc, char** argv){
    setvbuf(stdout,NULL,_IOLBF,0);

    // блокируем SIGINT, будем ловить через sigwait
    sigset_t set; sigemptyset(&set); sigaddset(&set, SIGINT);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    Params P; params_set_defaults(&P);

    // сначала найдём конфиг (если есть)
    for(int i=1;i+1<argc;i++) if(!strcmp(argv[i],"--config")) { P.cfg_path=argv[i+1]; break; }
    if(P.cfg_path){ if(!load_config(P.cfg_path, &P)) return 1; tslog("КОНФИГ","загружен: %s", P.cfg_path); }

    // потом CLI перекрывает 
    if(!parse_cli(argc, argv, &P)) return 1;

    // валидация
    if(P.clients<1) P.clients=1; 
    if(P.clients>MAX_CLIENTS) P.clients=MAX_CLIENTS;
    if(P.rooms<1)   P.rooms=1;
    if(P.arrive_min_ms<0) P.arrive_min_ms=0;
    if(P.arrive_max_ms<P.arrive_min_ms) P.arrive_max_ms=P.arrive_min_ms;
    if(P.nights_min<1) P.nights_min=1;
    if(P.nights_max<P.nights_min) P.nights_max=P.nights_min;
    if(P.day_ms<10) P.day_ms=10;

    if(P.out_path){ log_file=fopen(P.out_path,"w"); if(!log_file){ perror("open out"); return 1; } }

    srand(P.seed);

    rooms_cap = P.rooms;
    rooms   = calloc((size_t)P.rooms,   sizeof(Room));
    clients = calloc((size_t)P.clients, sizeof(Client));
    if(!rooms||!clients){ perror("calloc"); return 1; }

    for(int i=0;i<P.rooms;i++){ rooms[i].client_id=-1; rooms[i].nights_left=0; }
    free_rooms = P.rooms;
    total_clients = P.clients;

    for(int i=0;i<P.clients;i++){
        clients[i].id = i;
        clients[i].arrival_ms = rnd_incl(P.arrive_min_ms, P.arrive_max_ms);
        clients[i].nights     = rnd_incl(P.nights_min,    P.nights_max);
        clients[i].room_no    = -2;
        pthread_mutex_init(&clients[i].mx, NULL);
        pthread_cond_init (&clients[i].cv_checkin, NULL);
        pthread_cond_init (&clients[i].cv_checkout, NULL);
        clients[i].checkin_ready=false;
        clients[i].checkout_ready=false;
    }

    q_init(&waitq);

    tslog("ИНФО","rooms=%d, clients=%d, arrive=%d..%d ms, nights=%d..%d, day=%d ms, seed=%u%s%s",
         P.rooms,P.clients,P.arrive_min_ms,P.arrive_max_ms,P.nights_min,P.nights_max,P.day_ms,P.seed,
         P.out_path?" | out=":"", P.out_path?P.out_path:"");

    // барьер: админ + все клиенты
    pthread_barrier_init(&start_barrier, NULL, (unsigned)(P.clients + 1));

    // поток ловли сигнала
    pthread_t sigth; pthread_create(&sigth,NULL,sig_thread,NULL);

    // админ
    pthread_t admin; pthread_create(&admin,NULL,admin_thread,&P);
    // клиенты
    pthread_t* th = calloc((size_t)P.clients, sizeof(pthread_t));
    for(int i=0;i<P.clients;i++) pthread_create(&th[i],NULL,client_thread,&clients[i]);

    for(int i=0;i<P.clients;i++) pthread_join(th[i],NULL);
    pthread_join(admin,NULL);

    // завершаем сигнальный поток 
    pthread_cancel(sigth); pthread_detach(sigth);

    int final_waiters=q_count(&waitq); int occupied=0; for(int i=0;i<P.rooms;i++) if(rooms[i].client_id>=0) occupied++;
    tslog("ОТЧЁТ","итог: завершили=%d/%d, в очереди=%d, занято номеров=%d",
         atomic_load(&clients_done), total_clients, final_waiters, occupied);

    // освобождение
    for(int i=0;i<P.clients;i++){
        pthread_cond_destroy(&clients[i].cv_checkin);
        pthread_cond_destroy(&clients[i].cv_checkout);
        pthread_mutex_destroy(&clients[i].mx);
    }
    pthread_barrier_destroy(&start_barrier);
    pthread_mutex_destroy(&hotel_mx);
    pthread_cond_destroy(&cv_admin);
    free(th); free(clients); free(rooms);
    if(log_file) fclose(log_file);
    return 0;
}
