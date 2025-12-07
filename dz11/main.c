#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <stdbool.h>
#include <stdarg.h>

#define N_PRODUCERS 100
#define BUF_CAP 256 

//логгер 
static pthread_mutex_t log_mx = PTHREAD_MUTEX_INITIALIZER;

static inline long long now_ms(void){
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts); 
    return (long long)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

static void tslog(const char* tag, const char* fmt, ...) {
    char line[512];
    long long ms = now_ms();
    int hh = (ms/3600000)%24, mm=(ms/60000)%60, ss=(ms/1000)%60, mmm=ms%1000;

    size_t off = (size_t)snprintf(line, sizeof line, "%02d:%02d:%02d.%03d | %s | ",hh, mm, ss, mmm, tag);

    va_list ap;
    va_start(ap, fmt);
    off += (size_t)vsnprintf(line + off, sizeof line - off, fmt, ap);
    va_end(ap);

    if (off < sizeof line - 1) line[off++] = '\n';
    line[off] = '\0';

    pthread_mutex_lock(&log_mx);
    fputs(line, stdout);
    fflush(stdout);
    pthread_mutex_unlock(&log_mx);
}

static int rnd_incl(int a, int b){ return a + rand() % (b - a + 1); }

// кольцевой буфер целых
typedef struct {
    int data[BUF_CAP];
    int head, tail, count;
    pthread_mutex_t m;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} ringbuf;

static void rb_init(ringbuf* rb){
    rb->head = rb->tail = rb->count = 0;
    pthread_mutex_init(&rb->m, NULL);
    pthread_cond_init(&rb->not_empty, NULL);
    pthread_cond_init(&rb->not_full, NULL);
}
static void rb_push(ringbuf* rb, int x){
    pthread_mutex_lock(&rb->m);
    while (rb->count == BUF_CAP) pthread_cond_wait(&rb->not_full, &rb->m);
    rb->data[rb->tail] = x;
    rb->tail = (rb->tail + 1) % BUF_CAP;
    rb->count++;
    pthread_cond_signal(&rb->not_empty);
    pthread_mutex_unlock(&rb->m);
}
// Вариант pop под уже захваченным rb->m:
static int rb_pop_locked(ringbuf* rb){
    int x = rb->data[rb->head];
    rb->head = (rb->head + 1) % BUF_CAP;
    rb->count--;
    pthread_cond_signal(&rb->not_full);
    return x;
}

// глобальное состояние
static ringbuf Q;  // общий буфер
static int producers_left = N_PRODUCERS;
static int active_adders = 0;
static long long produced_sum = 0;  // для самопроверки
static int next_sum_id = 1;
static int final_result = 0;
static bool got_final = false;

// сумматор
typedef struct { int a, b, id; } add_args;

static void* adder_thread(void* vp){
    add_args* args = (add_args*)vp;
    int id = args->id, a = args->a, b = args->b;
    int delay = rnd_incl(3,6);

    sleep(delay);
    int s = a + b;

    //кладем сумму без захваченного Q.m- rb_push сам лочит
    rb_push(&Q, s);

    //корректируем счётчики и вызываем диспетчер
    pthread_mutex_lock(&Q.m);
    active_adders--;
    int sz = Q.count;
    pthread_cond_broadcast(&Q.not_empty); // зовем на случай финала
    pthread_mutex_unlock(&Q.m);

    tslog("СУММАТОР", "S%03d: %d+%d=%d - в буфер (размер=%d)", id, a, b, s, sz);

    free(args);
    return NULL;
}

//источник (порождает одно число) 
static void* producer_thread(void* vp){
    long id = (long)vp;
    int t = rnd_incl(1,7);
    sleep(t);
    int val = rnd_incl(1,100);

    // учтем в контрольной сумме 
    pthread_mutex_lock(&Q.m);
    produced_sum += val;
    pthread_mutex_unlock(&Q.m);

    // кладем значение
    rb_push(&Q, val);

    // уменьшаем счетчик производителей и вызываем диспетчер
    pthread_mutex_lock(&Q.m);
    int sz = Q.count;
    producers_left--;
    pthread_cond_broadcast(&Q.not_empty);
    pthread_mutex_unlock(&Q.m);

    tslog("ИСТОЧНИК", "P%03ld: %d - буфер (размер=%d). Осталось=%d",
         id, val, sz, producers_left);
    return NULL;
}

// диспетчер
static void* dispatcher_thread(void* _){
    for(;;){
        pthread_mutex_lock(&Q.m);
        while (Q.count < 2) {
            // Условие завершения
            if (producers_left==0 && active_adders==0) {
                if (Q.count==1) {
                    final_result = Q.data[Q.head];
                    got_final = true;
                    pthread_mutex_unlock(&Q.m);
                    tslog("ГОТОВО", "Результат = %d", final_result);
                    return NULL;
                } else if (Q.count==0) {
                    pthread_mutex_unlock(&Q.m);
                    tslog("ОШИБКА", "Очередь пуста при завершении (неожиданно)");
                    return NULL;
                }
            }
            pthread_cond_wait(&Q.not_empty, &Q.m);
        }
        // есть как минимум два числ-забираем пару и запускаем сумматор
        int a = rb_pop_locked(&Q);
        int b = rb_pop_locked(&Q);
        int sz = Q.count;
        int id = next_sum_id++;
        active_adders++;
        pthread_mutex_unlock(&Q.m);

        tslog("ДИСПЕТЧЕР", "берем пару (%d,%d), запускаем S%03d (размер буфера=%d)", a, b, id, sz);

        add_args* args = (add_args*)malloc(sizeof(add_args));
        args->a=a; args->b=b; args->id=id;
        pthread_t th; pthread_create(&th, NULL, adder_thread, args);
        pthread_detach(th); // сумматоры отделяем
    }
    return NULL;
}

int main(void){
    srand((unsigned)time(NULL));
    setvbuf(stdout, NULL, _IOLBF, 0); // построчное буферизование
    rb_init(&Q);

    pthread_t disp;
    pthread_create(&disp, NULL, dispatcher_thread, NULL);

    pthread_t prod[N_PRODUCERS];
    for (long i=0;i<N_PRODUCERS;i++)
        pthread_create(&prod[i], NULL, producer_thread, (void*)i);

    for (int i=0;i<N_PRODUCERS;i++) pthread_join(prod[i], NULL);
    pthread_join(disp, NULL); // дождемся результата

    // самопроверка
    tslog("ПРОВЕРКА", "Ожидаемая сумма = %lld; Результат = %d", produced_sum, final_result);
    return 0;
}
