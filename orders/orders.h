#ifndef ORDERS_H
#define ORDERS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <time.h>

#define ORDER_STATUS_PENDING  "pending"
#define ORDER_STATUS_PAID     "paid"
#define ORDER_STATUS_REJECTED "rejected"

typedef struct {
    char *sku;
    int qty;
    int64_t price_cents;
} Item;

typedef struct {
    char id[64];
    char *user_id;
    int64_t amount_cents;
    int attempts;
    Item *items;
    size_t items_len;
} Order;

typedef struct {
    int worker_threads;
    int payment_max_inflight;
    int payment_timeout_ms;
    int inventory_timeout_ms;
    int breaker_base_ms;
    int breaker_max_ms;
    int backoff_base_ms;
    int backoff_max_ms;
    int lease_seconds;
} WorkerConfig;

/* logging ------------------------------------------------------------------ */
static inline void log_msg(const char *level, const char *fmt, ...)
{
    char msg[2048];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    struct timespec ts;
    struct tm tm;
    clock_gettime(CLOCK_REALTIME, &ts);
    gmtime_r(&ts.tv_sec, &tm);
    fprintf(stderr, "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ %-5s %s\n",
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000,
            level, msg);
}

#define log_info(...)  log_msg("INFO", __VA_ARGS__)
#define log_warn(...)  log_msg("WARN", __VA_ARGS__)
#define log_error(...) log_msg("ERROR", __VA_ARGS__)

/* db ----------------------------------------------------------------------- */
typedef struct db_pool db_pool;

db_pool *db_pool_create(const char *url, int size);
void db_pool_destroy(db_pool *p);
int db_init_schema(db_pool *p);
void db_reset_leases(db_pool *p);

int db_insert_order(db_pool *p, const char *id, const char *user_id,
                    int64_t amount_cents, const char *items_json);
/* 1 — найден, 0 — нет, -1 — ошибка */
int db_get_order(db_pool *p, const char *id, char *status, size_t status_sz,
                 int64_t *amount_cents);

/* 1 — заказ захвачен, 0 — нет готовых, -1 — ошибка */
int db_claim_order(db_pool *p, Order *out);
int db_set_terminal(db_pool *p, const char *id, const char *status);
int db_retry_later(db_pool *p, const char *id, int delay_ms, const char *err);

void order_free(Order *o);

/* http client -------------------------------------------------------------- */
#include <curl/curl.h>

typedef struct {
    long status;
    char *body;
    size_t len;
    CURLcode curl_code;
} HttpResp;

void http_resp_free(HttpResp *r);
CURLcode http_request(const char *method, const char *url, const char *body,
                      const char *idem_key, long timeout_ms, HttpResp *out);

/* worker ------------------------------------------------------------------- */
typedef struct worker worker;

worker *worker_start(db_pool *db, const char *payment_url,
                     const char *inventory_url, const WorkerConfig *cfg);
void worker_stop(worker *w);

#endif
