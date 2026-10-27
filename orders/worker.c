#define _GNU_SOURCE
#include "orders.h"

#include <cjson/cJSON.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum {
    BR_CLOSED = 0,
    BR_OPEN = 1,
    BR_HALF = 2,
};

enum {
    FAIL_OK = 0,
    FAIL_TRANSPORT = 1,
    FAIL_SERVER = 2,
};

enum {
    PAY_OK = 0,
    PAY_DECLINED = 1,
    PAY_RETRY = 2,
};

typedef struct {
    pthread_mutex_t mu;
    int state;
    int inflight;
    int max_inflight;
    int transport_failures;
    int server_failures;
    int transport_threshold;
    int server_threshold;
    int base_ms;
    int max_ms;
    int open_ms;
    int64_t open_until;
    int probe_inflight;
} breaker;

struct worker {
    db_pool *db;
    const char *payment_url;
    const char *inventory_url;
    const char *events_url;
    WorkerConfig cfg;
    breaker br;
    breaker bus_br;
    pthread_t *tids;
    int nthreads;
    pthread_t *event_tids;
    int nevent_threads;
    volatile int stop;
};

static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void sleep_ms(int ms)
{
    if (ms <= 0) {
        return;
    }
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

/* breaker ------------------------------------------------------------------ */

static void breaker_init(breaker *b, int max_inflight, int base_ms, int max_ms,
                         int transport_thr, int server_thr)
{
    memset(b, 0, sizeof(*b));
    pthread_mutex_init(&b->mu, NULL);
    b->state = BR_CLOSED;
    b->max_inflight = max_inflight > 0 ? max_inflight : 1;
    b->base_ms = base_ms > 0 ? base_ms : 500;
    b->max_ms = max_ms >= b->base_ms ? max_ms : b->base_ms;
    b->open_ms = b->base_ms;
    b->transport_threshold = transport_thr > 0 ? transport_thr : 2;
    b->server_threshold = server_thr > 0 ? server_thr : 20;
}

static int breaker_cooldown_ms(breaker *b)
{
    int r = 0;
    pthread_mutex_lock(&b->mu);
    if (b->state == BR_OPEN) {
        int64_t rem = b->open_until - now_ms();
        if (rem > 0) {
            r = (int)rem;
        }
    } else if (b->state == BR_HALF && b->probe_inflight) {
        r = 50;
    }
    pthread_mutex_unlock(&b->mu);
    return r;
}

/* 1 — запрос можно отправлять, 0 — сейчас нельзя */
static int breaker_try_begin(breaker *b, int *is_probe)
{
    int allow = 0;
    *is_probe = 0;
    pthread_mutex_lock(&b->mu);
    if (b->state == BR_OPEN) {
        if (now_ms() >= b->open_until) {
            b->state = BR_HALF;
            b->probe_inflight = 0;
        }
    }
    if (b->state == BR_HALF) {
        if (!b->probe_inflight && b->inflight < b->max_inflight) {
            b->probe_inflight = 1;
            b->inflight++;
            *is_probe = 1;
            allow = 1;
        }
    } else if (b->state == BR_CLOSED) {
        if (b->inflight < b->max_inflight) {
            b->inflight++;
            allow = 1;
        }
    }
    pthread_mutex_unlock(&b->mu);
    return allow;
}

static void breaker_end(breaker *b, int is_probe, int fail_kind)
{
    pthread_mutex_lock(&b->mu);
    if (b->inflight > 0) {
        b->inflight--;
    }
    if (is_probe) {
        b->probe_inflight = 0;
        if (b->state == BR_HALF) {
            if (fail_kind == FAIL_OK) {
                b->state = BR_CLOSED;
                b->transport_failures = 0;
                b->server_failures = 0;
                b->open_ms = b->base_ms;
            } else {
                b->state = BR_OPEN;
                b->open_until = now_ms() + b->open_ms;
                if (b->open_ms < b->max_ms) {
                    b->open_ms *= 2;
                    if (b->open_ms > b->max_ms) {
                        b->open_ms = b->max_ms;
                    }
                }
            }
        }
    } else if (b->state == BR_CLOSED) {
        if (fail_kind == FAIL_OK) {
            b->transport_failures = 0;
            b->server_failures = 0;
        } else if (fail_kind == FAIL_TRANSPORT) {
            b->transport_failures++;
            b->server_failures = 0;
            if (b->transport_failures >= b->transport_threshold) {
                b->state = BR_OPEN;
                b->open_until = now_ms() + b->open_ms;
                if (b->open_ms < b->max_ms) {
                    b->open_ms *= 2;
                    if (b->open_ms > b->max_ms) {
                        b->open_ms = b->max_ms;
                    }
                }
            }
        } else if (fail_kind == FAIL_SERVER) {
            b->server_failures++;
            b->transport_failures = 0;
            /* Отдельные 5xx — обычные ретраи; цепь рвём только на потоке
             * ошибок, чтобы не заваливать действительно упавший сервис. */
            if (b->server_failures >= b->server_threshold) {
                b->state = BR_OPEN;
                b->open_until = now_ms() + b->open_ms;
                if (b->open_ms < b->max_ms) {
                    b->open_ms *= 2;
                    if (b->open_ms > b->max_ms) {
                        b->open_ms = b->max_ms;
                    }
                }
            }
        }
    }
    pthread_mutex_unlock(&b->mu);
}

/* backoff ------------------------------------------------------------------ */

static int retry_delay_ms(const worker *w, int attempts)
{
    int d = w->cfg.backoff_base_ms > 0 ? w->cfg.backoff_base_ms : 200;
    int max = w->cfg.backoff_max_ms > 0 ? w->cfg.backoff_max_ms : 4000;
    for (int i = 0; i < attempts && d < max; i++) {
        if (d > max / 2) {
            d = max;
            break;
        }
        d *= 2;
    }
    if (d > max) {
        d = max;
    }
    int half = d / 2;
    int jitter = half > 0 ? (int)(now_ms() % (unsigned)half) : 0;
    return half + jitter;
}

/* inventory ---------------------------------------------------------------- */

static int reserve(worker *w, Order *o)
{
    cJSON *root = cJSON_CreateObject();
    if (root == NULL) {
        return -1;
    }
    cJSON_AddStringToObject(root, "order_id", o->id);
    cJSON *items = cJSON_AddArrayToObject(root, "items");
    for (size_t i = 0; i < o->items_len; i++) {
        cJSON *it = cJSON_CreateObject();
        if (it == NULL) {
            cJSON_Delete(root);
            return -1;
        }
        cJSON_AddStringToObject(it, "sku", o->items[i].sku);
        cJSON_AddNumberToObject(it, "qty", o->items[i].qty);
        cJSON_AddItemToArray(items, it);
    }
    char *body = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (body == NULL) {
        return -1;
    }

    char url[2048];
    snprintf(url, sizeof(url), "%s/reservations", w->inventory_url);
    HttpResp r;
    CURLcode rc = http_request("POST", url, body, NULL,
                               w->cfg.inventory_timeout_ms, &r);
    free(body);

    int ok = (rc == CURLE_OK) && (r.status == 200 || r.status == 201);
    if (!ok) {
        log_warn("inventory reserve %s: curl=%d status=%ld", o->id, (int)rc,
                 r.status);
    }
    http_resp_free(&r);
    return ok ? 0 : -1;
}

static int release(worker *w, Order *o)
{
    char url[2048];
    snprintf(url, sizeof(url), "%s/reservations/%s", w->inventory_url, o->id);
    HttpResp r;
    CURLcode rc = http_request("DELETE", url, NULL, NULL,
                               w->cfg.inventory_timeout_ms, &r);
    int ok = (rc == CURLE_OK) && (r.status / 100 == 2);
    if (!ok) {
        log_warn("inventory release %s: curl=%d status=%ld", o->id, (int)rc,
                 r.status);
    }
    http_resp_free(&r);
    return ok ? 0 : -1;
}

/* payment ------------------------------------------------------------------ */

static int payment_has_charge(worker *w, const char *order_id, int *has)
{
    char url[2048];
    snprintf(url, sizeof(url), "%s/payments?order_id=%s", w->payment_url,
             order_id);
    HttpResp r;
    CURLcode rc = http_request("GET", url, NULL, NULL,
                               w->cfg.payment_timeout_ms, &r);
    if (rc != CURLE_OK || r.status != 200) {
        http_resp_free(&r);
        return -1;
    }
    cJSON *arr = cJSON_Parse(r.body != NULL ? r.body : "");
    int ok = cJSON_IsArray(arr);
    if (ok) {
        *has = cJSON_GetArraySize(arr) > 0;
    }
    cJSON_Delete(arr);
    http_resp_free(&r);
    return ok ? 0 : -1;
}

static int charge(worker *w, Order *o, int *fail_kind, char *err, size_t err_sz)
{
    char url[2048];
    snprintf(url, sizeof(url), "%s/payments", w->payment_url);

    char body[256];
    snprintf(body, sizeof(body),
             "{\"order_id\":\"%s\",\"amount_cents\":%lld,\"currency\":\"RUB\"}",
             o->id, (long long)o->amount_cents);

    HttpResp r;
    CURLcode rc = http_request("POST", url, body, o->id,
                               w->cfg.payment_timeout_ms, &r);
    if (rc == CURLE_OK) {
        long st = r.status;
        http_resp_free(&r);
        if (st == 200 || st == 201) {
            *fail_kind = FAIL_OK;
            return PAY_OK;
        }
        if (st == 402) {
            *fail_kind = FAIL_OK;
            return PAY_DECLINED;
        }
        snprintf(err, err_sz, "payment_status_%ld", st);
        *fail_kind = FAIL_SERVER;
        return PAY_RETRY;
    }

    int ambiguous = !(rc == CURLE_OPERATION_TIMEDOUT ||
                      rc == CURLE_COULDNT_CONNECT ||
                      rc == CURLE_COULDNT_RESOLVE_HOST);
    http_resp_free(&r);
    snprintf(err, err_sz, "payment_curl_%d", (int)rc);

    if (!ambiguous) {
        *fail_kind = FAIL_TRANSPORT;
        return PAY_RETRY;
    }

    /* Соединение разорвалось после выполнения — спрашиваем, было ли списание. */
    int has = 0;
    if (payment_has_charge(w, o->id, &has) == 0) {
        *fail_kind = FAIL_OK;
        if (has) {
            snprintf(err, err_sz, "reconciled_charge");
            return PAY_OK;
        }
        snprintf(err, err_sz, "payment_not_charged");
        return PAY_RETRY;
    }

    *fail_kind = FAIL_TRANSPORT;
    snprintf(err, err_sz, "payment_curl_%d_reconcile_failed", (int)rc);
    return PAY_RETRY;
}

/* order processing --------------------------------------------------------- */

static void finish(worker *w, Order *o, const char *status)
{
    for (int i = 0; i < 5; i++) {
        if (db_set_terminal(w->db, o->id, status) == 0) {
            return;
        }
        sleep_ms(20);
    }
    (void)db_retry_later(w->db, o->id, 200, "db_set_terminal_failed");
}

static void process_order(worker *w, Order *o)
{
    if (reserve(w, o) != 0) {
        (void)db_retry_later(w->db, o->id, retry_delay_ms(w, o->attempts),
                             "inventory_reserve");
        return;
    }

    if (o->amount_cents == 0) {
        finish(w, o, ORDER_STATUS_PAID);
        return;
    }

    int is_probe = 0;
    if (!breaker_try_begin(&w->br, &is_probe)) {
        (void)db_retry_later(w->db, o->id,
                             200 + (int)(now_ms() % 200), "payment_gate");
        return;
    }

    char err[256] = "";
    int fail_kind = FAIL_OK;
    int result = charge(w, o, &fail_kind, err, sizeof(err));
    breaker_end(&w->br, is_probe, fail_kind);

    if (result == PAY_OK) {
        finish(w, o, ORDER_STATUS_PAID);
    } else if (result == PAY_DECLINED) {
        if (release(w, o) == 0) {
            finish(w, o, ORDER_STATUS_REJECTED);
        } else {
            (void)db_retry_later(w->db, o->id,
                                 retry_delay_ms(w, o->attempts),
                                 "inventory_release");
        }
    } else {
        (void)db_retry_later(w->db, o->id, retry_delay_ms(w, o->attempts),
                             err[0] != '\0' ? err : "payment");
    }
}

/* outgoing events ---------------------------------------------------------- */

static void publish_event(worker *w, Event *e)
{
    int is_probe = 0;
    if (!breaker_try_begin(&w->bus_br, &is_probe)) {
        (void)db_event_retry_later(w->db, e->id,
                                   200 + (int)(now_ms() % 200), "bus_gate");
        return;
    }

    char url[2048];
    snprintf(url, sizeof(url), "%s/events", w->events_url);

    char body[512];
    snprintf(body, sizeof(body),
             "{\"id\":\"%s\",\"type\":\"order.paid\",\"order_id\":\"%s\","
             "\"occurred_at\":\"%s\"}",
             e->id, e->id, e->occurred_at);

    HttpResp r;
    CURLcode rc = http_request("POST", url, body, NULL,
                               w->cfg.event_timeout_ms, &r);

    int fail_kind = FAIL_OK;
    int ok = 0;
    char err[256] = "";
    if (rc == CURLE_OK) {
        if (r.status == 202) {
            ok = 1;
        } else {
            fail_kind = FAIL_SERVER;
            snprintf(err, sizeof(err), "bus_status_%ld", r.status);
        }
    } else {
        fail_kind = FAIL_TRANSPORT;
        snprintf(err, sizeof(err), "bus_curl_%d", (int)rc);
    }
    http_resp_free(&r);
    breaker_end(&w->bus_br, is_probe, fail_kind);

    if (ok) {
        if (db_mark_event_published(w->db, e->id) != 0) {
            (void)db_event_retry_later(w->db, e->id, 200,
                                       "db_mark_event_failed");
        }
        return;
    }

    log_warn("event %s: %s", e->id, err);
    (void)db_event_retry_later(w->db, e->id, retry_delay_ms(w, e->attempts),
                               err);
}

static void *event_worker_main(void *arg)
{
    worker *w = arg;
    while (!w->stop) {
        int cd = breaker_cooldown_ms(&w->bus_br);
        if (cd > 0) {
            sleep_ms(cd > 200 ? 200 : cd);
            continue;
        }
        Event e;
        memset(&e, 0, sizeof(e));
        int r = db_claim_event(w->db, &e);
        if (r < 0) {
            sleep_ms(200);
            continue;
        }
        if (r == 0) {
            sleep_ms(100);
            continue;
        }
        publish_event(w, &e);
    }
    return NULL;
}

static void *worker_main(void *arg)
{
    worker *w = arg;
    while (!w->stop) {
        int cd = breaker_cooldown_ms(&w->br);
        if (cd > 0) {
            sleep_ms(cd > 200 ? 200 : cd);
            continue;
        }
        Order o;
        memset(&o, 0, sizeof(o));
        int r = db_claim_order(w->db, &o);
        if (r < 0) {
            sleep_ms(200);
            continue;
        }
        if (r == 0) {
            sleep_ms(100);
            continue;
        }
        process_order(w, &o);
        order_free(&o);
    }
    return NULL;
}

worker *worker_start(db_pool *db, const char *payment_url,
                     const char *inventory_url, const char *events_url,
                     const WorkerConfig *cfg)
{
    worker *w = calloc(1, sizeof(*w));
    if (w == NULL) {
        return NULL;
    }
    w->db = db;
    w->payment_url = payment_url;
    w->inventory_url = inventory_url;
    w->events_url = events_url;
    w->cfg = *cfg;
    breaker_init(&w->br, cfg->payment_max_inflight, cfg->breaker_base_ms,
                 cfg->breaker_max_ms, 2, 20);
    breaker_init(&w->bus_br, cfg->bus_max_inflight,
                 cfg->bus_breaker_base_ms, cfg->bus_breaker_max_ms, 2, 2);

    int n = cfg->worker_threads > 0 ? cfg->worker_threads : 8;
    int ne = cfg->event_threads > 0 ? cfg->event_threads : 2;
    w->tids = calloc((size_t)n, sizeof(*w->tids));
    w->event_tids = calloc((size_t)ne, sizeof(*w->event_tids));
    if (w->tids == NULL || w->event_tids == NULL) {
        pthread_mutex_destroy(&w->br.mu);
        pthread_mutex_destroy(&w->bus_br.mu);
        free(w->tids);
        free(w->event_tids);
        free(w);
        return NULL;
    }

    for (int i = 0; i < n; i++) {
        if (pthread_create(&w->tids[i], NULL, worker_main, w) != 0) {
            log_error("не удалось создать worker %d", i);
            break;
        }
        w->nthreads++;
    }
    if (w->nthreads == 0) {
        pthread_mutex_destroy(&w->br.mu);
        pthread_mutex_destroy(&w->bus_br.mu);
        free(w->tids);
        free(w->event_tids);
        free(w);
        return NULL;
    }

    if (events_url != NULL && *events_url != '\0') {
        for (int i = 0; i < ne; i++) {
            if (pthread_create(&w->event_tids[i], NULL, event_worker_main,
                               w) != 0) {
                log_error("не удалось создать event worker %d", i);
                break;
            }
            w->nevent_threads++;
        }
        if (w->nevent_threads == 0) {
            w->stop = 1;
            for (int i = 0; i < w->nthreads; i++) {
                pthread_join(w->tids[i], NULL);
            }
            pthread_mutex_destroy(&w->br.mu);
            pthread_mutex_destroy(&w->bus_br.mu);
            free(w->tids);
            free(w->event_tids);
            free(w);
            return NULL;
        }
    }

    log_info("orders: запущено %d воркеров, %d event-воркеров",
             w->nthreads, w->nevent_threads);
    return w;
}

void worker_stop(worker *w)
{
    if (w == NULL) {
        return;
    }
    w->stop = 1;
    for (int i = 0; i < w->nthreads; i++) {
        pthread_join(w->tids[i], NULL);
    }
    for (int i = 0; i < w->nevent_threads; i++) {
        pthread_join(w->event_tids[i], NULL);
    }
    pthread_mutex_destroy(&w->br.mu);
    pthread_mutex_destroy(&w->bus_br.mu);
    free(w->tids);
    free(w->event_tids);
    free(w);
}
