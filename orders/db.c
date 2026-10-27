#define _GNU_SOURCE
#include "orders.h"

#include <cjson/cJSON.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct db_pool {
    PGconn **conns;
    unsigned char *used;
    int size;
    pthread_mutex_t mu;
    pthread_cond_t cv;
};

static void copy_str(char *dst, size_t dst_sz, const char *src)
{
    if (dst_sz == 0) {
        return;
    }
    snprintf(dst, dst_sz, "%s", src ? src : "");
}

static PGconn *connect_one(const char *url)
{
    PGconn *c = PQconnectdb(url);
    if (c == NULL) {
        return NULL;
    }
    if (PQstatus(c) != CONNECTION_OK) {
        log_error("postgres: %s", PQerrorMessage(c));
        PQfinish(c);
        return NULL;
    }
    return c;
}

db_pool *db_pool_create(const char *url, int size)
{
    if (size < 1) {
        size = 1;
    }
    db_pool *p = calloc(1, sizeof(*p));
    if (p == NULL) {
        return NULL;
    }
    p->conns = calloc((size_t)size, sizeof(*p->conns));
    p->used = calloc((size_t)size, sizeof(*p->used));
    if (p->conns == NULL || p->used == NULL) {
        free(p->conns);
        free(p->used);
        free(p);
        return NULL;
    }
    p->size = size;
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv, NULL);

    for (int i = 0; i < size; i++) {
        p->conns[i] = connect_one(url);
        if (p->conns[i] == NULL) {
            db_pool_destroy(p);
            return NULL;
        }
    }
    return p;
}

void db_pool_destroy(db_pool *p)
{
    if (p == NULL) {
        return;
    }
    for (int i = 0; i < p->size; i++) {
        if (p->conns[i] != NULL) {
            PQfinish(p->conns[i]);
        }
    }
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cv);
    free(p->conns);
    free(p->used);
    free(p);
}

static PGconn *pool_get(db_pool *p)
{
    pthread_mutex_lock(&p->mu);
    for (;;) {
        for (int i = 0; i < p->size; i++) {
            if (!p->used[i]) {
                p->used[i] = 1;
                PGconn *c = p->conns[i];
                pthread_mutex_unlock(&p->mu);
                if (PQstatus(c) != CONNECTION_OK) {
                    PQreset(c);
                }
                return c;
            }
        }
        pthread_cond_wait(&p->cv, &p->mu);
    }
}

static void pool_put(db_pool *p, PGconn *c)
{
    pthread_mutex_lock(&p->mu);
    for (int i = 0; i < p->size; i++) {
        if (p->conns[i] == c) {
            p->used[i] = 0;
            break;
        }
    }
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->mu);
}

static PGresult *exec_params(db_pool *p, const char *sql, int nparams,
                             const char *const *values)
{
    PGconn *c = pool_get(p);
    PGresult *r = PQexecParams(c, sql, nparams, NULL, values, NULL, NULL, 0);
    pool_put(p, c);
    return r;
}

static int result_ok(const PGresult *r)
{
    if (r == NULL) {
        return 0;
    }
    ExecStatusType s = PQresultStatus(r);
    return s == PGRES_COMMAND_OK || s == PGRES_TUPLES_OK;
}

int db_init_schema(db_pool *p)
{
    static const char *sql =
        "CREATE TABLE IF NOT EXISTS orders ("
        "  id TEXT PRIMARY KEY,"
        "  user_id TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  amount_cents BIGINT NOT NULL,"
        "  items JSONB NOT NULL,"
        "  attempts INT NOT NULL DEFAULT 0,"
        "  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
        "  lease_until TIMESTAMPTZ,"
        "  last_error TEXT,"
        "  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
        "  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
        ");"
        "CREATE INDEX IF NOT EXISTS orders_pending_idx"
        "  ON orders (status, next_attempt_at) WHERE status = 'pending';";

    PGconn *c = pool_get(p);
    PGresult *r = PQexec(c, sql);
    int ok = result_ok(r);
    if (!ok) {
        log_error("postgres: schema: %s", PQerrorMessage(c));
    }
    PQclear(r);
    pool_put(p, c);
    return ok ? 0 : -1;
}

void db_reset_leases(db_pool *p)
{
    PGconn *c = pool_get(p);
    PGresult *r = PQexec(c,
        "UPDATE orders SET lease_until = NULL, updated_at = now()"
        " WHERE status = 'pending' AND lease_until IS NOT NULL");
    if (!result_ok(r)) {
        log_warn("postgres: reset leases: %s", PQerrorMessage(c));
    }
    PQclear(r);
    pool_put(p, c);
}

int db_insert_order(db_pool *p, const char *id, const char *user_id,
                    int64_t amount_cents, const char *items_json)
{
    char amount[32];
    snprintf(amount, sizeof(amount), "%lld", (long long)amount_cents);
    const char *vals[5] = {id, user_id, ORDER_STATUS_PENDING, amount, items_json};
    PGresult *r = exec_params(p,
        "INSERT INTO orders (id, user_id, status, amount_cents, items)"
        " VALUES ($1, $2, $3, $4::bigint, $5::jsonb)", 5, vals);
    int ok = result_ok(r);
    if (!ok && r != NULL) {
        log_error("postgres: insert order %s: %s", id, PQresultErrorMessage(r));
    }
    PQclear(r);
    return ok ? 0 : -1;
}

int db_get_order(db_pool *p, const char *id, char *status, size_t status_sz,
                 int64_t *amount_cents)
{
    const char *vals[1] = {id};
    PGresult *r = exec_params(p,
        "SELECT status, amount_cents::text FROM orders WHERE id = $1", 1, vals);
    if (!result_ok(r)) {
        if (r != NULL) {
            log_error("postgres: get order %s: %s", id, PQresultErrorMessage(r));
        }
        PQclear(r);
        return -1;
    }
    int rc;
    if (PQntuples(r) == 0) {
        rc = 0;
    } else {
        copy_str(status, status_sz, PQgetvalue(r, 0, 0));
        if (amount_cents != NULL) {
            *amount_cents = strtoll(PQgetvalue(r, 0, 1), NULL, 10);
        }
        rc = 1;
    }
    PQclear(r);
    return rc;
}

static int parse_items(const char *json, Order *o)
{
    cJSON *arr = cJSON_Parse(json);
    if (!cJSON_IsArray(arr)) {
        cJSON_Delete(arr);
        return -1;
    }
    int n = cJSON_GetArraySize(arr);
    if (n < 1) {
        cJSON_Delete(arr);
        return -1;
    }
    o->items = calloc((size_t)n, sizeof(*o->items));
    if (o->items == NULL) {
        cJSON_Delete(arr);
        return -1;
    }
    int i = 0;
    cJSON *it = NULL;
    cJSON_ArrayForEach(it, arr) {
        cJSON *sku = cJSON_GetObjectItemCaseSensitive(it, "sku");
        cJSON *qty = cJSON_GetObjectItemCaseSensitive(it, "qty");
        if (!cJSON_IsString(sku) || sku->valuestring == NULL ||
            !cJSON_IsNumber(qty)) {
            cJSON_Delete(arr);
            return -1;
        }
        o->items[i].sku = strdup(sku->valuestring);
        o->items[i].qty = (int)qty->valuedouble;
        if (o->items[i].sku == NULL) {
            cJSON_Delete(arr);
            return -1;
        }
        i++;
    }
    o->items_len = (size_t)i;
    cJSON_Delete(arr);
    return 0;
}

int db_claim_order(db_pool *p, Order *out)
{
    static const char *sql =
        "WITH due AS ("
        "  SELECT id FROM orders"
        "   WHERE status = 'pending'"
        "     AND next_attempt_at <= now()"
        "     AND (lease_until IS NULL OR lease_until < now())"
        "   ORDER BY next_attempt_at, created_at"
        "   LIMIT 1"
        "   FOR UPDATE SKIP LOCKED"
        ")"
        "UPDATE orders o"
        "   SET lease_until = now() + interval '60 seconds',"
        "       updated_at = now()"
        "  FROM due"
        " WHERE o.id = due.id"
        " RETURNING o.id, o.user_id, o.amount_cents::text,"
        "           o.items::text, o.attempts::text";

    PGresult *r = exec_params(p, sql, 0, NULL);
    if (!result_ok(r)) {
        if (r != NULL) {
            log_error("postgres: claim: %s", PQresultErrorMessage(r));
        }
        PQclear(r);
        return -1;
    }
    if (PQntuples(r) == 0) {
        PQclear(r);
        return 0;
    }

    memset(out, 0, sizeof(*out));
    copy_str(out->id, sizeof(out->id), PQgetvalue(r, 0, 0));
    out->user_id = strdup(PQgetvalue(r, 0, 1));
    out->amount_cents = strtoll(PQgetvalue(r, 0, 2), NULL, 10);
    out->attempts = atoi(PQgetvalue(r, 0, 4));

    char *items_json = strdup(PQgetvalue(r, 0, 3));
    PQclear(r);

    if (out->user_id == NULL || items_json == NULL) {
        free(items_json);
        order_free(out);
        return -1;
    }
    if (parse_items(items_json, out) != 0) {
        free(items_json);
        char id[sizeof(out->id)];
        copy_str(id, sizeof(id), out->id);
        order_free(out);
        (void)db_retry_later(p, id, 1000, "bad_items_json");
        return -1;
    }
    free(items_json);
    return 1;
}

int db_set_terminal(db_pool *p, const char *id, const char *status)
{
    const char *vals[2] = {id, status};
    PGresult *r = exec_params(p,
        "UPDATE orders"
        "   SET status = $2, lease_until = NULL, last_error = NULL,"
        "       updated_at = now()"
        " WHERE id = $1", 2, vals);
    int ok = result_ok(r);
    if (!ok && r != NULL) {
        log_error("postgres: set %s order %s: %s", status, id,
                  PQresultErrorMessage(r));
    }
    PQclear(r);
    return ok ? 0 : -1;
}

int db_retry_later(db_pool *p, const char *id, int delay_ms, const char *err)
{
    if (delay_ms < 0) {
        delay_ms = 0;
    }
    char delay[32];
    snprintf(delay, sizeof(delay), "%d", delay_ms);
    const char *vals[3] = {id, err ? err : "", delay};
    PGresult *r = exec_params(p,
        "UPDATE orders"
        "   SET attempts = attempts + 1,"
        "       last_error = $2,"
        "       next_attempt_at = now() + ($3 || ' milliseconds')::interval,"
        "       lease_until = NULL,"
        "       updated_at = now()"
        " WHERE id = $1", 3, vals);
    int ok = result_ok(r);
    if (!ok && r != NULL) {
        log_error("postgres: retry order %s: %s", id, PQresultErrorMessage(r));
    }
    PQclear(r);
    return ok ? 0 : -1;
}

void order_free(Order *o)
{
    if (o == NULL) {
        return;
    }
    free(o->user_id);
    for (size_t i = 0; i < o->items_len; i++) {
        free(o->items[i].sku);
    }
    free(o->items);
    memset(o, 0, sizeof(*o));
}
