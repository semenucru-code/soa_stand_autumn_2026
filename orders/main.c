#define _GNU_SOURCE
#include "orders.h"

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <fcntl.h>
#include <microhttpd.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <unistd.h>

#define MAX_BODY_BYTES (1024 * 1024)

typedef struct {
    db_pool *db;
    worker *wrk;
} app_ctx;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
    int too_large;
} ReqBody;

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* helpers ------------------------------------------------------------------ */

static const char *must_env(const char *name)
{
    const char *v = getenv(name);
    if (v == NULL || *v == '\0') {
        log_error("не задана переменная окружения %s", name);
        exit(1);
    }
    return v;
}

static char *url_env(const char *name)
{
    char *s = strdup(must_env(name));
    if (s == NULL) {
        log_error("нет памяти");
        exit(1);
    }
    size_t n = strlen(s);
    while (n > 0 && s[n - 1] == '/') {
        s[--n] = '\0';
    }
    return s;
}

static int int_env(const char *name, int def)
{
    const char *v = getenv(name);
    if (v == NULL || *v == '\0') {
        return def;
    }
    char *end = NULL;
    long x = strtol(v, &end, 10);
    if (end == v || x <= 0 || x > 1000000) {
        return def;
    }
    return (int)x;
}

static void gen_uuid(char *out, size_t out_sz)
{
    unsigned char b[16];
    if (getrandom(b, sizeof(b), 0) != (ssize_t)sizeof(b)) {
        int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            ssize_t n = read(fd, b, sizeof(b));
            close(fd);
            if (n != (ssize_t)sizeof(b)) {
                memset(b, 0, sizeof(b));
            }
        } else {
            memset(b, 0, sizeof(b));
        }
    }
    b[6] = (unsigned char)((b[6] & 0x0f) | 0x40);
    b[8] = (unsigned char)((b[8] & 0x3f) | 0x80);
    snprintf(out, out_sz,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
             "%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
             b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

/* responses ---------------------------------------------------------------- */

static enum MHD_Result reply_json(struct MHD_Connection *conn, unsigned int code,
                                  const char *json)
{
    struct MHD_Response *resp = MHD_create_response_from_buffer(
        strlen(json), (void *)json, MHD_RESPMEM_MUST_COPY);
    if (resp == NULL) {
        return MHD_NO;
    }
    MHD_add_response_header(resp, "Content-Type", "application/json");
    MHD_add_response_header(resp, "Cache-Control", "no-store");
    enum MHD_Result ret = MHD_queue_response(conn, code, resp);
    MHD_destroy_response(resp);
    return ret;
}

static enum MHD_Result reply_error(struct MHD_Connection *conn, unsigned int code,
                                   const char *error)
{
    char body[256];
    snprintf(body, sizeof(body), "{\"error\":\"%s\"}", error);
    return reply_json(conn, code, body);
}

/* validation --------------------------------------------------------------- */

static int valid_request(cJSON *root, int64_t *amount_out, cJSON **items_out)
{
    cJSON *user = cJSON_GetObjectItemCaseSensitive(root, "user_id");
    if (!cJSON_IsString(user) || user->valuestring == NULL ||
        user->valuestring[0] == '\0') {
        return -1;
    }
    cJSON *items = cJSON_GetObjectItemCaseSensitive(root, "items");
    if (!cJSON_IsArray(items) || cJSON_GetArraySize(items) < 1) {
        return -1;
    }

    int64_t amount = 0;
    cJSON *it = NULL;
    cJSON_ArrayForEach(it, items) {
        cJSON *sku = cJSON_GetObjectItemCaseSensitive(it, "sku");
        cJSON *qty = cJSON_GetObjectItemCaseSensitive(it, "qty");
        cJSON *price = cJSON_GetObjectItemCaseSensitive(it, "price_cents");
        if (!cJSON_IsString(sku) || sku->valuestring == NULL ||
            sku->valuestring[0] == '\0') {
            return -1;
        }
        if (!cJSON_IsNumber(qty) || !cJSON_IsNumber(price)) {
            return -1;
        }
        double qd = qty->valuedouble;
        double pd = price->valuedouble;
        if (qd < 1.0 || qd > 1000000000.0 || qd != (double)(long long)qd) {
            return -1;
        }
        if (pd < 0.0 || pd > 9.0e18 || pd != (double)(long long)pd) {
            return -1;
        }
        int64_t line = 0;
        if (__builtin_mul_overflow((int64_t)qd, (int64_t)pd, &line)) {
            return -1;
        }
        if (__builtin_add_overflow(amount, line, &amount)) {
            return -1;
        }
    }

    *amount_out = amount;
    *items_out = items;
    return 0;
}

/* POST /orders ------------------------------------------------------------- */

static enum MHD_Result handle_create_order(app_ctx *app,
                                           struct MHD_Connection *conn,
                                           const char *body, size_t len)
{
    cJSON *root = cJSON_ParseWithLength(body, len);
    if (root == NULL) {
        return reply_error(conn, MHD_HTTP_BAD_REQUEST, "bad_request");
    }

    int64_t amount = 0;
    cJSON *items = NULL;
    if (valid_request(root, &amount, &items) != 0) {
        cJSON_Delete(root);
        return reply_error(conn, MHD_HTTP_BAD_REQUEST, "bad_request");
    }
    cJSON *user = cJSON_GetObjectItemCaseSensitive(root, "user_id");
    char *items_json = cJSON_PrintUnformatted(items);
    if (items_json == NULL) {
        cJSON_Delete(root);
        return reply_error(conn, MHD_HTTP_BAD_REQUEST, "bad_request");
    }

    char id[64];
    gen_uuid(id, sizeof(id));

    int rc = -1;
    for (int attempt = 0; attempt < 3 && rc != 0; attempt++) {
        rc = db_insert_order(app->db, id, user->valuestring, amount, items_json);
        if (rc != 0) {
            usleep(20000);
        }
    }
    if (rc != 0) {
        free(items_json);
        cJSON_Delete(root);
        return reply_error(conn, MHD_HTTP_INTERNAL_SERVER_ERROR, "internal");
    }

    char resp_json[256];
    snprintf(resp_json, sizeof(resp_json),
             "{\"id\":\"%s\",\"status\":\"%s\"}", id, ORDER_STATUS_PENDING);
    enum MHD_Result ret = reply_json(conn, MHD_HTTP_CREATED, resp_json);
    free(items_json);
    cJSON_Delete(root);
    log_info("order %s: %s, amount=%lld", id, ORDER_STATUS_PENDING,
             (long long)amount);
    return ret;
}

/* GET /orders/{id} --------------------------------------------------------- */

static enum MHD_Result handle_get_order(app_ctx *app,
                                        struct MHD_Connection *conn,
                                        const char *url)
{
    const char *id = url + strlen("/orders/");
    size_t id_len = strcspn(id, "?");
    if (id_len == 0 || id_len >= 64 || memchr(id, '/', id_len) != NULL) {
        return reply_error(conn, MHD_HTTP_NOT_FOUND, "not_found");
    }
    char order_id[64];
    memcpy(order_id, id, id_len);
    order_id[id_len] = '\0';

    char status[32];
    int64_t amount = 0;
    int rc = db_get_order(app->db, order_id, status, sizeof(status), &amount);
    if (rc == 0) {
        return reply_error(conn, MHD_HTTP_NOT_FOUND, "not_found");
    }
    if (rc < 0) {
        return reply_error(conn, MHD_HTTP_INTERNAL_SERVER_ERROR, "internal");
    }

    char resp_json[256];
    snprintf(resp_json, sizeof(resp_json),
             "{\"id\":\"%s\",\"status\":\"%s\",\"amount_cents\":%lld}",
             order_id, status, (long long)amount);
    return reply_json(conn, MHD_HTTP_OK, resp_json);
}

/* MHD callbacks ------------------------------------------------------------ */

static void request_completed(void *cls, struct MHD_Connection *conn,
                              void **con_cls,
                              enum MHD_RequestTerminationCode toe)
{
    (void)cls;
    (void)conn;
    (void)toe;
    ReqBody *rb = *con_cls;
    if (rb != NULL) {
        free(rb->data);
        free(rb);
        *con_cls = NULL;
    }
}

static enum MHD_Result handle_request(void *cls, struct MHD_Connection *conn,
                                      const char *url, const char *method,
                                      const char *version,
                                      const char *upload_data,
                                      size_t *upload_data_size, void **con_cls)
{
    (void)version;
    app_ctx *app = cls;

    if (strcmp(method, "POST") == 0 && strcmp(url, "/orders") == 0) {
        ReqBody *rb = *con_cls;
        if (rb == NULL) {
            rb = calloc(1, sizeof(*rb));
            if (rb == NULL) {
                return reply_error(conn, MHD_HTTP_INTERNAL_SERVER_ERROR,
                                   "internal");
            }
            *con_cls = rb;
            return MHD_YES;
        }
        if (*upload_data_size != 0) {
            size_t n = *upload_data_size;
            if (!rb->too_large) {
                if (rb->len + n > MAX_BODY_BYTES) {
                    rb->too_large = 1;
                } else {
                    if (rb->len + n + 1 > rb->cap) {
                        size_t cap = rb->cap == 0 ? 1024 : rb->cap;
                        while (cap < rb->len + n + 1) {
                            cap *= 2;
                        }
                        char *nb = realloc(rb->data, cap);
                        if (nb == NULL) {
                            rb->too_large = 1;
                        } else {
                            rb->data = nb;
                            rb->cap = cap;
                        }
                    }
                    if (!rb->too_large) {
                        memcpy(rb->data + rb->len, upload_data, n);
                        rb->len += n;
                        rb->data[rb->len] = '\0';
                    }
                }
            }
            *upload_data_size = 0;
            return MHD_YES;
        }

        enum MHD_Result ret;
        if (rb->too_large) {
            ret = reply_error(conn, MHD_HTTP_BAD_REQUEST, "bad_request");
        } else {
            ret = handle_create_order(app, conn,
                                      rb->data != NULL ? rb->data : "",
                                      rb->len);
        }
        free(rb->data);
        free(rb);
        *con_cls = NULL;
        return ret;
    }

    if (strcmp(method, "GET") == 0 && strcmp(url, "/healthz") == 0) {
        return reply_json(conn, MHD_HTTP_OK, "{\"status\":\"ok\"}");
    }

    if (strcmp(method, "GET") == 0 && strncmp(url, "/orders/", 8) == 0) {
        return handle_get_order(app, conn, url);
    }

    return reply_error(conn, MHD_HTTP_NOT_FOUND, "not_found");
}

/* main --------------------------------------------------------------------- */

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    const char *database_url = must_env("DATABASE_URL");
    char *payment_url = url_env("PAYMENT_URL");
    char *inventory_url = url_env("INVENTORY_URL");
    int port = int_env("PORT", 8080);

    WorkerConfig cfg;
    cfg.worker_threads = int_env("WORKER_THREADS", 8);
    cfg.payment_max_inflight = int_env("PAYMENT_MAX_INFLIGHT", 4);
    cfg.payment_timeout_ms = int_env("PAYMENT_TIMEOUT_MS", 5000);
    cfg.inventory_timeout_ms = int_env("INVENTORY_TIMEOUT_MS", 5000);
    cfg.breaker_base_ms = int_env("BREAKER_BASE_MS", 500);
    cfg.breaker_max_ms = int_env("BREAKER_MAX_MS", 4000);
    cfg.backoff_base_ms = int_env("BACKOFF_BASE_MS", 200);
    cfg.backoff_max_ms = int_env("BACKOFF_MAX_MS", 4000);
    cfg.lease_seconds = int_env("LEASE_SECONDS", 60);

    if (curl_global_init(CURL_GLOBAL_DEFAULT) != 0) {
        log_error("curl_global_init");
        return 1;
    }

    db_pool *db = NULL;
    for (int i = 0; i < 120 && db == NULL; i++) {
        db = db_pool_create(database_url, 16);
        if (db != NULL && db_init_schema(db) != 0) {
            db_pool_destroy(db);
            db = NULL;
        }
        if (db == NULL) {
            log_info("база недоступна, жду");
            sleep(1);
        }
    }
    if (db == NULL) {
        log_error("не удалось подключиться к базе");
        return 1;
    }
    db_reset_leases(db);

    worker *wrk = worker_start(db, payment_url, inventory_url, &cfg);
    if (wrk == NULL) {
        log_error("не удалось запустить воркеры");
        db_pool_destroy(db);
        return 1;
    }

    app_ctx app = {db, wrk};

    struct MHD_Daemon *daemon = MHD_start_daemon(
        MHD_USE_THREAD_PER_CONNECTION, (uint16_t)port, NULL, NULL,
        &handle_request, &app,
        MHD_OPTION_NOTIFY_COMPLETED, request_completed, NULL,
        MHD_OPTION_CONNECTION_TIMEOUT, (unsigned int)60,
        MHD_OPTION_END);
    if (daemon == NULL) {
        log_error("не удалось запустить HTTP-сервер на порту %d", port);
        worker_stop(wrk);
        db_pool_destroy(db);
        return 1;
    }
    log_info("orders: слушаю :%d", port);

    while (!g_stop) {
        sleep(1);
    }
    log_info("orders: остановка");

    MHD_stop_daemon(daemon);
    worker_stop(wrk);
    db_pool_destroy(db);
    free(payment_url);
    free(inventory_url);
    curl_global_cleanup();
    return 0;
}
