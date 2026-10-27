#define _GNU_SOURCE
#include "orders.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>

#define MAX_RESP_BODY (1024 * 1024)

void http_resp_free(HttpResp *r)
{
    if (r == NULL) {
        return;
    }
    free(r->body);
    memset(r, 0, sizeof(*r));
}

static size_t write_cb(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    HttpResp *r = userdata;
    size_t n = size * nmemb;
    if (n == 0) {
        return 0;
    }
    if (r->len + n > MAX_RESP_BODY) {
        return 0;
    }
    char *nb = realloc(r->body, r->len + n + 1);
    if (nb == NULL) {
        return 0;
    }
    r->body = nb;
    memcpy(nb + r->len, ptr, n);
    r->len += n;
    nb[r->len] = '\0';
    return n;
}

CURLcode http_request(const char *method, const char *url, const char *body,
                      const char *idem_key, long timeout_ms, HttpResp *out)
{
    memset(out, 0, sizeof(*out));

    CURL *h = curl_easy_init();
    if (h == NULL) {
        return CURLE_FAILED_INIT;
    }

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    if (body != NULL) {
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, "Expect:");
    }
    if (idem_key != NULL) {
        char hdr[320];
        snprintf(hdr, sizeof(hdr), "Idempotency-Key: %s", idem_key);
        headers = curl_slist_append(headers, hdr);
    }

    curl_easy_setopt(h, CURLOPT_URL, url);
    curl_easy_setopt(h, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(h, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(h, CURLOPT_TIMEOUT_MS, timeout_ms);
    long connect_ms = timeout_ms < 2000 ? timeout_ms : 2000;
    curl_easy_setopt(h, CURLOPT_CONNECTTIMEOUT_MS, connect_ms);
    curl_easy_setopt(h, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(h, CURLOPT_WRITEDATA, out);
    curl_easy_setopt(h, CURLOPT_USERAGENT, "orders-c/1.0");

    if (strcmp(method, "POST") == 0) {
        curl_easy_setopt(h, CURLOPT_POST, 1L);
        if (body != NULL) {
            curl_easy_setopt(h, CURLOPT_POSTFIELDS, body);
            curl_easy_setopt(h, CURLOPT_POSTFIELDSIZE, (long)strlen(body));
        }
    } else if (strcmp(method, "GET") != 0) {
        curl_easy_setopt(h, CURLOPT_CUSTOMREQUEST, method);
    }

    CURLcode rc = curl_easy_perform(h);
    if (rc == CURLE_OK) {
        curl_easy_getinfo(h, CURLINFO_RESPONSE_CODE, &out->status);
    }
    out->curl_code = rc;

    curl_slist_free_all(headers);
    curl_easy_cleanup(h);
    return rc;
}
