// BoringSSL compatibility shims. librdkafka is routed onto swift-nio-ssl's vendored BoringSSL
// (CNIOBoringSSL), which omits two OpenSSL functions librdkafka references. Including the
// CNIOBoringSSL headers activates the symbol-prefix macros, so the BoringSSL calls below bind to
// CNIOBoringSSL_*; the two functions we define are absent from BoringSSL (not prefixed) and satisfy
// librdkafka's plain references.
#include <CNIOBoringSSL_rand.h>
#include <CNIOBoringSSL_ssl.h>
#include <CNIOBoringSSL_x509.h>

int RAND_priv_bytes(unsigned char *buf, int num) {
    return RAND_bytes(buf, (size_t)num);
}

int SSL_CTX_use_cert_and_key(SSL_CTX *ctx, X509 *cert, EVP_PKEY *pkey,
                             STACK_OF(X509) *chain, int override) {
    (void)override;
    if (cert != NULL && SSL_CTX_use_certificate(ctx, cert) != 1) return 0;
    if (pkey != NULL && SSL_CTX_use_PrivateKey(ctx, pkey) != 1) return 0;
    if (chain != NULL) {
        for (size_t i = 0; i < sk_X509_num(chain); i++)
            if (SSL_CTX_add1_chain_cert(ctx, sk_X509_value(chain, i)) != 1) return 0;
    }
    return 1;
}
