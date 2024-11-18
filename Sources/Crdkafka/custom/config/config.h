#ifndef _CONFIG_H_
#define _CONFIG_H_

// distro
#ifdef __APPLE__
#define SOLIB_EXT ".dylib"
#elif defined(__linux__)
#define SOLIB_EXT ".so"
#endif

#pragma clang diagnostic ignored "-Wimplicit-function-declaration"

#ifdef __x86_64__
#define ARCH "x86_64"
#elif defined(__arm64__) || defined(__arm__) || defined(__aarch64__)
#define ARCH "arm64"
#endif

#define CPU "generic"
#define WITHOUT_OPTIMIZATION 0

#define ENABLE_ZLIB 1
#define ENABLE_ZSTD 1
#define ENABLE_SSL 1
#define ENABLE_GSSAPI 1
#define ENABLE_CURL 0
#define ENABLE_DEVEL 0
#define ENABLE_VALGRIND 0
#define ENABLE_REFCNT_DEBUG 0
#define ENABLE_LZ4_EXT 1
#define ENABLE_LZ4_EXT 1
#define ENABLE_REGEX_EXT 1
#define ENABLE_C11THREADS "try"
#define ENABLE_ZLIB 1
#define ENABLE_ZSTD 1
#define ENABLE_SSL 1
#define ENABLE_GSSAPI 1
#define ENABLE_LZ4_EXT 1
#define WITH_STATIC_LINKING 1
#define MKL_APP_NAME "librdkafka"
#define MKL_APP_DESC_ONELINE "The Apache Kafka C/C++ library"

#ifdef __APPLE__
#define WITH_STRIP 0
#define ENABLE_SYSLOG 1
#endif

#ifdef __APPLE__
// gcc
#define WITH_GCC 1
// gxx
#define WITH_GXX 1
#elif defined(__linux__)
// ccenv
#define WITH_CC 1
// cxxenv
#define WITH_CXX 1
#endif

// pkgconfig
#if !(defined(__linux__) && (defined(__arm64__) || defined(__arm__) || defined(__aarch64__)))
#define WITH_PKGCONFIG 1
#endif

#ifdef __linux__
// install
#define WITH_INSTALL 1
// gnulib
#define WITH_GNULD 1
#endif

#ifdef __APPLE__
// osxlib
#define WITH_OSXLD 1
// syslog
#define WITH_SYSLOG 1
#endif

// crc32chw
#ifdef __x86_64__
#define WITH_CRC32C_HW 1
#endif

#ifdef __APPLE__
// rand_r
#define HAVE_RAND_R 1
// strlcpy
#define HAVE_STRLCPY 1
// strcasestr
#define HAVE_STRCASESTR 1
// pthread_setname_darwin
#define HAVE_PTHREAD_SETNAME_DARWIN 1
// getrusage
#define HAVE_GETRUSAGE 1
#endif

#ifdef __linux__
// pthread_setname_gnu
#define HAVE_PTHREAD_SETNAME_GNU 1
#endif

// Common identifiers
// PIC
#define HAVE_PIC 1
// __atomic_32
#define HAVE_ATOMICS_32 1
// __atomic_32
#define HAVE_ATOMICS_32_ATOMIC 1
// atomic_32
#define ATOMIC_OP32(OP1,OP2,PTR,VAL) __atomic_ ## OP1 ## _ ## OP2(PTR, VAL, __ATOMIC_SEQ_CST)
// __atomic_64
#define HAVE_ATOMICS_64 1
// __atomic_64
#define HAVE_ATOMICS_64_ATOMIC 1
// atomic_64
#define ATOMIC_OP64(OP1,OP2,PTR,VAL) __atomic_ ## OP1 ## _ ## OP2(PTR, VAL, __ATOMIC_SEQ_CST)
// atomic_64
#define ATOMIC_OP(OP1,OP2,PTR,VAL) __atomic_ ## OP1 ## _ ## OP2(PTR, VAL, __ATOMIC_SEQ_CST)
// parseversion
#define RDKAFKA_VERSION_STR "2.1.0"
// parseversion
#define MKL_APP_VERSION "2.1.0"
// libdl
#define WITH_LIBDL 1
// WITH_PLUGINS
#define WITH_PLUGINS 1
// zlib
#define WITH_ZLIB 1
// libssl
#define WITH_SSL 1
// libcrypto
#define OPENSSL_SUPPRESS_DEPRECATED "OPENSSL_SUPPRESS_DEPRECATED"
// libsasl2
#define WITH_SASL_CYRUS 1
// libzstd
#define WITH_ZSTD 1
// libcurl
#define WITH_CURL 0
// WITH_HDRHISTOGRAM
#define WITH_HDRHISTOGRAM 1
// WITH_SNAPPY
#define WITH_SNAPPY 1
// WITH_SOCKEM
#define WITH_SOCKEM 1
// WITH_SASL_SCRAM
#define WITH_SASL_SCRAM 1
// WITH_SASL_OAUTHBEARER
#define WITH_SASL_OAUTHBEARER 0
// WITH_OAUTHBEARER_OIDC
#define WITH_OAUTHBEARER_OIDC 0
// regex
#define HAVE_REGEX 1
// strndup
#define HAVE_STRNDUP 1
// strerror_r
#define HAVE_STRERROR_R 1

// BUILT_WITH
#ifdef __APPLE__
#ifdef __x86_64__
#define BUILT_WITH "STATIC_LINKING GCC GXX PKGCONFIG OSXLD LIBDL PLUGINS ZLIB SSL SASL_CYRUS ZSTD CURL HDRHISTOGRAM SYSLOG SNAPPY SOCKEM SASL_SCRAM SASL_OAUTHBEARER OAUTHBEARER_OIDC CRC32C_HW"
#elif defined(__arm64__) || defined(__arm__) || (__aarch64__)
#define BUILT_WITH "STATIC_LINKING GCC GXX PKGCONFIG OSXLD LIBDL PLUGINS ZLIB SSL SASL_CYRUS ZSTD CURL HDRHISTOGRAM SYSLOG SNAPPY SOCKEM SASL_SCRAM SASL_OAUTHBEARER OAUTHBEARER_OIDC"
#endif
#elif defined(__linux__)
#ifdef __x86_64__
#define BUILT_WITH "STATIC_LINKING CC CXX PKGCONFIG INSTALL GNULD LIBDL PLUGINS ZLIB SSL SASL_CYRUS ZSTD CURL HDRHISTOGRAM SNAPPY SOCKEM SASL_SCRAM SASL_OAUTHBEARER OAUTHBEARER_OIDC CRC32C_HW"
#elif defined(__arm64__) || defined(__arm__) || defined(__aarch64__)
#define BUILT_WITH "STATIC_LINKING CC CXX INSTALL GNULD LIBDL PLUGINS ZLIB SSL SASL_CYRUS ZSTD CURL HDRHISTOGRAM SNAPPY SOCKEM SASL_SCRAM SASL_OAUTHBEARER OAUTHBEARER_OIDC"
#endif
#endif

#endif /* _CONFIG_H_ */
