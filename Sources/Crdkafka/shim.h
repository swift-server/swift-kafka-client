#ifndef __RD_KAFKA_SHIM_H__
#define __RD_KAFKA_SHIM_H__
#ifdef __APPLE__
#include "/usr/local/include/librdkafka/rdkafka.h"
#else
#include "/usr/include/librdkafka/rdkafka.h"
#endif
#endif
