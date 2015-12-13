#include "com_ipinyou_kafka_PyKafkaConsumer.h"
#include "PyKafkaClient.h"
#include <stdlib.h>
#include <errno.h>
#include <syslog.h>

#define CONSUME_BATCH_SIZE 1000
#define CONSUME_BATCH_TIMEOUT_MS 1000

JNIEXPORT jint JNICALL Java_com_ipinyou_kafka_PyKafkaConsumer_consumeMessages
  (JNIEnv *env, jobject obj, jint partition, jlong offset, jobject callback)
{
    if (env->IsSameObject(callback, NULL)) {
        write_log(LOG_ERR, "consume callback Null");
        return -1;
    }

    jclass cls = env->GetObjectClass(obj);
    jfieldID handle_fid = env->GetFieldID(cls, "handle_", "J");
    kafka_consumer_t *consumer =
        (kafka_consumer_t *)env->GetLongField(obj, handle_fid);

    cls = env->GetObjectClass(callback);
    if (env->IsSameObject(cls, NULL)) {
        write_log(LOG_ERR, "class for consume callback Null");
        return -1;
    }
    jmethodID consume_mid = env->GetMethodID(cls, "consume",
                                             "(Ljava/lang/String;)I");
    if (consume_mid == NULL) {
        write_log(LOG_ERR, "GetMethodID[consume] failed");
        return -1;
    }

    kafka_message_t **messages = (kafka_message_t **)calloc(CONSUME_BATCH_SIZE,
                                                     sizeof(kafka_message_t *));
    if (messages == NULL) {
        write_log(LOG_ERR, "calloc failed for messages to be consumed");
        return -1;
    }

    if (rd_kafka_consume_start(consumer->rkt, partition, offset) == -1) {
        write_log(LOG_ERR, "rd_kafka_consume_start failed with errno[%d]",
                  errno);
        free(messages);
        return -1;
    }

    int num = 0;
    bool continued = true;

    do {
        int n = rd_kafka_consume_batch(consumer->rkt, partition,
                                       CONSUME_BATCH_TIMEOUT_MS,
                                       messages, CONSUME_BATCH_SIZE);
        if (n == -1) {
            write_log(LOG_ERR, "rd_kafka_consume_batch failed");
            continue;
        }
        int ret = 0;
        for (int i = 0; i < n; i++) {
            if (ret == 0) {
                jstring message = env->NewStringUTF((char *)messages[i]->payload);
                ret = env->CallIntMethod(callback, consume_mid, message);
#if 0
                int ec = rd_kafka_offset_store(messages[i]->rkt,
                                               messages[i]->partition,
                                               messages[i]->offset);
                if (ec != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    write_log(LOG_ERR, "rd_kafka_store_offset failed"
                              " with errcode[%d]", ec);
                }
#endif
                num++;
            } else if (ret == 1) {
                write_log(LOG_INFO, "message consuming is stopping");
                continued = false;
                ret = 2;
            }
            rd_kafka_message_destroy(messages[i]);
            messages[i] = NULL;
        }
        rd_kafka_poll(consumer->rk, 0);
    } while (continued);

    if (rd_kafka_consume_stop(consumer->rkt, partition) == -1) {
        write_log(LOG_ERR, "rd_kafka_consume_stop failed with errno[%d]",
                  errno);
    }

    free(messages);

    return num;
}

JNIEXPORT jboolean JNICALL Java_com_ipinyou_kafka_PyKafkaConsumer_initialize
  (JNIEnv *env, jobject obj, jstring topic, jstring brokers, jstring conf_path)
{
    jclass cls;
    jfieldID handle_fid;

    jboolean ret = 1;
    const char *topic_str = env->IsSameObject(topic, NULL) ?
        NULL : env->GetStringUTFChars(topic, NULL);
    const char *brokers_str = env->IsSameObject(brokers, NULL) ?
        NULL : env->GetStringUTFChars(brokers, NULL);
    const char *conf_path_str = env->IsSameObject(conf_path, NULL) ?
        NULL : env->GetStringUTFChars(conf_path, NULL);

    kafka_consumer_t *consumer = create_kafka_consumer(topic_str, brokers_str,
                                                       conf_path_str);
    if (consumer == NULL) {
        ret = 0;
        goto rtn;
    }

    cls = env->GetObjectClass(obj);
    handle_fid = env->GetFieldID(cls, "handle_", "J");
    env->SetLongField(obj, handle_fid, (jlong)consumer);

rtn:
    if (topic_str != NULL)
        env->ReleaseStringUTFChars(topic, topic_str);
    if (brokers_str != NULL)
        env->ReleaseStringUTFChars(brokers, brokers_str);
    if (conf_path_str != NULL)
        env->ReleaseStringUTFChars(conf_path, conf_path_str);
    return ret;
}

JNIEXPORT void JNICALL Java_com_ipinyou_kafka_PyKafkaConsumer_dispose
  (JNIEnv *env, jobject obj)
{
    jclass cls = env->GetObjectClass(obj);
    jfieldID handle_fid = env->GetFieldID(cls, "handle_", "J");
    kafka_consumer_t *consumer =
        (kafka_consumer_t *)env->GetLongField(obj, handle_fid);
    env->SetLongField(obj, handle_fid, (jlong)0);
    destroy_kafka_consumer(consumer);
}
