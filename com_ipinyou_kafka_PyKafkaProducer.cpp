#include "com_ipinyou_kafka_PyKafkaProducer.h"
#include "PyKafkaClient.h"
#include <string>
#include <vector>
#include <stdlib.h>
#include <errno.h>
#include <syslog.h>

JNIEXPORT jint JNICALL Java_com_ipinyou_kafka_PyKafkaProducer_produceMessages
  (JNIEnv *env, jobject obj, jobjectArray payloads, jobjectArray keys)
{
    jsize n = env->GetArrayLength(payloads);

    jclass cls = env->GetObjectClass(obj);
    jfieldID handle_fid = env->GetFieldID(cls, "handle_", "J");
    kafka_producer_t *producer =
        (kafka_producer_t *)env->GetLongField(obj, handle_fid);

    std::vector<std::string> payload_arr, key_arr;

    if (env->IsSameObject(keys, NULL)) {
        for (jsize i = 0; i < n; i++) {
            jstring payload = (jstring)env->GetObjectArrayElement(payloads, i);
            const char *payload_str = env->GetStringUTFChars(payload, NULL);
            payload_arr.push_back(payload_str);
            env->ReleaseStringUTFChars(payload, payload_str);
        }
    } else {
        if (n != env->GetArrayLength(keys)) {
            write_log(LOG_ERR, "numbers of payloads and keys mismatch");
            return -1;
        }
        for (jsize i = 0; i < n; i++) {
            jstring payload = (jstring)env->GetObjectArrayElement(payloads, i);
            jstring key = (jstring)env->GetObjectArrayElement(keys, i);
            const char *payload_str = env->GetStringUTFChars(payload, NULL);
            const char *key_str = env->GetStringUTFChars(key, NULL);
            payload_arr.push_back(payload_str);
            key_arr.push_back(key_str);
            env->ReleaseStringUTFChars(payload, payload_str);
            env->ReleaseStringUTFChars(key, key_str);
        }
    }

    return (jint)produce_messages(producer, payload_arr, key_arr);
}

JNIEXPORT jboolean JNICALL Java_com_ipinyou_kafka_PyKafkaProducer_initialize
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

    kafka_producer_t *producer = create_kafka_producer(topic_str, brokers_str,
                                                       conf_path_str);
    if (producer == NULL) {
        ret = 0;
        goto rtn;
    }

    cls = env->GetObjectClass(obj);
    handle_fid = env->GetFieldID(cls, "handle_", "J");
    env->SetLongField(obj, handle_fid, (jlong)producer);

rtn:
    if (topic_str != NULL)
        env->ReleaseStringUTFChars(topic, topic_str);
    if (brokers_str != NULL)
        env->ReleaseStringUTFChars(brokers, brokers_str);
    if (conf_path_str != NULL)
        env->ReleaseStringUTFChars(conf_path, conf_path_str);
    return ret;
}

JNIEXPORT void JNICALL Java_com_ipinyou_kafka_PyKafkaProducer_dispose
  (JNIEnv *env, jobject obj)
{
    jclass cls = env->GetObjectClass(obj);
    jfieldID handle_fid = env->GetFieldID(cls, "handle_", "J");
    kafka_producer_t *producer =
        (kafka_producer_t *)env->GetLongField(obj, handle_fid);
    env->SetLongField(obj, handle_fid, (jlong)0);
    destroy_kafka_producer(producer);
}
