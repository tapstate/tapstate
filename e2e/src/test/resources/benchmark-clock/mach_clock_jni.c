#include <jni.h>
#include <dlfcn.h>
#include <limits.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <mach/mach_vm.h>
#include <mach-o/loader.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/sysctl.h>
#if __has_feature(ptrauth_calls)
#include <ptrauth.h>
#endif

/* Only the immutable conversion ratio is retained, never a timestamp or process origin. */
static mach_timebase_info_data_t timebase;

static void refuse(JNIEnv *env, const char *type, const char *message) {
    jclass exception = (*env)->FindClass(env, type);
    if (exception != NULL) (*env)->ThrowNew(env, exception, message);
}

static jlong convert(JNIEnv *env, uint64_t ticks, uint32_t numer, uint32_t denom) {
    if (numer == 0 || denom == 0) {
        refuse(env, "java/lang/ArithmeticException", "Direct clock timebase is zero"); return 0;
    }
    __uint128_t nanos = (__uint128_t) ticks * numer / denom;
    if (nanos > INT64_MAX) {
        refuse(env, "java/lang/ArithmeticException", "Direct clock nanoseconds exceed signed long"); return 0;
    }
    return (jlong) nanos;
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    (void) vm; (void) reserved;
    if (mach_timebase_info(&timebase) != KERN_SUCCESS || timebase.numer == 0 || timebase.denom == 0) return JNI_ERR;
    return JNI_VERSION_1_8;
}

JNIEXPORT jlong JNICALL Java_io_tapstate_adapters_pdk_PdkBenchmarkClock_nativeNanoTime(JNIEnv *env, jclass type) {
    (void) type;
    uint64_t ticks = mach_absolute_time();
    return convert(env, ticks, timebase.numer, timebase.denom);
}

JNIEXPORT jlong JNICALL Java_io_tapstate_adapters_pdk_PdkBenchmarkClock_convertedForControl(
        JNIEnv *env, jclass type, jlong ticks, jlong numer, jlong denom) {
    (void) type;
    if (numer < 0 || numer > UINT32_MAX || denom < 0 || denom > UINT32_MAX) {
        refuse(env, "java/lang/ArithmeticException", "Control scale exceeds unsigned 32 bits"); return 0;
    }
    return convert(env, (uint64_t) ticks, (uint32_t) numer, (uint32_t) denom);
}

static void *code_pointer(void *pointer) {
#if __has_feature(ptrauth_calls)
    return ptrauth_strip(pointer, ptrauth_key_function_pointer);
#else
    return pointer;
#endif
}

static int copy_loaded(uintptr_t address, void *destination, size_t size) {
    mach_vm_size_t actual = 0;
    return mach_vm_read_overwrite(mach_task_self(), (mach_vm_address_t) address, size,
            (mach_vm_address_t) destination, &actual) == KERN_SUCCESS && actual == size;
}

static int image_uuid(uintptr_t base, char output[33]) {
    struct mach_header_64 header;
    if (!copy_loaded(base, &header, sizeof(header)) || header.magic != MH_MAGIC_64
            || header.ncmds > 2048 || header.sizeofcmds > 8388608 || base > UINTPTR_MAX - sizeof(header) - header.sizeofcmds) return 0;
    uintptr_t next = base + sizeof(header), end = next + header.sizeofcmds;
    for (uint32_t i = 0; i < header.ncmds; i++) {
        struct load_command command;
        if (end - next < sizeof(command) || !copy_loaded(next, &command, sizeof(command))
                || command.cmdsize < sizeof(command) || command.cmdsize > end - next) return 0;
        if (command.cmd == LC_UUID) {
            struct uuid_command uuid;
            if (command.cmdsize < sizeof(uuid) || !copy_loaded(next, &uuid, sizeof(uuid))) return 0;
            for (unsigned j = 0; j < 16; j++) snprintf(output + 2 * j, 3, "%02x", uuid.uuid[j]);
            return 1;
        }
        next += command.cmdsize;
    }
    return 0;
}

JNIEXPORT jobjectArray JNICALL Java_io_tapstate_adapters_pdk_PdkBenchmarkClock_nativeFacts(JNIEnv *env, jclass type) {
    (void) type;
    char boot[128] = {0}; size_t length = sizeof(boot);
    Dl_info jni, os;
    void *getter = code_pointer((void *) &Java_io_tapstate_adapters_pdk_PdkBenchmarkClock_nativeNanoTime);
    void *counter = code_pointer((void *) &mach_absolute_time);
    void *resolved = code_pointer(dlsym(RTLD_DEFAULT, "mach_absolute_time"));
    if (sysctlbyname("kern.bootsessionuuid", boot, &length, NULL, 0) != 0 || length == 0 || length > sizeof(boot)
            || memchr(boot, 0, length) == NULL || !dladdr(getter, &jni) || jni.dli_fname == NULL
            || counter == NULL || resolved != counter || !dladdr(counter, &os) || os.dli_fbase == NULL
            || os.dli_fname == NULL || os.dli_sname == NULL || strcmp(os.dli_sname, "mach_absolute_time") != 0
            || strnlen(jni.dli_fname, 513) > 512 || strnlen(os.dli_fname, 513) > 512 || (uintptr_t) counter < (uintptr_t) os.dli_fbase) {
        refuse(env, "java/lang/IllegalStateException", "Direct native clock identity is unavailable"); return NULL;
    }
    char uuid[33] = {0}, code_hex[289] = {0}; unsigned char code[144];
    if (!image_uuid((uintptr_t) os.dli_fbase, uuid) || !copy_loaded((uintptr_t) counter, code, sizeof(code))) {
        refuse(env, "java/lang/IllegalStateException", "Loaded OS clock image cannot be bound"); return NULL;
    }
    /* This is a bounded prefix, not a claim that the complete function body ends here. */
    for (unsigned i = 0; i < sizeof(code); i++) snprintf(code_hex + 2 * i, 3, "%02x", code[i]);
    uint8_t selector; uint64_t offset;
#if defined(__aarch64__)
    const uintptr_t comm_page = UINT64_C(0x0000000fffffc000);
    if (!copy_loaded(comm_page + 0x090, &selector, sizeof(selector)) || !copy_loaded(comm_page + 0x088, &offset, sizeof(offset))) {
        refuse(env, "java/lang/IllegalStateException", "Current user timebase route cannot be read"); return NULL;
    }
#else
    refuse(env, "java/lang/IllegalStateException", "Current user timebase route is unsupported"); return NULL;
#endif
    char numer[16], denom[16], image_offset[32], selected[16], time_offset[32];
    snprintf(numer, sizeof(numer), "%u", timebase.numer); snprintf(denom, sizeof(denom), "%u", timebase.denom);
    snprintf(image_offset, sizeof(image_offset), "%llu", (unsigned long long) ((uintptr_t) counter - (uintptr_t) os.dli_fbase));
    snprintf(selected, sizeof(selected), "%u", selector); snprintf(time_offset, sizeof(time_offset), "%llu", (unsigned long long) offset);
    const char *facts[] = {boot, numer, denom, jni.dli_fname, os.dli_fname, uuid, image_offset, code_hex,
            selected, time_offset, "mach_absolute_time"};
    jclass string = (*env)->FindClass(env, "java/lang/String"); if (string == NULL) return NULL;
    jobjectArray result = (*env)->NewObjectArray(env, 11, string, NULL); if (result == NULL) return NULL;
    for (unsigned i = 0; i < 11; i++) {
        jstring value = (*env)->NewStringUTF(env, facts[i]); if (value == NULL) return NULL;
        (*env)->SetObjectArrayElement(env, result, (jsize) i, value); (*env)->DeleteLocalRef(env, value);
        if ((*env)->ExceptionCheck(env)) return NULL;
    }
    return result;
}
