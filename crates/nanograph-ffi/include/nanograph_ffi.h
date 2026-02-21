#ifndef NANOGRAPH_FFI_H
#define NANOGRAPH_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NanoGraphHandle NanoGraphHandle;

const char *nanograph_last_error_message(void);
void nanograph_string_free(char *value);

NanoGraphHandle *nanograph_db_init(const char *db_path, const char *schema_source);
NanoGraphHandle *nanograph_db_open(const char *db_path);
int32_t nanograph_db_close(NanoGraphHandle *handle);
void nanograph_db_destroy(NanoGraphHandle *handle);

int32_t nanograph_db_load(NanoGraphHandle *handle, const char *data_source, const char *mode);

char *nanograph_db_run(
    NanoGraphHandle *handle,
    const char *query_source,
    const char *query_name,
    const char *params_json
);

char *nanograph_db_check(NanoGraphHandle *handle, const char *query_source);
char *nanograph_db_describe(NanoGraphHandle *handle);
char *nanograph_db_compact(NanoGraphHandle *handle, const char *options_json);
char *nanograph_db_cleanup(NanoGraphHandle *handle, const char *options_json);
char *nanograph_db_doctor(NanoGraphHandle *handle);

#ifdef __cplusplus
}
#endif

#endif
