#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

PG_MODULE_MAGIC;

/* GUC variables */
static char *mysqld_path = NULL;
static char *mysql_client_path = NULL;
static char *mysql_basedir = NULL;

void _PG_init(void);

void _PG_init(void) {
  DefineCustomStringVariable(
      "pg_mysql.mysqld_path", "Path to the mysqld binary.", NULL, &mysqld_path,
      "/opt/homebrew/bin/mysqld", PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomStringVariable("pg_mysql.mysql_path",
                             "Path to the mysql client binary.", NULL,
                             &mysql_client_path, "/opt/homebrew/bin/mysql",
                             PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomStringVariable("pg_mysql.basedir",
                             "Base directory for all MySQL instances. Each "
                             "instance gets <basedir>/<port>/.",
                             NULL, &mysql_basedir, "/tmp/pg_mysql", PGC_SUSET,
                             0, NULL, NULL, NULL);
}

/* ----------------------------------------------------------------
 * Path helpers — everything derived from basedir + port
 * ---------------------------------------------------------------- */
static void instance_datadir(char *buf, size_t sz, int port) {
  snprintf(buf, sz, "%s/%d/data", mysql_basedir, port);
}

static void instance_pidfile(char *buf, size_t sz, int port) {
  snprintf(buf, sz, "%s/%d/mysqld.pid", mysql_basedir, port);
}

static void instance_socket(char *buf, size_t sz, int port) {
  snprintf(buf, sz, "%s/%d/mysql.sock", mysql_basedir, port);
}

static void instance_dir(char *buf, size_t sz, int port) {
  snprintf(buf, sz, "%s/%d", mysql_basedir, port);
}

static void instance_errorlog(char *buf, size_t sz, int port) {
  snprintf(buf, sz, "--log-error=%s/%d/mysqld.err", mysql_basedir, port);
}

/* ----------------------------------------------------------------
 * Process helpers
 * ---------------------------------------------------------------- */
static pid_t read_pid_for_port(int port) {
  char path[MAXPGPATH];
  FILE *f;
  long v;

  instance_pidfile(path, sizeof(path), port);
  f = fopen(path, "r");
  if (!f)
    return 0;
  if (fscanf(f, "%ld", &v) != 1) {
    fclose(f);
    return 0;
  }
  fclose(f);
  if (kill((pid_t)v, 0) == -1)
    return 0;
  return (pid_t)v;
}

static bool datadir_needs_init(int port) {
  char path[MAXPGPATH];
  struct stat st;
  instance_datadir(path, sizeof(path), port);
  snprintf(path + strlen(path), sizeof(path) - strlen(path), "/ibdata1");
  return (stat(path, &st) != 0);
}

/*
 * Check if a TCP port is available by attempting to bind to it.
 */
static bool port_is_free(int port) {
  int fd;
  struct sockaddr_in addr;
  int reuse = 1;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return false;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

#define PG_MYSQL_PORT_MIN 33061
#define PG_MYSQL_PORT_MAX 34060

/*
 * Pick a random free port in [PG_MYSQL_PORT_MIN, PG_MYSQL_PORT_MAX].
 */
static int allocate_port(void) {
  int range = PG_MYSQL_PORT_MAX - PG_MYSQL_PORT_MIN + 1;
  int start;
  int i;

  /* random starting offset within the range */
  start = (int)(pg_prng_uint64(&pg_global_prng_state) % range);

  for (i = 0; i < range; i++) {
    int port = PG_MYSQL_PORT_MIN + ((start + i) % range);
    if (port_is_free(port))
      return port;
  }

  ereport(ERROR, (errmsg("pg_mysql: no free port in range %d-%d",
                         PG_MYSQL_PORT_MIN, PG_MYSQL_PORT_MAX)));
  return 0;
}

/*
 * mkdir -p (simple two-level: basedir/<port> and basedir/<port>/data already
 * handled by mysqld --initialize, but we need the parent).
 */
static void ensure_instance_dir(int port) {
  char path[MAXPGPATH];
  mkdir(mysql_basedir, 0700);
  instance_dir(path, sizeof(path), port);
  mkdir(path, 0700);
}

/*
 * Fork+exec mysql client, capture stdout+stderr into a palloc'd string.
 * Used by query() and status() in the PG backend context.
 */
static char *capture_mysql_output(const char *client, int port, const char *sql,
                                  const char *extra_flag) {
  int pipefd[2];
  pid_t pid;
  int st;
  char port_str[16];
  StringInfoData buf;
  char chunk[4096];
  ssize_t n;

  snprintf(port_str, sizeof(port_str), "%d", port);

  if (pipe(pipefd) == -1)
    ereport(ERROR, (errmsg("pg_mysql: pipe() failed: %m")));

  pid = fork();
  if (pid == -1) {
    close(pipefd[0]);
    close(pipefd[1]);
    ereport(ERROR, (errmsg("pg_mysql: fork() failed: %m")));
  }

  if (pid == 0) {
    close(pipefd[0]);
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);

    if (extra_flag)
      execl(client, client, "-h", "127.0.0.1", "-P", port_str, "-u", "root",
            extra_flag, "-e", sql, (char *)NULL);
    else
      execl(client, client, "-h", "127.0.0.1", "-P", port_str, "-u", "root",
            "-e", sql, (char *)NULL);
    _exit(127);
  }

  close(pipefd[1]);
  initStringInfo(&buf);
  while ((n = read(pipefd[0], chunk, sizeof(chunk) - 1)) > 0) {
    chunk[n] = '\0';
    appendStringInfoString(&buf, chunk);
  }
  close(pipefd[0]);
  waitpid(pid, &st, 0);

  /* trim trailing newlines */
  while (buf.len > 0 && buf.data[buf.len - 1] == '\n')
    buf.data[--buf.len] = '\0';

  return buf.data;
}

/*
 * Run mysql client silently (for use in forked children — no ereport).
 * Returns 0 on success.
 */
static int run_mysql_cmd_quiet(const char *client, int port, const char *sql) {
  pid_t pid;
  int st;
  char ps[16];
  snprintf(ps, sizeof(ps), "%d", port);

  pid = fork();
  if (pid == -1)
    return -1;
  if (pid == 0) {
    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0) {
      dup2(devnull, 1);
      dup2(devnull, 2);
      close(devnull);
    }
    execl(client, client, "-h", "127.0.0.1", "-P", ps, "-u", "root", "-e", sql,
          (char *)NULL);
    _exit(127);
  }
  if (waitpid(pid, &st, 0) == -1)
    return -1;
  return (WIFEXITED(st) && WEXITSTATUS(st) == 0) ? 0 : -1;
}

static int wait_for_mysql_ready(const char *client, int port,
                                int timeout_secs) {
  int i, max = timeout_secs * 10;
  for (i = 0; i < max; i++) {
    usleep(100000);
    if (run_mysql_cmd_quiet(client, port, "SELECT 1") == 0)
      return 0;
  }
  return -1;
}

/*
 * Parse a field from SHOW REPLICA STATUS\G output.
 * Lines look like "     Replica_IO_Running: Yes"
 */
static bool parse_field(const char *output, const char *field, char *val,
                        size_t valsz) {
  const char *p;
  const char *eol;
  size_t flen = strlen(field);

  p = output;
  while ((p = strstr(p, field)) != NULL) {
    /* Ensure we're at the start of the field name (after whitespace + colon
     * pattern) */
    const char *colon = p + flen;
    if (*colon != ':') {
      p++;
      continue;
    }
    colon++;
    while (*colon == ' ')
      colon++;
    eol = colon;
    while (*eol && *eol != '\n')
      eol++;
    if ((size_t)(eol - colon) < valsz) {
      memcpy(val, colon, eol - colon);
      val[eol - colon] = '\0';
      return true;
    }
    return false;
  }
  return false;
}

/*
 * Helper: check if a running instance is a replica by querying
 * SHOW REPLICA STATUS for a Source_UUID.
 */
static bool instance_is_replica(int port) {
  char *output;
  char uuid[64] = "";

  if (read_pid_for_port(port) == 0)
    return false;

  output = capture_mysql_output(mysql_client_path, port,
                                "SHOW REPLICA STATUS\\G", NULL);
  parse_field(output, "Source_UUID", uuid, sizeof(uuid));
  return (uuid[0] != '\0');
}

/*
 * Scan basedir for the primary instance (a running instance that is
 * not a replica).  Returns the port, or 0 if none found.
 */
static int find_primary_port(void) {
  DIR *d;
  struct dirent *ent;
  int port;

  d = opendir(mysql_basedir);
  if (!d)
    return 0;

  while ((ent = readdir(d)) != NULL) {
    port = atoi(ent->d_name);
    if (port <= 0)
      continue;
    if (read_pid_for_port(port) > 0 && !instance_is_replica(port)) {
      closedir(d);
      return port;
    }
  }
  closedir(d);
  return 0;
}

/* ----------------------------------------------------------------
 * mysql.start(port int DEFAULT 0) -> (pid int, port int, datadir text)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(pg_mysql_start);
Datum pg_mysql_start(PG_FUNCTION_ARGS) {
  TupleDesc tupdesc;
  Datum values[3];
  bool nulls[3] = {false, false, false};
  HeapTuple tuple;
  int requested_port;
  int actual_port;
  bool semi_sync;
  pid_t child_pid;
  int status;
  char datadir[MAXPGPATH];
  char pidpath[MAXPGPATH];
  char sockpath[MAXPGPATH];
  char errlog[MAXPGPATH];
  char port_str[16];
  bool needs_init;

  if (!superuser())
    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("only superusers can start MySQL")));

  requested_port = PG_GETARG_INT32(0);
  semi_sync = PG_GETARG_BOOL(1);
  actual_port = (requested_port == 0) ? allocate_port() : requested_port;

  /* Already running? */
  {
    pid_t existing = read_pid_for_port(actual_port);
    if (existing > 0) {
      instance_datadir(datadir, sizeof(datadir), actual_port);
      if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("return type must be a row type")));
      values[0] = Int32GetDatum((int32)actual_port);
      values[1] = Int32GetDatum((int32)existing);
      values[2] = CStringGetTextDatum(datadir);
      tuple = heap_form_tuple(tupdesc, values, nulls);
      ereport(NOTICE, (errmsg("MySQL already running (PID %d, port %d)",
                              (int)existing, actual_port)));
      PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }
  }

  /* Also check if any primary is already running when port=0 */
  if (requested_port == 0) {
    int existing_port = find_primary_port();
    if (existing_port > 0) {
      pid_t existing = read_pid_for_port(existing_port);
      instance_datadir(datadir, sizeof(datadir), existing_port);
      if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("return type must be a row type")));
      values[0] = Int32GetDatum((int32)existing_port);
      values[1] = Int32GetDatum((int32)existing);
      values[2] = CStringGetTextDatum(datadir);
      tuple = heap_form_tuple(tupdesc, values, nulls);
      ereport(NOTICE, (errmsg("MySQL already running (PID %d, port %d)",
                              (int)existing, existing_port)));
      PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }
  }

  ensure_instance_dir(actual_port);
  needs_init = datadir_needs_init(actual_port);

  instance_datadir(datadir, sizeof(datadir), actual_port);
  instance_pidfile(pidpath, sizeof(pidpath), actual_port);
  instance_socket(sockpath, sizeof(sockpath), actual_port);
  instance_errorlog(errlog, sizeof(errlog), actual_port);
  snprintf(port_str, sizeof(port_str), "%d", actual_port);

  child_pid = fork();
  if (child_pid == -1)
    ereport(ERROR, (errmsg("pg_mysql: fork() failed: %m")));

  if (child_pid == 0) {
    pid_t gc = fork();
    if (gc == -1)
      _exit(1);
    if (gc > 0)
      _exit(0);
    setsid();

    if (needs_init) {
      pid_t ip;
      int is;
      ip = fork();
      if (ip == -1)
        _exit(1);
      if (ip == 0) {
        execl(mysqld_path, mysqld_path, "--initialize-insecure", "--datadir",
              datadir, errlog, (char *)NULL);
        _exit(127);
      }
      if (waitpid(ip, &is, 0) == -1)
        _exit(1);
      if (!WIFEXITED(is) || WEXITSTATUS(is) != 0)
        _exit(1);
    }

    {
      pid_t mp = fork();
      if (mp == -1)
        _exit(1);
      if (mp == 0) {
        if (semi_sync)
          execl(mysqld_path, mysqld_path, "--datadir", datadir, "--port",
                port_str, "--socket", sockpath, "--pid-file", pidpath, errlog,
                "--mysqlx=OFF", "--bind-address", "0.0.0.0", "--server-id=1",
                "--log-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                "--loose-rpl-semi-sync-source-enabled=ON",
                "--loose-rpl-semi-sync-replica-enabled=ON", (char *)NULL);
        else
          execl(mysqld_path, mysqld_path, "--datadir", datadir, "--port",
                port_str, "--socket", sockpath, "--pid-file", pidpath, errlog,
                "--mysqlx=OFF", "--bind-address", "0.0.0.0", "--server-id=1",
                "--log-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                (char *)NULL);
        _exit(127);
      }

      if (semi_sync) {
        if (wait_for_mysql_ready(mysql_client_path, actual_port, 30) != 0)
          _exit(1);
        run_mysql_cmd_quiet(
            mysql_client_path, actual_port,
            "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';");
        run_mysql_cmd_quiet(mysql_client_path, actual_port,
                            "INSTALL PLUGIN rpl_semi_sync_replica SONAME "
                            "'semisync_replica.so';");
      }
      _exit(0);
    }
  }

  waitpid(child_pid, &status, 0);

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    ereport(ERROR, (errmsg("return type must be a row type")));

  values[0] = Int32GetDatum((int32)actual_port);
  values[1] = Int32GetDatum(0);
  values[2] = CStringGetTextDatum(datadir);
  tuple = heap_form_tuple(tupdesc, values, nulls);

  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* ----------------------------------------------------------------
 * mysql.start_replica(source_host text, source_port int,
 *                     port int DEFAULT 0)
 *     -> (pid int, port int, datadir text)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(pg_mysql_start_replica);
Datum pg_mysql_start_replica(PG_FUNCTION_ARGS) {
  TupleDesc tupdesc;
  Datum values[3];
  bool nulls[3] = {false, false, false};
  HeapTuple tuple;
  char *source_host;
  int source_port;
  int replica_port;
  bool semi_sync;
  int server_id;
  pid_t child_pid;
  int status;
  char datadir[MAXPGPATH];
  char pidpath[MAXPGPATH];
  char sockpath[MAXPGPATH];
  char errlog[MAXPGPATH];
  char port_str[16];
  char sid_str[32];
  char src_host_buf[256];
  char my_mysqld[MAXPGPATH];
  char my_client[MAXPGPATH];
  bool needs_init;

  if (!superuser())
    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("only superusers can start a MySQL replica")));

  source_host = text_to_cstring(PG_GETARG_TEXT_PP(0));
  source_port = PG_GETARG_INT32(1);
  replica_port = PG_GETARG_INT32(2);
  semi_sync = PG_GETARG_BOOL(3);
  if (replica_port == 0)
    replica_port = allocate_port();

  server_id = replica_port;
  strlcpy(src_host_buf, source_host, sizeof(src_host_buf));

  /* Already running? */
  {
    pid_t existing = read_pid_for_port(replica_port);
    if (existing > 0) {
      instance_datadir(datadir, sizeof(datadir), replica_port);
      if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("return type must be a row type")));
      values[0] = Int32GetDatum((int32)replica_port);
      values[1] = Int32GetDatum((int32)existing);
      values[2] = CStringGetTextDatum(datadir);
      tuple = heap_form_tuple(tupdesc, values, nulls);
      ereport(NOTICE, (errmsg("replica already running (PID %d, port %d)",
                              (int)existing, replica_port)));
      PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }
  }

  ensure_instance_dir(replica_port);
  needs_init = datadir_needs_init(replica_port);

  instance_datadir(datadir, sizeof(datadir), replica_port);
  instance_pidfile(pidpath, sizeof(pidpath), replica_port);
  instance_socket(sockpath, sizeof(sockpath), replica_port);
  instance_errorlog(errlog, sizeof(errlog), replica_port);
  snprintf(port_str, sizeof(port_str), "%d", replica_port);
  snprintf(sid_str, sizeof(sid_str), "--server-id=%d", server_id);

  strlcpy(my_mysqld, mysqld_path, sizeof(my_mysqld));
  strlcpy(my_client, mysql_client_path, sizeof(my_client));

  child_pid = fork();
  if (child_pid == -1)
    ereport(ERROR, (errmsg("pg_mysql: fork() failed: %m")));

  if (child_pid == 0) {
    pid_t gc = fork();
    if (gc == -1)
      _exit(1);
    if (gc > 0)
      _exit(0);
    setsid();

    /* Init datadir */
    if (needs_init) {
      pid_t ip;
      int is;
      ip = fork();
      if (ip == -1)
        _exit(1);
      if (ip == 0) {
        execl(my_mysqld, my_mysqld, "--initialize-insecure", "--datadir",
              datadir, errlog, (char *)NULL);
        _exit(127);
      }
      if (waitpid(ip, &is, 0) == -1)
        _exit(1);
      if (!WIFEXITED(is) || WEXITSTATUS(is) != 0)
        _exit(1);
    }

    /* Fork mysqld for the replica */
    {
      pid_t mp = fork();
      if (mp == -1)
        _exit(1);
      if (mp == 0) {
        if (semi_sync)
          execl(my_mysqld, my_mysqld, "--datadir", datadir, "--port", port_str,
                "--socket", sockpath, "--pid-file", pidpath, errlog,
                "--mysqlx=OFF", "--bind-address", "0.0.0.0", sid_str,
                "--log-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                "--read-only=ON", "--super-read-only=ON",
                "--relay-log-recovery=ON", "--skip-replica-start",
                "--loose-rpl-semi-sync-source-enabled=ON",
                "--loose-rpl-semi-sync-replica-enabled=ON", (char *)NULL);
        else
          execl(my_mysqld, my_mysqld, "--datadir", datadir, "--port", port_str,
                "--socket", sockpath, "--pid-file", pidpath, errlog,
                "--mysqlx=OFF", "--bind-address", "0.0.0.0", sid_str,
                "--log-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                "--read-only=ON", "--super-read-only=ON",
                "--relay-log-recovery=ON", "--skip-replica-start",
                (char *)NULL);
        _exit(127);
      }

      /*
       * Clone-based replica setup:
       *   1. Wait for source, install clone plugin, create users
       *   2. Wait for replica, install clone plugin
       *   3. Clone from source (replica auto-restarts)
       *   4. Wait for replica to come back, configure replication
       */

      /* Step 1: source setup */
      if (wait_for_mysql_ready(my_client, source_port, 30) != 0)
        _exit(1);

      run_mysql_cmd_quiet(my_client, source_port,
                          "INSTALL PLUGIN clone SONAME 'mysql_clone.so';");
      if (semi_sync) {
        run_mysql_cmd_quiet(
            my_client, source_port,
            "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';");
        run_mysql_cmd_quiet(my_client, source_port,
                            "INSTALL PLUGIN rpl_semi_sync_replica SONAME "
                            "'semisync_replica.so';");
      }
      /* ignore errors if already installed */

      run_mysql_cmd_quiet(
          my_client, source_port,
          "CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';"
          "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';"
          "CREATE USER IF NOT EXISTS 'clone_user'@'%' IDENTIFIED BY "
          "'clone_pass';"
          "GRANT BACKUP_ADMIN ON *.* TO 'clone_user'@'%';"
          "FLUSH PRIVILEGES;");

      /* Step 2: replica setup */
      if (wait_for_mysql_ready(my_client, replica_port, 60) != 0)
        _exit(1);

      run_mysql_cmd_quiet(my_client, replica_port,
                          "INSTALL PLUGIN clone SONAME 'mysql_clone.so';");
      if (semi_sync) {
        run_mysql_cmd_quiet(
            my_client, replica_port,
            "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';");
        run_mysql_cmd_quiet(my_client, replica_port,
                            "INSTALL PLUGIN rpl_semi_sync_replica SONAME "
                            "'semisync_replica.so';");
      }

      {
        char sql[2048];

        /* Set valid donor list */
        snprintf(sql, sizeof(sql), "SET GLOBAL clone_valid_donor_list='%s:%d';",
                 src_host_buf, source_port);
        run_mysql_cmd_quiet(my_client, replica_port, sql);

        /* Step 3: clone from source (replica restarts after) */
        snprintf(sql, sizeof(sql),
                 "CLONE INSTANCE FROM 'clone_user'@'%s':%d "
                 "IDENTIFIED BY 'clone_pass';",
                 src_host_buf, source_port);
        run_mysql_cmd_quiet(my_client, replica_port, sql);
      }

      /* Step 4: wait for replica to restart after clone */
      if (wait_for_mysql_ready(my_client, replica_port, 90) != 0)
        _exit(1);

      /* Step 5: re-install semi-sync plugins after clone restart */
      if (semi_sync) {
        run_mysql_cmd_quiet(
            my_client, replica_port,
            "SET GLOBAL super_read_only=OFF;"
            "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';"
            "INSTALL PLUGIN rpl_semi_sync_replica SONAME 'semisync_replica.so';"
            "SET GLOBAL super_read_only=ON;");
      }

      /* Step 6: configure and start replication */
      {
        char sql[1024];
        snprintf(sql, sizeof(sql),
                 "CHANGE REPLICATION SOURCE TO "
                 "SOURCE_HOST='%s',"
                 "SOURCE_PORT=%d,"
                 "SOURCE_USER='repl',"
                 "SOURCE_PASSWORD='repl',"
                 "SOURCE_AUTO_POSITION=1,"
                 "GET_SOURCE_PUBLIC_KEY=1;"
                 "START REPLICA;",
                 src_host_buf, source_port);
        run_mysql_cmd_quiet(my_client, replica_port, sql);
      }
      _exit(0);
    }
  }

  waitpid(child_pid, &status, 0);

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    ereport(ERROR, (errmsg("return type must be a row type")));

  values[0] = Int32GetDatum((int32)replica_port);
  values[1] = Int32GetDatum(0);
  values[2] = CStringGetTextDatum(datadir);
  tuple = heap_form_tuple(tupdesc, values, nulls);

  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* ----------------------------------------------------------------
 * mysql.stop(port int DEFAULT 0) -> text
 * port=0 means stop the primary.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(pg_mysql_stop);
Datum pg_mysql_stop(PG_FUNCTION_ARGS) {
  int port;
  pid_t pid;
  int retries;

  if (!superuser())
    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("only superusers can stop MySQL")));

  port = PG_GETARG_INT32(0);
  if (port <= 0)
    ereport(ERROR, (errmsg("port must be specified")));

  pid = read_pid_for_port(port);
  if (pid == 0)
    PG_RETURN_TEXT_P(
        cstring_to_text(psprintf("MySQL on port %d is not running", port)));

  if (kill(pid, SIGTERM) == -1)
    ereport(ERROR,
            (errmsg("pg_mysql: kill(%d, SIGTERM) failed: %m", (int)pid)));

  for (retries = 0; retries < 300; retries++) {
    pg_usleep(100000);
    if (kill(pid, 0) == -1)
      PG_RETURN_TEXT_P(cstring_to_text(
          psprintf("MySQL stopped (port %d, was PID %d)", port, (int)pid)));
  }

  ereport(WARNING,
          (errmsg("pg_mysql: mysqld did not stop after 30s, sending SIGKILL")));
  kill(pid, SIGKILL);
  pg_usleep(500000);

  PG_RETURN_TEXT_P(cstring_to_text(
      psprintf("MySQL killed (port %d, was PID %d)", port, (int)pid)));
}

/*
 * Build one status row for a given port into values/nulls arrays.
 */
static void build_status_row(int port, Datum *values, bool *nulls) {
  pid_t pid;
  char datadir[MAXPGPATH];

  memset(nulls, false, 10 * sizeof(bool));

  pid = read_pid_for_port(port);
  instance_datadir(datadir, sizeof(datadir), port);

  values[0] = Int32GetDatum((int32)port);
  values[1] = Int32GetDatum((int32)pid);
  values[2] = BoolGetDatum(pid > 0);
  values[3] = CStringGetTextDatum(datadir);

  if (pid > 0) {
    char *repl_output;
    char src_uuid[64] = "";
    char io_val[64] = "";
    char sql_val[64] = "";
    char sbm_val[64] = "";

    repl_output = capture_mysql_output(mysql_client_path, port,
                                       "SHOW REPLICA STATUS\\G", NULL);

    parse_field(repl_output, "Source_UUID", src_uuid, sizeof(src_uuid));
    parse_field(repl_output, "Replica_IO_Running", io_val, sizeof(io_val));
    parse_field(repl_output, "Replica_SQL_Running", sql_val, sizeof(sql_val));
    parse_field(repl_output, "Seconds_Behind_Source", sbm_val, sizeof(sbm_val));

    if (src_uuid[0]) {
      /* It's a replica — source_uuid is the primary's UUID */
      values[4] = CStringGetTextDatum("replica");
      values[5] = CStringGetTextDatum(src_uuid);
      values[6] = CStringGetTextDatum(io_val[0] ? io_val : "unknown");
      values[7] = CStringGetTextDatum(sql_val[0] ? sql_val : "unknown");

      if (sbm_val[0] && strcmp(sbm_val, "NULL") != 0)
        values[8] = Int32GetDatum(atoi(sbm_val));
      else
        nulls[8] = true;
    } else {
      /* It's a primary — source_uuid is its own server_uuid */
      char *uuid_output;
      char uuid_val[64] = "";

      values[4] = CStringGetTextDatum("primary");

      uuid_output = capture_mysql_output(
          mysql_client_path, port, "SELECT @@server_uuid AS uuid\\G", NULL);
      parse_field(uuid_output, "uuid", uuid_val, sizeof(uuid_val));

      if (uuid_val[0])
        values[5] = CStringGetTextDatum(uuid_val);
      else
        nulls[5] = true;

      nulls[6] = true;
      nulls[7] = true;
      nulls[8] = true;
    }

    /* Check semi-sync status (source or replica plugin) */
    {
      char *semi_output;
      char semi_val[64] = "";

      semi_output = capture_mysql_output(
          mysql_client_path, port,
          "SELECT @@rpl_semi_sync_source_enabled AS v\\G", NULL);
      parse_field(semi_output, "v", semi_val, sizeof(semi_val));

      if (!semi_val[0]) {
        semi_output = capture_mysql_output(
            mysql_client_path, port,
            "SELECT @@rpl_semi_sync_replica_enabled AS v\\G", NULL);
        parse_field(semi_output, "v", semi_val, sizeof(semi_val));
      }

      if (semi_val[0])
        values[9] = BoolGetDatum(strcmp(semi_val, "1") == 0);
      else
        nulls[9] = true;
    }
  } else {
    nulls[4] = true;
    nulls[5] = true;
    nulls[6] = true;
    nulls[7] = true;
    nulls[8] = true;
    nulls[9] = true;
  }
}

/* ----------------------------------------------------------------
 * mysql.status(port int DEFAULT 0) -> SETOF status_info
 *
 * port=0: returns one row per instance directory under basedir.
 * port>0: returns one row for that specific port.
 * ---------------------------------------------------------------- */
static int int_cmp(const void *a, const void *b) {
  return (*(const int *)a) - (*(const int *)b);
}

PG_FUNCTION_INFO_V1(pg_mysql_status);
Datum pg_mysql_status(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  TupleDesc tupdesc;

  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldctx;
    int req_port;
    int *ports;
    int nports = 0;

    funcctx = SRF_FIRSTCALL_INIT();
    oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    req_port = PG_GETARG_INT32(0);

    if (req_port > 0) {
      ports = palloc(sizeof(int));
      ports[0] = req_port;
      nports = 1;
    } else {
      DIR *d;
      struct dirent *ent;

      ports = palloc(sizeof(int) * 128);

      d = opendir(mysql_basedir);
      if (d) {
        while ((ent = readdir(d)) != NULL && nports < 128) {
          int p = atoi(ent->d_name);
          if (p > 0)
            ports[nports++] = p;
        }
        closedir(d);
      }

      /* Sort by port ascending */
      if (nports > 1)
        qsort(ports, nports, sizeof(int), int_cmp);
    }

    funcctx->user_fctx = ports;
    funcctx->max_calls = nports;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
      ereport(ERROR, (errmsg("return type must be a row type")));
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldctx);
  }

  funcctx = SRF_PERCALL_SETUP();

  if (funcctx->call_cntr < funcctx->max_calls) {
    int *ports = (int *)funcctx->user_fctx;
    int port = ports[funcctx->call_cntr];
    Datum values[10];
    bool nulls[10];
    HeapTuple tuple;

    build_status_row(port, values, nulls);
    tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
  }

  SRF_RETURN_DONE(funcctx);
}

/* ----------------------------------------------------------------
 * mysql.query(sql text, port int DEFAULT 0) -> text
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(pg_mysql_query);
Datum pg_mysql_query(PG_FUNCTION_ARGS) {
  char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
  int port = PG_GETARG_INT32(1);
  char *result;

  if (port == 0)
    port = find_primary_port();
  if (port == 0)
    ereport(ERROR, (errmsg("no MySQL instance is running")));
  if (read_pid_for_port(port) == 0)
    ereport(ERROR, (errmsg("MySQL on port %d is not running", port)));

  result = capture_mysql_output(mysql_client_path, port, sql, "--table");

  PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* ----------------------------------------------------------------
 * mysql.delete(port int) -> text
 *
 * Stops the instance if running, then removes <basedir>/<port>/
 * so it no longer appears in status().
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(pg_mysql_delete);
Datum pg_mysql_delete(PG_FUNCTION_ARGS) {
  int port;
  pid_t pid;
  int retries;
  char instdir[MAXPGPATH];
  pid_t rm_pid;
  int rm_status;

  if (!superuser())
    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("only superusers can delete MySQL instances")));

  port = PG_GETARG_INT32(0);
  if (port <= 0)
    ereport(ERROR, (errmsg("port must be specified")));

  instance_dir(instdir, sizeof(instdir), port);

  /* Verify the directory exists */
  {
    struct stat st;
    if (stat(instdir, &st) != 0)
      ereport(ERROR, (errmsg("no instance found at %s", instdir)));
  }

  /*
   * If this instance is running, get its server_uuid and check if any
   * other running instance has it as Source_UUID — if so, block delete.
   */
  pid = read_pid_for_port(port);
  if (pid > 0) {
    char uuid_buf[64] = "";
    char *my_uuid = NULL;
    char *output;
    DIR *d;
    struct dirent *ent;

    output = capture_mysql_output(mysql_client_path, port,
                                  "SELECT @@server_uuid AS uuid\\G", NULL);
    parse_field(output, "uuid", uuid_buf, sizeof(uuid_buf));
    if (uuid_buf[0])
      my_uuid = pstrdup(uuid_buf);

    if (my_uuid) {
      d = opendir(mysql_basedir);
      if (d) {
        while ((ent = readdir(d)) != NULL) {
          int other_port = atoi(ent->d_name);
          char src_uuid[64] = "";
          char *rout;

          if (other_port <= 0 || other_port == port)
            continue;
          if (read_pid_for_port(other_port) == 0)
            continue;

          rout = capture_mysql_output(mysql_client_path, other_port,
                                      "SHOW REPLICA STATUS\\G", NULL);
          parse_field(rout, "Source_UUID", src_uuid, sizeof(src_uuid));

          if (strcmp(src_uuid, my_uuid) == 0) {
            closedir(d);
            ereport(ERROR, (errmsg("cannot delete instance on port %d: "
                                   "instance on port %d is replicating from it "
                                   "(source_uuid %s)",
                                   port, other_port, my_uuid)));
          }
        }
        closedir(d);
      }
    }
  }

  /* Stop if running */
  pid = read_pid_for_port(port);
  if (pid > 0) {
    kill(pid, SIGTERM);
    for (retries = 0; retries < 300; retries++) {
      pg_usleep(100000);
      if (kill(pid, 0) == -1)
        break;
    }
    if (kill(pid, 0) != -1) {
      kill(pid, SIGKILL);
      pg_usleep(500000);
    }
  }

  /* rm -rf the instance directory */
  rm_pid = fork();
  if (rm_pid == -1)
    ereport(ERROR, (errmsg("pg_mysql: fork() failed: %m")));

  if (rm_pid == 0) {
    execl("/bin/rm", "rm", "-rf", instdir, (char *)NULL);
    _exit(127);
  }

  if (waitpid(rm_pid, &rm_status, 0) == -1)
    ereport(ERROR, (errmsg("pg_mysql: waitpid() failed: %m")));

  if (!WIFEXITED(rm_status) || WEXITSTATUS(rm_status) != 0)
    ereport(ERROR, (errmsg("pg_mysql: failed to remove %s", instdir)));

  PG_RETURN_TEXT_P(cstring_to_text(
      psprintf("deleted instance on port %d (%s)", port, instdir)));
}
