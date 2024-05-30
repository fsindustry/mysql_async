//
// Created by fsindustry on 5/27/24.
//

#ifndef GTIDSYNC_GTID_MYSQL_CONN_H
#define GTIDSYNC_GTID_MYSQL_CONN_H

#include <string>
#include <ev.h>
#include <list>
#include "Gtid_MySQL_Result.h"

#define SAFE_DELETE(x)              \
    {                               \
        delete x;                   \
        x = nullptr;                \
    }

#define MAX_DISCONNECT_TIME 2
#define OPERATION_FINISHED 0
#define OPERATION_SUCCESS 0
#define SET_ERR_INFO(x, e, s)                \
    if (x != nullptr) {                      \
        x->error = e;                        \
        x->errstr = (s != nullptr) ? s : ""; \
    }

namespace gtid_sync {

    class Gtid_MySQL_Conn;

    typedef struct sql_task_s sql_task_t;

    /* database link info. */
    typedef struct db_info_s {
        int port = 0;
        int max_conn_cnt = 0;
        std::string host;
        std::string user;
        std::string password;
        std::string db_name;
        std::string charset;
    } db_info_t;


    typedef void(*mysql_exec_cb)(const Gtid_MySQL_Conn *, sql_task_t *task);

    typedef void(*mysql_query_cb)(const Gtid_MySQL_Conn *, sql_task_t *task, Gtid_MySQL_Result *res);

    typedef struct sql_task_s {
        enum class OPERATE {
            SELECT,
            EXEC,
        };
        std::string sql;
        OPERATE oper = OPERATE::SELECT;
        mysql_exec_cb fn_exec = nullptr;
        mysql_query_cb fn_query = nullptr;
        void *privdata = nullptr;
        int error = 0;
        std::string errstr;
    } sql_task_t;

    class Gtid_MySQL_Conn {

    public:
        enum class STATE {
            CONNECT_START = 0,
            CONNECT_CONT = 1,
            CONNECT_END = 9,
            QUERY_START = 10,
            QUERY_CONT = 11,
            STORE_RESULT_START = 30,
            STORE_RESULT_CONT = 31,
            STORE_RESULT_END = 39,
            PING_START,
            PING_CONT,
            PING_END
        };

        Gtid_MySQL_Conn();

        virtual ~Gtid_MySQL_Conn();

        bool init(const db_info_t *db_info, struct ev_loop *loop);

        bool add_task(sql_task_t *task);

        bool is_connected() const { return m_is_connected; }

    private:
        bool set_db_info(const db_info_t *db_info);

        void connect_failed();

        void connect_ok();

        /**--------------------- events handle --------------------------------**/

        void set_state(STATE state) { m_state = state; }

        /**
         * add ev timer to check connection
         */
        bool load_timer();

        /**
         * ping timer handle_callback
         */
        static void ping_timer_cb(struct ev_loop *loop, ev_timer *w, int events);

        /**
         * io handle_callback
         */
        static void libev_io_cb(struct ev_loop *loop, ev_io *w, int event);

        /**
         * state machine to handle mysql async api
         */
        void state_machine_handler(int event);

        void next_event(int mysql_status);


        static int event_status(int status);

        static int mysql_status(int event);

        /**--------------------- mysql async api --------------------------------**/

        bool connect_start();

        bool connect_cont(int event);

        bool connect_end();

        bool query_start();

        bool query_cont(int event);

        bool store_result_start();

        bool store_result_cont(int event);

        bool store_result_end();

        bool ping_start();

        bool ping_cont(int event);

        bool ping_end();

        /**--------------------- sql task api --------------------------------**/
        bool is_task_empty() { return (m_tasks.empty() && m_cur_task == nullptr); }

        sql_task_t *fetch_next_task();

        bool wait_next_task(int mysql_status = MYSQL_WAIT_WRITE);

        void stop_task();

        void clear_tasks();

        bool handle_error();

        void handle_callback(sql_task_t *task);

    private:

        ev_io m_watcher;
        ev_timer m_timer;
        bool m_reading = false;
        bool m_writing = false;
        struct ev_loop *m_loop = nullptr;
    public:
        struct ev_loop *get_loop() const;

    private:

        bool m_is_connected = true;
        int m_reconnect_cnt = 0;
        db_info_t *m_db_info = nullptr;

        MYSQL m_mysql;
        MYSQL_RES *m_query_res = nullptr;
        MYSQL *ret;
        int err;
        STATE m_state = STATE::CONNECT_START;
        Gtid_MySQL_Result m_mysql_result;

        std::list<sql_task_t *> m_tasks;
        sql_task_t *m_cur_task = nullptr;

    };

} // gtid_sync

#endif //GTIDSYNC_GTID_MYSQL_CONN_H
