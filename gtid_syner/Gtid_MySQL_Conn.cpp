//
// Created by fsindustry on 5/27/24.
//

#include <cstring>
#include <errmsg.h>
#include "Gtid_MySQL_Conn.h"

namespace gtid_syner {


    Gtid_MySQL_Conn::Gtid_MySQL_Conn() {
        memset(&m_timer, 0, sizeof(m_timer));
        memset(&m_watcher, 0, sizeof(m_watcher));
    }

    Gtid_MySQL_Conn::~Gtid_MySQL_Conn() {
        clear_tasks();
        SAFE_DELETE(m_db_info);
    }


    bool Gtid_MySQL_Conn::init(const db_info_t *db_info, struct ev_loop *loop) {
        if (!set_db_info(db_info) || loop == nullptr) {
            return false;
        }

        bool reconnect = true;
        unsigned int timeout = 3;
        mysql_init(&m_mysql);
        mysql_options(&m_mysql, MYSQL_OPT_NONBLOCK, 0);  // set async.
        mysql_options(&m_mysql, MYSQL_OPT_CONNECT_TIMEOUT, reinterpret_cast<char *>(&timeout));
        mysql_options(&m_mysql, MYSQL_OPT_COMPRESS, NULL);
        mysql_options(&m_mysql, MYSQL_OPT_LOCAL_INFILE, NULL);
        mysql_options(&m_mysql, MYSQL_OPT_RECONNECT, reinterpret_cast<char *>(&reconnect));

        m_loop = loop;
        connect_start();
        load_timer();
        return true;
    }

    bool Gtid_MySQL_Conn::set_db_info(const db_info_t *db_info) {
        if (db_info == nullptr) {
            return false;
        }

        SAFE_DELETE(m_db_info);

        m_db_info = new db_info_t;
        m_db_info->port = db_info->port;
        m_db_info->max_conn_cnt = db_info->max_conn_cnt;
        m_db_info->host = db_info->host;
        m_db_info->db_name = db_info->db_name;
        m_db_info->password = db_info->password;
        m_db_info->charset = db_info->charset;
        m_db_info->user = db_info->user;
        return true;
    }

    void Gtid_MySQL_Conn::connect_ok() {
        m_is_connected = true;
        m_reconnect_cnt = 0;
    }

    void Gtid_MySQL_Conn::connect_failed() {
        m_is_connected = false;
        stop_task();
    }

    /**--------------------- events handle --------------------------------**/


    /**
     * add ev timer to check connection
     */
    bool Gtid_MySQL_Conn::load_timer() {
        if (m_loop == nullptr) {
            return false;
        }
        // todo config ping time interval
        ev_timer_init(&m_timer, ping_timer_cb, 1.0, 1.0);
        ev_timer_start(m_loop, &m_timer);
        m_timer.data = this;
        return true;
    }

    /**
     * ping timer handle_callback
     */
    void Gtid_MySQL_Conn::ping_timer_cb(struct ev_loop *loop, ev_timer *w, int revents) {
        Gtid_MySQL_Conn *c = static_cast<Gtid_MySQL_Conn *>(w->data);
        c->ping_start();
    }

    /**
     * io handle_callback
     */
    void Gtid_MySQL_Conn::libev_io_cb(struct ev_loop *loop, ev_io *w, int event) {
        Gtid_MySQL_Conn *c = static_cast<Gtid_MySQL_Conn *>(w->data);
        c->state_machine_handler(loop, w, event);
    }

    /**
     * state machine to handle mysql async api
     */
    void Gtid_MySQL_Conn::state_machine_handler(struct ev_loop *loop, ev_io *w, int event) {
        again: // quick start next loop
        switch (m_state) {
            case STATE::CONNECT_START:
                if (connect_start()) {
                    goto again;
                }
                break;
            case STATE::CONNECT_CONT:
                if (connect_cont(loop, w, event)) {
                    goto again;
                }
                break;
            case STATE::CONNECT_END:
                if (connect_end()) {
                    goto again;
                }
                break;
            case STATE::QUERY_START:
                if (query_start()) {
                    goto again;
                }
                break;
            case STATE::QUERY_CONT:
                if (query_cont(loop, w, event)) {
                    goto again;
                }
                break;
            case STATE::STORE_RESULT_START:
                if (store_result_start()) {
                    goto again;
                }
                break;
            case STATE::STORE_RESULT_CONT:
                if (store_result_cont(loop, w, event)) {
                    goto again;
                }
                break;
            case STATE::STORE_RESULT_END:
                if (store_result_end()) {
                    goto again;
                }
                break;
            case STATE::PING_START:
                if (ping_start()) {
                    goto again;
                }
                break;
            case STATE::PING_CONT:
                if (ping_cont(loop, w, event)) {
                    goto again;
                }
                break;
            case STATE::PING_END:
                if (ping_end()) {
                    goto again;
                }
                break;
            default:
                // todo error log
                break;
        }
    }

    bool Gtid_MySQL_Conn::connect_start() {
        int status = mysql_real_connect_start(&ret, &m_mysql,
                                              m_db_info->host.c_str(),
                                              m_db_info->user.c_str(),
                                              m_db_info->password.c_str(),
                                              m_db_info->db_name.c_str(),
                                              m_db_info->port, NULL, 0);
        if (status != OPERATION_FINISHED) {
            set_state(STATE::CONNECT_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::CONNECT_END);
            return true;
        }
    }

    bool Gtid_MySQL_Conn::connect_cont(struct ev_loop *loop, ev_io *watcher, int event) {
        int status = mysql_status(event);
        status = mysql_real_connect_cont(&ret, &m_mysql, status);
        if (status != OPERATION_FINISHED) {
            set_state(STATE::CONNECT_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::CONNECT_END);
            // todo handle ret
            return true;
        }
    }

    bool Gtid_MySQL_Conn::connect_end() {
        if (ret != nullptr) { // success
            connect_ok();
            mysql_set_character_set(&m_mysql, m_db_info->charset.c_str()); /* mysql 5.0 lib */
            set_state(STATE::QUERY_START);
            return true;
        } else { // error
            // todo error log
            connect_failed();
            return false;
        }
    }

    bool Gtid_MySQL_Conn::query_start() {

        SAFE_DELETE(m_cur_task);
        m_cur_task = fetch_next_task();
        if (m_cur_task == nullptr) {
            stop_task();
            return false;
        }

        int status = mysql_real_query_start(&err, &m_mysql, m_cur_task->sql.c_str(), m_cur_task->sql.size());
        if (status != OPERATION_FINISHED) {
            set_state(STATE::QUERY_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::STORE_RESULT_START);
            return true;
        }
    }

    bool Gtid_MySQL_Conn::query_cont(struct ev_loop *loop, ev_io *w, int event) {
        int status = mysql_status(event);
        status = mysql_real_query_cont(&err, &m_mysql, status);
        if (status != OPERATION_FINISHED) {
            set_state(STATE::QUERY_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::STORE_RESULT_START);
            return true;
        }
    }

    bool Gtid_MySQL_Conn::store_result_start() {
        int status = mysql_store_result_start(&m_query_res, &m_mysql);
        if (status != OPERATION_FINISHED) {
            set_state(STATE::STORE_RESULT_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::STORE_RESULT_END);
            return true;
        }
    }

    bool Gtid_MySQL_Conn::store_result_cont(struct ev_loop *loop, ev_io *watcher, int event) {
        int status = mysql_status(event);
        status = mysql_store_result_cont(&m_query_res, &m_mysql, status);
        if (status != OPERATION_FINISHED) {
            set_state(STATE::STORE_RESULT_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::STORE_RESULT_END);
            return true;
        }
    }

    bool Gtid_MySQL_Conn::store_result_end() {
        if (m_query_res) { // normal
            handle_callback(m_cur_task);
            set_state(STATE::QUERY_START);
            return true;
        } else { // error
            return handle_error();
        }
    }

    bool Gtid_MySQL_Conn::ping_start() {
        if (m_is_connected) {
            return false;
        }

        int status = mysql_ping_start(&err, &m_mysql);
        if (status != OPERATION_FINISHED) { // not finished
            set_state(STATE::PING_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::PING_END);
            return true;
        }
    }

    bool Gtid_MySQL_Conn::ping_cont(struct ev_loop *loop, ev_io *w, int event) {
        int status = mysql_status(event);
        status = mysql_ping_cont(&err, &m_mysql, status);
        if (status != OPERATION_FINISHED) { // not finished
            set_state(STATE::PING_CONT);
            next_event(status);
            return false;
        } else {
            set_state(STATE::PING_END);
            return true;
        }
    }


    bool Gtid_MySQL_Conn::ping_end() {
        if (err == OPERATION_SUCCESS) {
            if (!m_is_connected) {
                connect_ok();
                handle_error();
            }
        } else {
            connect_failed();
            // todo error log
            if (m_reconnect_cnt++ < MAX_DISCONNECT_TIME) {
                set_state(STATE::CONNECT_START);
                return true;
            }
        }

        return false;
    }

    void Gtid_MySQL_Conn::next_event(int mysql_status) {
        int events = event_status(mysql_status);

        if (events & EV_READ) { // read event
            if (!m_reading) {
                m_reading = true;
            }
        }

        if (events & EV_WRITE) { // write event
            if (!m_writing) {
                m_writing = true;
            }
        }

        if (!ev_is_active(&m_watcher)) { // initial watcher
            int fd = mysql_get_socket(&m_mysql);
            ev_io_init(&m_watcher, libev_io_cb, fd, events);
        }

        ev_io_stop(m_loop, &m_watcher);
        ev_io_set(&m_watcher, m_watcher.fd, m_watcher.events | events);
        ev_io_start(m_loop, &m_watcher);
        m_watcher.data = this;
    }

    int Gtid_MySQL_Conn::event_status(int status) {
        int events = 0;
        if (status & MYSQL_WAIT_READ) {
            events |= EV_READ;
        }
        if (status & MYSQL_WAIT_WRITE) {
            events |= EV_WRITE;
        }
        return events;
    }

    int Gtid_MySQL_Conn::mysql_status(int event) {
        int status = 0;
        if (event & EV_READ) {
            status |= MYSQL_WAIT_READ;
        }
        if (event & EV_WRITE) {
            status |= MYSQL_WAIT_WRITE;
        }
        if (event & EV_TIMEOUT) {
            status |= MYSQL_WAIT_TIMEOUT;
        }
        return status;
    }


    bool Gtid_MySQL_Conn::add_task(sql_task_t *task) {
        if (!m_is_connected || task == nullptr) {
            return false;
        }
        m_tasks.push_back(task);
        wait_next_task();
        return true;
    }

    void Gtid_MySQL_Conn::stop_task() {
        if (m_loop == nullptr) {
            return;
        }
        if (ev_is_active(&m_watcher)) {
            ev_io_stop(m_loop, &m_watcher);
        }
    }

    bool Gtid_MySQL_Conn::wait_next_task(int mysql_status) {
        if (!is_task_empty()) {
            next_event(mysql_status);
            return true;
        }
        return false;
    }

    sql_task_t *Gtid_MySQL_Conn::fetch_next_task() {
        sql_task_t *task = nullptr;
        if (!m_tasks.empty()) {
            task = m_tasks.front();
            m_tasks.pop_front();
        }
        return task;
    }

    void Gtid_MySQL_Conn::clear_tasks() {
        stop_task();

        if (m_cur_task != nullptr && m_cur_task->error == 0) {
            SET_ERR_INFO(m_cur_task, -1, "terminated!");
        }
        handle_callback(m_cur_task);
        SAFE_DELETE(m_cur_task);

        for (auto &it: m_tasks) {
            SET_ERR_INFO(it, -1, "terminated!");
            handle_callback(it);
            SAFE_DELETE(it);
        }
        m_tasks.clear();
    }


    bool Gtid_MySQL_Conn::handle_error() {
        int error = mysql_errno(&m_mysql);
        const char *errstr = mysql_error(&m_mysql);
        SET_ERR_INFO(m_cur_task, error, errstr);
        handle_callback(m_cur_task);
        if (error == CR_SERVER_LOST || error == CR_SERVER_GONE_ERROR) { // reconnect error
            connect_failed();
            // todo error log
            if (m_reconnect_cnt++ < MAX_DISCONNECT_TIME) {
                set_state(STATE::CONNECT_START);
                return true;
            }

            // over max reconnect count
            clear_tasks();
            return false;
        }

        // todo error log
        if (m_is_connected) {
            set_state(STATE::QUERY_START);
            return true;
        }

        return false;
    }

    void Gtid_MySQL_Conn::handle_callback(sql_task_t *task) {
        if (task == nullptr) {
            return;
        }

        if (task->fn_query != nullptr) {
            m_mysql_result.init(&m_mysql, m_query_res);
            task->fn_query(this, task, &m_mysql_result);
            if (m_query_res != nullptr) {
                mysql_free_result(m_query_res);
                m_query_res = nullptr;
            }
            task->fn_query = nullptr;
        }

        if (task->fn_exec != nullptr) {
            task->fn_exec(this, task);
            task->fn_exec = nullptr;
        }
    }

    struct ev_loop *Gtid_MySQL_Conn::get_loop() const {
        return m_loop;
    }
} // gtid_syner