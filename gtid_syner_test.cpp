//
// Created by fsindustry on 5/29/24.
//

#include <ev.h>
#include <iostream>
#include "gtid_syner/Gtid_MySQL_Conn.h"


void
mysql_query_cb(const gtid_syner::Gtid_MySQL_Conn *conn,
               gtid_syner::sql_task_t *task,
               gtid_syner::Gtid_MySQL_Result *res) {
    std::vector<gtid_syner::map_row_t> result;
    res->result_data(result);
    for (auto &row: result) {
        for (auto &pair: row) {
            std::cout << pair.first << ":" << pair.second << std::endl;
        }
    }

//    struct ev_timer *timer = static_cast<struct ev_timer *>(task->privdata);
//    ev_timer_again(conn->get_loop(), timer);
}

void timer_cb(EV_P_ ev_timer *w, int revents) {
    auto *task = new gtid_syner::sql_task_t;
    task->oper = gtid_syner::sql_task_t::OPERATE::SELECT;
    task->fn_query = mysql_query_cb;
    task->sql = "show master status;";
    task->privdata = w;

    auto *conn = static_cast<gtid_syner::Gtid_MySQL_Conn *>(w->data);
    conn->add_task(task);
}


int main(int args, char **argv) {

    struct ev_loop *loop = EV_DEFAULT;
    gtid_syner::db_info_t db_info;
    db_info.port = 3306;
    db_info.max_conn_cnt = 1;
    db_info.host = "127.0.0.1";
    db_info.user = "root";
    db_info.password = "Huawei@123";
    db_info.db_name = "mysql";
    db_info.charset = "utf8";
    auto *conn = new gtid_syner::Gtid_MySQL_Conn;
    conn->init(&db_info, loop);

    ev_timer timer;
    timer.data = conn;
    ev_timer_init(&timer, timer_cb, 3.0, 0.0);
    ev_timer_start(loop, &timer);

    ev_run(loop, 0);
}