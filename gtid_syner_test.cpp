//
// Created by fsindustry on 5/29/24.
//

#include <ev.h>
#include <iostream>
#include <sstream>
#include "gtid_syner/Gtid_MySQL_Conn.h"


#define EXECUTED_GTID_KEY "Executed_Gtid_Set"

struct gtid_info {
    std::string uuid;
    std::vector <std::pair<long, long>> trxids;
};

gtid_info parse_gtid(const std::string &input) {
    std::stringstream ss(input);
    std::string segment;
    // split multiple gtids by ','
    int i = 0;
    std::string uuid;
    std::vector<std::pair<long, long>> trxids;
    while (std::getline(ss, segment, ':')) {
        // trim
        segment.erase(0, segment.find_first_not_of(" \t\n\r"));
        segment.erase(segment.find_last_not_of(" \t\n\r") + 1);

        if (i == 0) { // uuid
            uuid = segment;
            i++;
        } else { // trxids
            size_t dashPos = segment.find('-');
            if (dashPos != std::string::npos) {
                long start = std::stol(segment.substr(0, dashPos));
                long end = std::stol(segment.substr(dashPos + 1));
                trxids.emplace_back(start, end);
            } else {
                long value = std::stol(segment);
                trxids.emplace_back(value, value);
            }
        }
    }

    // pack data
    gtid_info data;
    data.uuid = uuid;
    data.trxids = trxids;
    return data;
}

std::vector <gtid_info> parse_gtids(const std::string &input) {
    std::vector <gtid_info> gtids;
    std::stringstream ss(input);
    std::string segment;
    // split multiple gtids by ','
    while (std::getline(ss, segment, ',')) {
        // trim
        segment.erase(0, segment.find_first_not_of(" \t\n\r"));
        segment.erase(segment.find_last_not_of(" \t\n\r") + 1);
        gtids.push_back(parse_gtid(segment));
    }

    return gtids;
}

void mysql_query_cb(const gtid_sync::Gtid_MySQL_Conn *conn,
                    gtid_sync::sql_task_t *task,
                    gtid_sync::Gtid_MySQL_Result *res);

void timer_cb(EV_P_ ev_timer

*w,
int revents
) {
auto *task = new gtid_sync::sql_task_t;
task->
oper = gtid_sync::sql_task_t::OPERATE::SELECT;
task->
fn_query = mysql_query_cb;
task->
sql = "show master status;";
task->
privdata = w;

auto *conn = static_cast<gtid_sync::Gtid_MySQL_Conn *>(w->data);
conn->
add_task(task);
}

void mysql_query_cb(const gtid_sync::Gtid_MySQL_Conn *conn,
                    gtid_sync::sql_task_t *task,
                    gtid_sync::Gtid_MySQL_Result *res) {

    if (task->error != gtid_sync::ERR_OK) {
        // todo handle error msg
        return;
    }

    if (res == nullptr || !res->is_ok()) {
        // todo handle error msg
        return;
    }

    // get result
    std::vector <gtid_sync::map_row_t> result;
    if (!res->result_data(result) || result.empty()) {
        // todo handle error msg
        return;
    }

    auto &row = result[0];
    auto it = row.find(EXECUTED_GTID_KEY);
    if (it == row.end()) {
        // todo handle error msg
        return;
    }

    std::vector <gtid_info> gtids = parse_gtids(it->second);
    if (gtids.empty()) {
        // todo handle error msg
        return;
    }

    for (const auto &gtid: gtids) {
        for(const auto &trxid: gtid.trxids){
            std::cout << "UUID: " << gtid.uuid << ", Start: " << trxid.first << ", End: " << trxid.second << std::endl;
        }
    }
}


int main(int args, char **argv) {

    struct ev_loop *loop = EV_DEFAULT;
    gtid_sync::db_info_t db_info;
    db_info.port = 3306;
    db_info.max_conn_cnt = 1;
    db_info.host = "127.0.0.1";
    db_info.user = "root";
    db_info.password = "Huawei@123";
    db_info.db_name = "mysql";
    db_info.charset = "utf8";
    auto *conn = new gtid_sync::Gtid_MySQL_Conn;
    conn->init(&db_info, loop);

    auto *task = new gtid_sync::sql_task_t;
    task->oper = gtid_sync::sql_task_t::OPERATE::SELECT;
    task->fn_query = mysql_query_cb;
    task->sql = "show master status;";
    conn->init_task_timer(task, 3, 3);

    std::vector <gtid_info> gtids = parse_gtids("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:11:47-49,2174B383-5441-11E8-B90A-C80AA9429562:1-3, 24DA167-0C0C-11E8-8442-00059A3C7B00:1-19");
    for (const auto &gtid: gtids) {
        for(const auto &trxid: gtid.trxids){
            std::cout << "UUID: " << gtid.uuid << ", Start: " << trxid.first << ", End: " << trxid.second << std::endl;
        }
    }

    ev_run(loop, 0);
}