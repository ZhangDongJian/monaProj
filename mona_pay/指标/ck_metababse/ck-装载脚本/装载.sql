--hive建表
drop table if exists middle_table.mona_user_info_display;
create table middle_table.mona_user_info_display
(
    `stat_date`                      date,
    `uid`                            string,
    `is_active_user`                 int,
    `is_new_user`                    int,
    `reg_date`                       date,
    `reg_channel`                    string,
    `retention_days`                 int,
    `use_bot_count`                  int,
    `chat_round_count`               int,
    `rewriting_count`                int,
    `rewriting_count_distinct`       int,
    `session_count`                  int,
    `chat_times`                     decimal(16, 2),
    `lastest_exp`                    string,
    `is_user_charge`                 int,
    `is_user_charge_history`         int,
    `first_user_charge_time_history` date,
    `first_user_charge_time_type`    string,
    `is_user_charge_status`          int,
    `is_user_charge_status_standard` int,
    `is_user_charge_status_premium`  int,
    `standard_buy_count`             int,
    `premium_buy_count`              int,
    `is_user_charge_status_money`    decimal(16, 2),
    `is_user_charge_money`           decimal(16, 2),
    `is_user_charge_standard`        int,
    `is_user_charge_premium`         int,
    `user_charge_type`               string,
    `is_chat_intercepted`            int,
    `is_create_bot_intercepted`      int,
    `is_user_renew`                  int,
    `is_user_over_range`             int,
    `user_over_type`                 string,
    `is_user_over_type_bot_creation` int,
    `is_user_over_type_chat`         int,
    `create_bot_count`               int,
    `total_tokens`                  bigint,
    `input_tokens`                  bigint,
    `output_tokens`                 bigint
) USING org.apache.spark.sql.jdbc OPTIONS (
    url "jdbc:clickhouse://172.26.0.1:8123/mona",
    dbtable "mona.mona_user_info_display",
    driver "com.clickhouse.jdbc.ClickHouseDriver",
    user "default",
    password "SlrYXPpGOKDq",
    useSSL "false"
);
---------------------------------------------------------
drop table if exists mona.mona_user_info_display;
CREATE TABLE IF NOT EXISTS mona.mona_user_info_display
(
    `stat_date`                      date,
    `uid`                            varchar(50),
    `is_active_user`                 int,
    `is_new_user`                    int,
    `reg_date`                       date,
    `reg_channel`                    varchar(50),
    `retention_days`                 int,
    `use_bot_count` Nullable(int),
    `chat_round_count` Nullable(int),
    `rewriting_count` Nullable(int),
    `rewriting_count_distinct` Nullable(int),
    `session_count` Nullable(int),
    `chat_times` Nullable(decimal(16, 2)),
    `lastest_exp` Nullable(varchar(50)),
    `is_user_charge`                 int,
    `is_user_charge_history`         int,
    `first_user_charge_time_history` Nullable(date),
    `first_user_charge_time_type` Nullable(varchar(50)),
    `is_user_charge_status`          int,
    `is_user_charge_status_standard` int,
    `is_user_charge_status_premium`  int,
    `standard_buy_count`             int,
    `premium_buy_count`              int,
    `is_user_charge_status_money`    decimal(16, 2),
    `is_user_charge_money`           decimal(16, 2),
    `is_user_charge_standard`        int,
    `is_user_charge_premium`         int,
    `user_charge_type` Nullable(varchar(50)),
    `is_chat_intercepted`            int,
    `is_create_bot_intercepted`      int,
    `is_user_renew`                  int,
    `is_user_over_range`             int,
    `user_over_type` Nullable(varchar(50)),
    `is_user_over_type_bot_creation` int,
    `is_user_over_type_chat`         int,
    `create_bot_count` Nullable(int),
    `total_tokens` Nullable(bigint),
    `input_tokens` Nullable(bigint),
    `output_tokens` Nullable(bigint)
    ) ENGINE = ReplacingMergeTree()
    order by (stat_date, uid)
    SETTINGS old_parts_lifetime = 1;

-----------------------------------------------
insert into table middle_table.mona_user_info_display
select to_date(t1.stat_date, 'yyyyMMdd') stat_date,
       t1.uid,
       is_active_user,
       is_new_user,
       to_date(reg_date, 'yyyyMMdd'),
       reg_channel,
       retention_days,
       use_bot_count,
       chat_round_count,
       rewriting_count,
       rewriting_count_distinct,
       session_count,
       chat_times,
       nvl(lastest_exp, 'null'),
       nvl(is_user_charge, 0),
       nvl(is_user_charge_history, 0),
       to_date(first_user_charge_time_history, 'yyyyMMdd'),
       first_user_charge_time_type,
       nvl(is_user_charge_status, 0),
       nvl(is_user_charge_status_standard, 0),
       nvl(is_user_charge_status_premium, 0),
       nvl(standard_buy_count, 0),
       nvl(premium_buy_count, 0),
       nvl(is_user_charge_status_money, 0),
       nvl(is_user_charge_money, 0),
       nvl(is_user_charge_standard, 0),
       nvl(is_user_charge_premium, 0),
       user_charge_type,
       nvl(is_chat_intercepted, 0),
       nvl(is_create_bot_intercepted, 0),
       nvl(is_user_renew, 0),
       nvl(is_user_over_range, 0),
       user_over_type,
       nvl(is_user_over_type_bot_creation, 0),
       nvl(is_user_over_type_chat, 0),
       create_bot_count,
       total_token,
       input_token,
       output_token
from (select stat_date,
             uid,
             cast(nvl(max(case when key = '10001' then val end), 0) as int)    is_active_user,
             cast(nvl(max(case when key = '10002' then val end), 0) as int)    is_new_user,
             cast(max(case when key = '10007' then val end) as int)            retention_days,
             cast(max(case when key = '10008' then val end) as int)            use_bot_count,
             cast(max(case when key = '10009' then val end) as int)            chat_round_count,
             cast(max(case when key = '10010' then val end) as int)            rewriting_count,
             cast(max(case when key = '10011' then val end) as int)            rewriting_count_distinct,
             cast(max(case when key = '10012' then val end) as int)            session_count,
             cast(max(case when key = '10013' then val end) as decimal(16, 2)) chat_times,
             cast(max(case when key = '10016' then val end) as int)            is_user_charge,
             cast(max(case when key = '10019' then val end) as int)            is_user_charge_status,
             cast(max(case when key = '10020' then val end) as int)            standard_buy_count,
             cast(max(case when key = '10021' then val end) as int)            premium_buy_count,
             cast(max(case when key = '10023' then val end) as int)            is_user_charge_status_standard,
             cast(max(case when key = '10024' then val end) as int)            is_user_charge_status_premium,
             cast(max(case when key = '10025' then val end) as decimal(16, 2)) is_user_charge_status_money,
             cast(max(case when key = '10026' then val end) as decimal(16, 2)) is_user_charge_money,
             cast(max(case when key = '10027' then val end) as int)            is_user_charge_standard,
             cast(max(case when key = '10028' then val end) as int)            is_user_charge_premium,
             max(case when key = '10029' then val end)                         user_charge_type,
             cast(max(case when key = '10031' then val end) as int)            is_chat_intercepted,
             cast(max(case when key = '10033' then val end) as int)            is_create_bot_intercepted,
             cast(max(case when key = '10035' then val end) as int)            is_user_renew,
             cast(max(case when key = '100
36' then val end) as int)            is_user_over_range,
             max(case when key = '10037' then val end)                         user_over_type,
             cast(max(case when key = '10038' then val end) as int)            is_user_over_type_bot_creation,
             cast(max(case when key = '10039' then val end) as int)            is_user_over_type_chat,
             cast(max(case when key = '10030' then val end) as int)            create_bot_count,
             cast(max(case when key = '10040' then val end) as bigint)         total_token,
             cast(max(case when key = '10041' then val end) as bigint)         input_token,
             cast(max(case when key = '10042' then val end) as bigint)         output_token

      from middle_table.mona_user_info
      where stat_date = '$do_date'
      group by stat_date, uid) t1
         left join (select stat_date,
                           uid,
                           val reg_date
                    from middle_table.mona_user_info
                    where key = '10005') t2 on t1.stat_date = t2.stat_date and t1.uid = t2.uid
         left join (select stat_date,
                           uid,
                           val reg_channel
                    from middle_table.mona_user_info
                    where key = '10006') t3 on t1.stat_date = t3.stat_date and t1.uid = t3.uid
         left join (select stat_date,
                           uid,
                           val lastest_exp
                    from middle_table.mona_user_info
                    where key = '10015') t4 on t1.stat_date = t4.stat_date and t1.uid = t4.uid
         left join (select stat_date,
                           uid,
                           cast(val as int) is_user_charge_history
                    from middle_table.mona_user_info
                    where key = '10017') t5 on t1.stat_date = t5.stat_date and t1.uid = t5.uid
         left join (select stat_date,
                           uid,
                           val first_user_charge_time_history
                    from middle_table.mona_user_info
                    where key = '10018') t6 on t1.stat_date = t6.stat_date and t1.uid = t6.uid
         left join (select stat_date,
                           uid,
                           val first_user_charge_time_type
                    from middle_table.mona_user_info
                    where key = '10022') t7 on t1.stat_date = t7.stat_date and t1.uid = t7.uid
where is_active_user = 1
order by stat_date, t1.uid;

