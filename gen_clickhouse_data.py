import json
import random

"""
数据写入clickhouse表名： _cw_distributed_db.docp_log_anomaly_detection_pid
"""

sids = "[1]" # sid配置，和任务配置一致
window = 5 # 窗口长度/分钟
ts_window = 10
rule_id = "100" # 异常检测任务id
sample_size = 10

def p1_message_gen(begin_time, end_time):
    logs = []
    for _ in range(sample_size):
        uid = "u" + str(random.randrange(1000000, 10000000))
        uname = chr(ord("a") + random.randrange(0, 26)) + "_" + str(random.randint(10, 99))
        time = int(random.randrange(begin_time, end_time) / 1000) * 1000
        ip = "10.0.0.1"
        msg = 'JsonParseException when handling {"user_info":{"user_id":' + uid + ', "user_nick_name":"'+ uname +'"}}'

        data = {"_cw_host_ip":ip, "_cw_message": msg, "_cw_raw_time": time}
        logs.append(data)

    return json.dumps(logs, ensure_ascii=True)

def p1_stats_gen(count):
    return ""


def p2_message_gen1(begin_time, end_time):
    logs = []
    for _ in range(sample_size):
        time_usage = random.randint(1,200)
        src = random.choices(["gateway", "portal"],weights=[50,48],k=1)[0]
        ip = "10.0.0.1"
        msg = 'RPCLoginHandler request from {:s} to trace cost {:d} ms'.format(src, time_usage)
        time = int(random.randrange(begin_time, end_time) / 1000) * 1000

        data = {"_cw_host_ip": ip, "_cw_message": msg, "_cw_raw_time": time}
        logs.append(data)

    return json.dumps(logs, ensure_ascii=True)

def p2_stats_gen1(count):
    gateway_count = int(count * (50 / 98) * random.gauss(1,0.1))
    portal_count = count - gateway_count

    data = {"src": {"diversityCount": 2, "topDistribution": {"gateway": gateway_count, "portal": portal_count}}}
    if portal_count > gateway_count:
        portal_count, gateway_count = gateway_count, portal_count
    return json.dumps(data)

def p2_message_gen2(begin_time, end_time):
    logs = []
    for _ in range(sample_size):
        time_usage = random.randint(1, 200)
        src = "portal"
        ip = "10.0.0.2"
        msg = 'RPCLoginHandler request from {:s} to trace cost {:d} ms'.format(src, time_usage)
        time = int(random.randrange(begin_time, end_time) / 1000) * 1000

        data = {"_cw_host_ip": ip, "_cw_message": msg, "_cw_raw_time": time}
        logs.append(data)

    return json.dumps(logs, ensure_ascii=True)

def p2_stats_gen2(count):
    portal_count = count

    data = {"src": {"diversityCount": 1, "topDistribution": {"portal": portal_count}}}
    return json.dumps(data)

def p3_message_gen1(begin_time, end_time):
    logs = []
    for _ in range(sample_size):
        time_usage = random.randint(1, 50)
        src = random.choices(["trace", "dola", "domm"], weights=[50, 10, 10], k=1)[0]
        ip = "10.0.0.3"
        msg = 'RPCTenantLoginHandler request from {:s} to tenant cost {:d} ms'.format(src, time_usage)
        time = int(random.randrange(begin_time, end_time) / 1000) * 1000

        data = {"_cw_host_ip": ip, "_cw_message": msg, "_cw_raw_time": time}
        logs.append(data)

    return json.dumps(logs, ensure_ascii=True)

def p3_stats_gen1(count):
    trace_count = int(count * (50 / 70) * random.gauss(1, 0.05))
    dola_count = int(count * (10 / 70))
    domm_count = count - trace_count - dola_count

    if domm_count < 1:
        domm_count = 1
        trace_count = count - dola_count - domm_count


    if domm_count > dola_count:
        domm_count, dola_count = dola_count, domm_count

    data = {"src": {"diversityCount": 3, "topDistribution": {"trace": trace_count, "dola": dola_count, "domm": domm_count}}}
    return json.dumps(data)

def p3_message_gen2(begin_time, end_time):
    logs = []
    for _ in range(sample_size):
        time_usage = random.randint(1, 50)
        src = random.choices(["dola", "domm"], weights=[10, 10], k=1)[0]
        ip = "10.0.0.3"
        msg = 'RPCTenantLoginHandler request from {:s} to tenant cost {:d} ms'.format(src, time_usage)
        time = int(random.randrange(begin_time, end_time) / 1000) * 1000

        data = {"_cw_host_ip": ip, "_cw_message": msg, "_cw_raw_time": time}
        logs.append(data)

    return json.dumps(logs, ensure_ascii=True)

def p3_stats_gen2(count):
    dola_count=int(count * (10 / 20) * random.gauss(1, 0.05))
    domm_count = count - dola_count

    if domm_count < 1:
        domm_count = 1
        trace_count = count - dola_count - domm_count

    if domm_count > dola_count:
        domm_count, dola_count = dola_count, domm_count

    data = {"src": {"diversityCount": 2, "topDistribution": {"dola": dola_count, "domm": domm_count}}}
    return json.dumps(data)


p1_config = {
    "seg": [
        {
            "start": 1634205900000,
            "end": 1634209200000,
            "anomaly_type": 2,
            "anomaly_value": 0,
            "history_new": True,
            "base": 50 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 800
            },
            "stats": p1_stats_gen,
            "log_sample": p1_message_gen
        }
    ],
    "rule_id": rule_id,
    "pattern": 'JsonParseException when handling {"user_info":{"user_id":*, "user_nick_name":"*"}}',
    "pattern_id": "154c5c2e-2cc2-11ec-8d3d-0242ac130003",
    "group_field": {"_cw_biz": "gateway"},
    "trend_granularity": window,
    "source_ids": sids
}

p2_config = {
    "seg": [
        {
            "start": 1634054400000,
            "end": 1634205900000,
            "anomaly_type": 0,
            "anomaly_value": 0,
            "history_new": False,
            "base": 99 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 0.19 * 60 * 5
            },
            "stats": p2_stats_gen1,
            "log_sample": p2_message_gen1
        },
        {
            "start": 1634205900000,
            "end": 1634209200000,
            "anomaly_type": 4,
            "anomaly_value": ((99 - 48) / 99) * 100,
            "history_new": False,
            "base": 48 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 0.1 * 60 * 5
            },
            "stats": p2_stats_gen2,
            "log_sample": p2_message_gen2
        },
        {
            "start": 1634209200000,
            "end": 1634212800000,
            "anomaly_type": 0,
            "anomaly_value": 0,
            "history_new": False,
            "base": 99 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 0.19 * 60 * 5
            },
            "stats": p2_stats_gen1,
            "log_sample": p2_message_gen1
        }
    ],
    "rule_id": rule_id,
    "pattern": 'RPCLoginHandler request from * to trace take cost * ms',
    "pattern_id": "4ae4c752-2cc4-11ec-8d3d-0242ac130003",
    "group_field": {"_cw_biz": "trace"},
    "trend_granularity": window,
    "source_ids": sids
}

p3_config = {
    "seg": [
        {
            "start": 1634054400000,
            "end": 1634205900000,
            "anomaly_type": 0,
            "anomaly_value": 0,
            "history_new": False,
            "base": 70 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 0.18 * 60 * 5
            },
            "stats": p3_stats_gen1,
            "log_sample": p3_message_gen1
        },
        {
            "start": 1634205900000,
            "end": 1634209200000,
            "anomaly_type": 4,
            "anomaly_value": ((70 - 20) / 70) * 100,
            "history_new": False,
            "base": 20 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 0.1 * 60 * 5
            },
            "stats": p3_stats_gen2,
            "log_sample": p3_message_gen2
        },
        {
            "start": 1634209200000,
            "end": 1634212800000,
            "anomaly_type": 0,
            "anomaly_value": 0,
            "history_new": False,
            "base": 70 * 60 * 5,
            "noise": {
                "type": "gauss",
                "mean": 0,
                "stddev": 0.18 * 60 * 5
            },
            "stats": p3_stats_gen1,
            "log_sample": p3_message_gen1
        }
    ],
    "rule_id": rule_id,
    "pattern": 'RPCTenantLoginHandler request from * to tenant cost * ms',
    "pattern_id": "4ae4c752-2cc4-11ec-8d3d-0242ac130003",
    "group_field": {"_cw_biz": "tenant"},
    "trend_granularity": window,
    "source_ids": sids
}


def anomaly_desc_gen(type, value):
    if type == 0:
        return "未发现异常"
    if type == 1:
        return "历史新模式异常"
    if type == 2:
        return "时段新模式异常"
    if type == 3:
        return f"时段突增异常达到{value}%"
    if type == 4:
        return f"时段突降异常达到{value}%"

def ts_gen(count, begin_time, end_time):
    ts = begin_time
    n = int(window * 60 / ts_window)
    items = {}
    while ts < end_time:
        curr_count = int(count / n * random.gauss(1, 0.1) if n > 1 else 1)
        count -= curr_count
        n -= 1
        items[str(ts)] = curr_count
        ts += ts_window * 1000

    msg = json.dumps(items)

    return msg

def generate(config):
    for sc in config["seg"]:
        begin_time = sc["start"]
        while begin_time < sc["end"]:
            end_time = begin_time + config["trend_granularity"] * 60 * 1000
            count = int(sc["base"] + random.gauss(sc["noise"]["mean"], sc["noise"]["stddev"]))

            if sc["history_new"] and begin_time == sc["start"]:
                anomaly_type = 1
                anomaly_value = 0
            else:
                anomaly_type = sc["anomaly_type"]
                anomaly_value = int(sc["anomaly_value"])

            anomaly_description = anomaly_desc_gen(anomaly_type, anomaly_value)
            time_series = ts_gen(count, begin_time, end_time)
            group_field = json.dumps(config["group_field"], ensure_ascii=False)
            logs = sc["log_sample"](begin_time, end_time)
            stats = sc["stats"](count)

            items = [
                begin_time, end_time, config["pattern"], count,
                anomaly_type, anomaly_description, anomaly_value,
                config["rule_id"], time_series, stats, group_field,
                config["trend_granularity"], logs, config["source_ids"],
                config["pattern_id"]
            ]
            msg = "\t".join(str(x) for x in items)
            # msg = json.dumps(items, ensure_ascii=False)
            yield msg

            begin_time = end_time


with open("data.tsv", "w") as f:
    for message in generate(p1_config):
        f.write(message + "\n")

    for message in generate(p2_config):
        f.write(message + "\n")

    for message in generate(p3_config):
        f.write(message + "\n")
