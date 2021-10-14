#! coding: utf-8




data = [
    "100",  # id
    "5f621c27545a920e6cae9fb7a5228aef", # md5 不需要校验
    "Anomaly Detection Sample", # 任务名
    "Anomaly Detection Sample", # 任务描述
    "{\"10\": \"演示数据\"}", # 指定日志分组
    "10", # 指定日志表
    '{"kafkaBrokers":"127.0.0.1:8801","kafkaTopic":"topic_name"}', # 指定kafka配置
    "_cw_biz", # 分组字段
    "[]", # 日志源过滤条件
    '[{"order":1, "fieldName":"src", "chartType":"PIE", "top":10}]', # 分析字段配置
    "110", # account id
    "2021-10-13 00:00:00", # update time
    "2021-10-13 00:00:00", # create time
    "1", # user id
    "1" # user id
]


with open("mysql_data.tsv") as f:
    f.read("\t".join(data))
