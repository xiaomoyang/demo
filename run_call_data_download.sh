while true
do
end=`date +%s`000
start=`date -d "-1hour" +%s`000
echo $start $end
sql='select * from cw_db.call_chain_topology_basic where creation_time > '$start' and creation_time < '$end';'
echo "$sql" | /data/app/clickhouse/bin/clickhouse-client --port 18101 --password Rootmaster@777 > call.data.done
mv call.data.done call.data
sleep 3600
done