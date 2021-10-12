cd ..
rm -rf demo.tar.gz
tar -czvf demo.tar.gz demo
ssh root@10.0.12.208 -p 36000 "cd /data/app/omp_salt_agent/env/bin;rm -rf demo.tar.gz;rm -rf demo"  
scp -P 36000 demo.tar.gz root@10.0.12.208:/data/app/omp_salt_agent/env/bin
ssh root@10.0.12.208 -p 36000 "cd /data/app/omp_salt_agent/env/bin;tar -xzvf demo.tar.gz"  


