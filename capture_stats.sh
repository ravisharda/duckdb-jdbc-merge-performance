#!/usr/bin/env bash
prefix=$(date +"%Y_%m_%d_%I_%M_%p")
while true
do
   echo -e "\n"`date` >>  command_outputs/${prefix}_ps.out
   docker exec -it duckdb-test-container bash -c "/bin/ps -q 1 -o rss --no-headers" >> command_outputs/${prefix}_ps.out

   echo -e "\n"`date` >>  command_outputs/${prefix}_docker_stats.out
   docker stats --no-stream >> command_outputs/${prefix}_docker_stats.out

   echo -e "\n"`date` >>  command_outputs/${prefix}_free.out
   docker exec -it duckdb-test-container bash -c "free -mh">> command_outputs/${prefix}_free.out

   sleep 3
done
