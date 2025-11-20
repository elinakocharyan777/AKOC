Кочарян Элина Самвеловна БПИ-244
  ---
  Два независимых процесса (server/client) обмениваются данными через POSIX shared memory и корректно завершаются при остановке любого из них.

  ---

  Для проверки:

- gcc -std=c11 -Wall -Wextra -O2 shm_server.c -o shm_server
- gcc -std=c11 -Wall -Wextra -O2 shm_client.c -o shm_client
- ./shm_server
- ./shm_client
