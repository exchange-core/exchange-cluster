REM (c) 2021 Maksim Zheravin



ssh -i ~/.ssh/lab.ppk mzheravin@192.168.0.61  "killall -9 java"


rem ssh -i ~/.ssh/lab.ppk  mzheravin@192.168.0.61  "java -cp ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar exchange.core2.cluster.example.ClientRunner --mode=MULTISERVER --service-mode=STRESS_SMALL --client-endpoint=192.168.7.4:19003"

ssh -i ~/.ssh/lab.ppk  mzheravin@192.168.0.61  "java -cp ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar exchange.core2.cluster.example.ClientRunner --mode=MULTISERVER --service-mode=STRESS_SMALL --client-endpoint=192.168.0.61:19003"

rem ssh -i ~/.ssh/lab.ppk  mzheravin@192.168.0.61  "java -cp ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar exchange.core2.cluster.example.ClientRunner --mode=MULTISERVER --service-mode=TESTING --client-endpoint=192.168.0.61:19003 >~/cluster/log_out 2>~/cluster/log_err"
