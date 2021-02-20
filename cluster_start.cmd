REM (c) 2021 Maksim Zheravin

ssh -i ~/.ssh/lab.ppk mzheravin@192.168.0.51  "nohup java -jar ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar --mode=MULTISERVER --node=0 >~/cluster/log_out 2>~/cluster/log_err &"
ssh -i ~/.ssh/lab.ppk mzheravin@192.168.0.52  "nohup java -jar ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar --mode=MULTISERVER --node=1 >~/cluster/log_out 2>~/cluster/log_err &"
ssh -i ~/.ssh/lab.ppk mzheravin@192.168.0.53  "nohup java -jar ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar --mode=MULTISERVER --node=2 >~/cluster/log_out 2>~/cluster/log_err &"
