REM (c) 2021 Maksim Zheravin

    
rem ssh mzheravin@192.168.0.51 "nohup java -cp ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar io.aeron.cluster.ClusterTool . list-members "
rem ssh mzheravin@192.168.0.51 "nohup java -cp ~/cluster/exchange-cluster-1.0-SNAPSHOT.jar io.aeron.cluster.ClusterTool ~/aeron-cluster-driver-0/consensus-module list-members"

ssh -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk mzheravin@192.168.0.50  "killall -9 java"
ssh -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk mzheravin@192.168.0.51  "killall -9 java"
ssh -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk mzheravin@192.168.0.52  "killall -9 java"
ssh -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk mzheravin@192.168.0.53  "killall -9 java"




