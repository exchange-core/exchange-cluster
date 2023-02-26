REM (c) 2021 Maksim Zheravin

call gradlew build -x test

scp -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk ./build/libs/exchange-cluster-1.0-SNAPSHOT.jar mzheravin@192.168.0.51:~/cluster/exchange-cluster-1.0-SNAPSHOT.jar
scp -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk ./build/libs/exchange-cluster-1.0-SNAPSHOT.jar mzheravin@192.168.0.52:~/cluster/exchange-cluster-1.0-SNAPSHOT.jar
scp -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk ./build/libs/exchange-cluster-1.0-SNAPSHOT.jar mzheravin@192.168.0.53:~/cluster/exchange-cluster-1.0-SNAPSHOT.jar
scp -oStrictHostKeyChecking=no -i ~/.ssh/lab.ppk ./build/libs/exchange-cluster-1.0-SNAPSHOT.jar mzheravin@192.168.0.50:~/cluster/exchange-cluster-1.0-SNAPSHOT.jar
