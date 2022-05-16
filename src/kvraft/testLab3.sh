# /bin/bash
for i in {1..10}
do
	go test -run TestSnapshotRecoverManyClients3B >> debug.log
done
echo "test finished" >> debugLab3
