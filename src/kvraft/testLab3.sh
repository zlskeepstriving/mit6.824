# /bin/bash
for i in {1..50}
do
	go test -run 3 >> debugLab3 -race
done
echo "test finished" >> debugLab3
