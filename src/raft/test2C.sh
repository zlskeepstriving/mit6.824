# /bin/bash
time=$(date)
echo $time >> test2C.log

for i in {1..10}
do
	go test -run 2C >> test2C.log
done
echo "test 2C finished" >> test2C.log
echo " "
