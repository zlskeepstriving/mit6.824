# /bin/bash
for i in {1..10}
do
	go test -run 2C >> debug2C.log
done
echo "test 2C finished" >> debug2C.log
