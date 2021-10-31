# /bin/bash
for i in {1..100}
do
	go test -run 2C >> debug2C1
done
echo "test finished" >> debug2C