# /bin/bash
for i in {1..30}
do
	go test -run 2C >> debug2C
done
echo "test finished" >> debug2C