# /bin/bash
for i in {1..50}
do
	go test -run 2B >> debug2B
done
echo "test 2B finished" >> debug2B