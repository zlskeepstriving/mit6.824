# /bin/bash
for i in {1..20}
do
	go test -run 2A >> debug2A
done
echo "test finished" >> debug2A