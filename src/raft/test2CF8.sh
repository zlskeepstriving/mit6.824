# /bin/bash
for i in {1..30}
do
	go test -run TestFigure82C >> debug2CFigure8-$i
done
echo "test finished" >> debug2CFigure8Unreliable