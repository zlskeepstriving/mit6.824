# /bin/bash
for i in {1..100}
do
	go test -run TestFigure8Unreliable2C >> debug2CFigure8Unreliable
done
echo "test finished" >> debug2CFigure8Unreliable