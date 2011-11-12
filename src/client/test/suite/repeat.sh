if [ $# -lt 1 ]
then
  x=10
else
  x=$1
fi

n=$x
err=0

while [ $x -gt 0 ]
do
  d=$(($n - $x))
  printf "Run: %i\n" $d
  make run
  if [ ! $? -eq 0 ]
  then
    printf "Error after %i runs!!!\n" $d
    err=1
    break
  else
    x=$(($x - 1))
  fi
done

if [ $err -eq 0 ]
then
  printf "Success after %i runs\n" $n
fi 
